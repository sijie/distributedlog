/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.distributedlog.statestore.impl.rocksdb.checkpoint;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.rocksdb.Checkpoint;
import org.rocksdb.RocksDBException;

/**
 * Represents an inprogress checkpoint.
 */
@Slf4j
class RocksCheckpointInprogress implements Runnable, FutureEventListener<List<RocksFileInfo>> {

    private final String checkpointName;
    private final Checkpoint checkpoint;
    private final File path;
    private final ScheduledExecutorService ioScheduler;
    private final CompletableFuture<RocksCheckpoint> future;
    private final RocksFiles rocksFiles;

    // state
    private File[] files;
    private List<CompletableFuture<RocksFileInfo>> copyTasks;

    RocksCheckpointInprogress(Checkpoint checkpoint,
                              String checkpointName,
                              File path,
                              RocksFiles rocksFiles,
                              ScheduledExecutorService ioScheduler) {
        this.checkpointName = checkpointName;
        this.path = path;
        this.checkpoint = checkpoint;
        this.rocksFiles = rocksFiles;
        this.ioScheduler = ioScheduler;

        this.future = FutureUtils.createFuture();
    }

    CompletableFuture<RocksCheckpoint> future() {
        return future;
    }

    @Override
    public void run() {
        // 1. create a checkpoint of current local db instance to a checkpoint directory
        try {
            this.checkpoint.createCheckpoint(path.getAbsolutePath());
        } catch (RocksDBException e) {
            future.completeExceptionally(
                new IOException("Failed to create a rocksdb checkpoint at " + path, e));
            return;
        }
        // 2. copy all the files
        this.files = path.listFiles();
        this.copyTasks = Lists.newArrayListWithExpectedSize(files.length);
        for (File file : files) {
            if (file.getName().endsWith(".sst")) {
                copyTasks.add(rocksFiles.copySstFile(checkpointName, file.getName(), file));
            } else {
                String name = path.getName() + "/" + file.getName(); // checkpoint + filename
                copyTasks.add(rocksFiles.copySstFile(checkpointName, name, file));
            }
        }
        // 3. after the all files are copied, the checkpoint are done.
        FutureUtils.collect(copyTasks)
            .whenCompleteAsync(this, ioScheduler);
    }


    @Override
    public void onSuccess(List<RocksFileInfo> fis) {
        // generate a checkpoint result
        ImmutableMap.Builder<String, RocksFileInfo> builder = ImmutableMap.builder();
        for (RocksFileInfo fi : fis) {
            builder.put(fi.getName(), fi);
        }
        RocksCheckpoint rc = new RocksCheckpoint(builder.build());
        future.complete(rc);
    }

    @Override
    public void onFailure(Throwable throwable) {
        // fail to create a checkpoint, rollback
        FutureUtils.ensure(
            rollbackCheckpoint(),
            () -> future.completeExceptionally(throwable));
    }

    private CompletableFuture<Void> rollbackCheckpoint() {
        // 1. first cancel all the copy tasks
        for (CompletableFuture<RocksFileInfo> task : copyTasks) {
            task.cancel(true);
        }
        // 2. after all the copy tasks are cancelled, delete the files
        List<CompletableFuture<RocksFileInfo>> deleteTasks = Lists.newArrayListWithExpectedSize(files.length);
        for (File file : files) {
            String name;
            if (file.getName().endsWith(".sst")) {
                name = file.getName();
            } else {
                name = path.getName() + "/" + file.getName();
            }
            deleteTasks.add(rocksFiles.deleteSstFile(checkpointName, name));
        }
        return FutureUtils.collect(deleteTasks).thenApply(ignored -> null);
    }


}
