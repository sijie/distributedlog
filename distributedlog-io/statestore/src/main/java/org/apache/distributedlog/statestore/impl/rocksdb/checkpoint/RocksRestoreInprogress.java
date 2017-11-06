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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.common.concurrent.FutureUtils;

/**
 * Represents an inprogress task that restore a checkpoint.
 */
@Slf4j
class RocksRestoreInprogress implements Runnable {

    private final BookKeeper bk;
    private final RocksCheckpoint checkpoint;
    private final File destDir;
    private final ScheduledExecutorService ioScheduler;
    private final CompletableFuture<Void> future;

    RocksRestoreInprogress(BookKeeper bk,
                           RocksCheckpoint checkpoint,
                           File destDir,
                           ScheduledExecutorService ioScheduler) {
        this.bk = bk;
        this.checkpoint = checkpoint;
        this.destDir = destDir;
        this.ioScheduler = ioScheduler;
        this.future = FutureUtils.createFuture();
    }

    CompletableFuture<Void> future() {
        return future;
    }

    @Override
    public void run() {
        restore();
    }

    private void restore() {
        if (!destDir.exists()) {
            future.completeExceptionally(new IOException("Dir " + destDir + " doesn't exist"));
            return;
        }

        File[] localFiles = destDir.listFiles();
        String localDirName = destDir.getAbsoluteFile().getName();
        Map<String, LedgerSaver> filesToCopy = Maps.newHashMapWithExpectedSize(checkpoint.getFiles().size());
        List<File> filesToDelete = Lists.newArrayListWithExpectedSize(checkpoint.getFiles().size());
        for (Map.Entry<String, RocksFileInfo> entry : checkpoint.getFiles().entrySet()) {
            filesToCopy.put(
                entry.getKey(),
                new LedgerSaver(
                    bk,
                    entry.getValue(),
                    destDir,
                    ioScheduler));
        }
        for (File localFile : localFiles) {
            String localFileName;
            if (localFile.getName().endsWith(".sst")) {
                localFileName = localFile.getName();
            } else {
                localFileName = localDirName + "/" + localFile.getName();
            }
            RocksFileInfo fi = checkpoint.getFiles().get(localFileName);
            if (null != fi) {
                if (localFile.length() != fi.getLength()) {
                    filesToDelete.add(localFile);
                } else {
                    filesToCopy.remove(fi.getName());
                }
            } else {
                filesToDelete.add(localFile);
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("Restoring checkpoint: files to delete = {}, files to copy = {}",
                filesToDelete, filesToCopy);
        }

        deleteFiles(filesToDelete);

        List<CompletableFuture<RocksFileInfo>> copyInprogress =
            Lists.newArrayListWithExpectedSize(filesToCopy.size());
        for (LedgerSaver saver : filesToCopy.values()) {
            ioScheduler.submit(saver);
            copyInprogress.add(saver.future());
        }

        FutureUtils.proxyTo(
            FutureUtils.collect(copyInprogress).thenApply(ignored -> null),
            future);
    }

    private void deleteFiles(List<File> files) {
        for (File file : files) {
            file.delete();
        }
    }
}
