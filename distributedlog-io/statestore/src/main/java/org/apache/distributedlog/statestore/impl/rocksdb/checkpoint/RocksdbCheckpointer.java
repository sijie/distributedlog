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

import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.distributedlog.common.concurrent.FutureUtils;
import org.apache.distributedlog.statestore.exceptions.StateStoreRuntimeException;
import org.apache.distributedlog.statestore.impl.rocksdb.RocksUtils;
import org.rocksdb.Checkpoint;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * A checkpointer that checkpoints rocksdb.
 */
class RocksdbCheckpointer implements AutoCloseable {

    static class CheckpointTask implements Runnable {

        private final String ckptName;
        private final Checkpoint checkpoint;
        private final File path;
        private final CompletableFuture<RocksdbCheckpointer> future;

        CheckpointTask(Checkpoint checkpoint,
                       String ckptName,
                       File path) {
            this.ckptName = ckptName;
            this.checkpoint = checkpoint;
            this.path = path;
            this.future = FutureUtils.createFuture();
        }

        @Override
        public void run() {
            // Step 1: create a checkpoint of current local db instance to a checkpoint diretory
            try {
                this.checkpoint.createCheckpoint(path.getAbsolutePath());
            } catch (RocksDBException e) {
                future.completeExceptionally(
                    new StateStoreRuntimeException("Failed to create rocksdb checkpoint at " + path, e));
                return;
            }
            // after the checkpoint is done
            File[] allFiles = path.listFiles();
        }

        /**
         * Record checkpoint into a general.
         */
        void prepareCheckpoint(File[] allFiles) {
            for (File file : allFiles) {
                if (file.getName().endsWith(".sst")) { // it is a sst file

                } else { // other files
                }
            }
        }

    }

    private final RocksDB db;
    private final Checkpoint checkpoint;
    private final ScheduledExecutorService executor;
    private final File checkpointsDir;

    // checkpoint management
    // sst files

    RocksdbCheckpointer(RocksDB db,
                        File checkpointsDir,
                        ScheduledExecutorService executor) {
        this.db = db;
        this.executor = executor;
        this.checkpointsDir = checkpointsDir;
        this.checkpoint = Checkpoint.create(db);
    }

    CompletableFuture<RocksdbCheckpointer> checkpoint(String ckptName) {
        CheckpointTask task = new CheckpointTask(
            checkpoint,
            ckptName,
            new File(checkpointsDir, ckptName));
        executor.submit(task);
        return task.future;
    }

    @Override
    public void close() {
        RocksUtils.close(checkpoint);
    }
}
