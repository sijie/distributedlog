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

import static org.apache.distributedlog.statestore.impl.rocksdb.RocksUtils.NOP_CMD;
import static org.apache.distributedlog.statestore.impl.rocksdb.RocksUtils.newCheckpointCommand;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.util.HardLink;
import org.apache.commons.io.FileUtils;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.LogRecordWithDLSN;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.LogReader;
import org.apache.distributedlog.statestore.impl.rocksdb.RocksUtils;
import org.apache.distributedlog.statestore.proto.CheckpointCommand;
import org.rocksdb.Checkpoint;

/**
 * A manager that manages all the checkpoints.
 */
@Slf4j
class RocksCheckpoints implements AutoCloseable {

    private final Checkpoint checkpoint;
    private final RocksFiles rocksFiles;
    private final ScheduledExecutorService executor;
    private final File checkpointDir;
    private final Map<String, RocksCheckpoint> completedCheckpoints;
    private final Map<String, CompletableFuture<RocksCheckpoint>> inprogressCheckpoints;
    private final Map<String, CompletableFuture<Void>> deleteCheckpoints;

    // journal the checkpoint metadata updates
    private DLCheckpointLog checkpointLog;

    RocksCheckpoints(Checkpoint checkpoint,
                     File checkpointDir,
                     RocksFiles rocksFiles,
                     ScheduledExecutorService executor) {
        this.checkpoint = checkpoint;
        this.checkpointDir = checkpointDir;
        this.rocksFiles = rocksFiles;
        this.executor = executor;
        this.completedCheckpoints = Maps.newHashMap();
        this.inprogressCheckpoints = Maps.newHashMap();
        this.deleteCheckpoints = Maps.newHashMap();
    }

    //
    // Checkpoint Metadata
    //

    CompletableFuture<Void> init(@Nullable DistributedLogManager logManager) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        executor.submit(() -> {
            this.initCheckpointLog(logManager).whenCompleteAsync(new FutureEventListener<Long>() {
                @Override
                public void onSuccess(Long lastTxId) {
                    try {
                        replayCheckpointLog(logManager, lastTxId);
                        FutureUtils.complete(future, null);
                    } catch (IOException e) {
                        FutureUtils.completeExceptionally(future, e);
                    }
                }

                @Override
                public void onFailure(Throwable throwable) {
                    FutureUtils.completeExceptionally(future, throwable);
                }
            });
        }, executor);
        return future;
    }

    private CompletableFuture<Long> initCheckpointLog(DistributedLogManager logManager) {
        return logManager.openAsyncLogWriter().thenComposeAsync(writer -> {
            this.checkpointLog = new DLCheckpointLog(
                logManager,
                writer,
                executor);
            return this.checkpointLog.writeCommand(NOP_CMD);
        }, executor);
    }

    private void replayCheckpointLog(DistributedLogManager logManager, long endTxId)
            throws IOException {
        LogReader reader = logManager.openLogReader(DLSN.InitialDLSN);
        try {
            replayCheckpointLog(reader, endTxId);
        } finally {
            reader.close();
        }
    }

    private void replayCheckpointLog(LogReader reader, long endTxId) throws IOException {
        Map<String, RocksCheckpointBuilder> inprogressCheckpoints;
        Map<String, RocksCheckpoint> completedCheckpoints;
        AtomicBoolean restoreSnapshot = new AtomicBoolean(false);

        LogRecordWithDLSN record;
        while (true) {
            record = reader.readNext(false);
            if (null == record) {
                throw new IOException("No record found in checkpoint log");
            }

            CheckpointCommand command = newCheckpointCommand(record.getPayloadBuf());
            switch (command.getCmdCase()) {
                case NOP_CMD:
                    return;
                // restore snapshot commands

            }

            if (record.getTransactionId() >= endTxId) {
                break;
            }
        }
    }

    //
    // Checkpoints
    //

    @VisibleForTesting
    RocksCheckpoint getCompletedCheckpoint(String name) {
        return completedCheckpoints.get(name);
    }

    CompletableFuture<RocksCheckpoint> checkpoint(String name) {
        final CompletableFuture<RocksCheckpoint> future;
        synchronized (this) {
            RocksCheckpoint checkpoint = completedCheckpoints.get(name);
            if (null != checkpoint) {
                return FutureUtils.value(checkpoint);
            }
            CompletableFuture<RocksCheckpoint> localFuture = inprogressCheckpoints.get(name);
            if (null != localFuture) {
                return localFuture;
            }
            future = FutureUtils.createFuture();
            inprogressCheckpoints.put(name, future);
        }

        executor.submit(() -> checkpoint(name, future));
        return future;
    }

    private void checkpoint(String name, CompletableFuture<RocksCheckpoint> future) {
        RocksCheckpointInprogress inprogress =
            new RocksCheckpointInprogress(
                checkpoint,
                name,
                new File(checkpointDir, name),
                rocksFiles,
                checkpointLog,
                executor);
        executor.submit(inprogress);
        inprogress.future().whenCompleteAsync(new FutureEventListener<RocksCheckpoint>() {
            @Override
            public void onSuccess(RocksCheckpoint checkpoint) {
                synchronized (RocksCheckpoints.this) {
                    completedCheckpoints.put(name, checkpoint);
                    inprogressCheckpoints.remove(name, future);
                }
                future.complete(checkpoint);
            }

            @Override
            public void onFailure(Throwable cause) {
                log.error("Failed to delete create a checkpoint {} under {}",
                    new Object[] { name, checkpointDir, cause });

                synchronized (RocksCheckpoints.this) {
                    inprogressCheckpoints.remove(name, future);
                }
                future.completeExceptionally(cause);
            }
        }, executor);
    }

    CompletableFuture<Void> deleteCheckpoint(String name) {
        RocksCheckpoint rc;
        CompletableFuture<Void> deleteFuture;
        synchronized (this) {
            deleteFuture = deleteCheckpoints.get(name);
            if (null != deleteFuture) {
                return deleteFuture;
            }

            rc = completedCheckpoints.get(name);
            if (null != rc) {
                deleteFuture = deleteCheckpoint(name, rc);
            } else {
                // no completed checkpoint is found
                CompletableFuture<RocksCheckpoint> inprogressCheckpoint = inprogressCheckpoints.get(name);
                if (null == inprogressCheckpoint) {
                    deleteFuture = FutureUtils.Void();
                } else {
                    deleteFuture = inprogressCheckpoint
                        .thenCompose(rocksCheckpoint -> deleteCheckpoint(name, rocksCheckpoint));
                }
            }
            deleteCheckpoints.put(name, deleteFuture);
        }
        return deleteFuture.thenApply(aVoid -> {
            synchronized (this) {
                completedCheckpoints.remove(name, rc);
                return null;
            }
        });
    }

    private CompletableFuture<Void> deleteCheckpoint(String name, RocksCheckpoint checkpoint) {
        if (null != checkpoint) {
            List<CompletableFuture<RocksFileInfo>> deleteTasks =
                Lists.newArrayListWithExpectedSize(checkpoint.getFiles().size());
            for (RocksFileInfo fi : checkpoint.getFiles().values()) {
                deleteTasks.add(rocksFiles.deleteSstFile(name, fi.getName()));
            }
            return FutureUtils.collect(deleteTasks)
                .thenCompose(ignored -> deleteLocalCheckpoint(name));
        } else {
            return deleteLocalCheckpoint(name);
        }
    }

    private CompletableFuture<Void> deleteLocalCheckpoint(String name) {
        CompletableFuture<Void> future = FutureUtils.createFuture();
        executor.submit(() -> {
            try {
                FileUtils.deleteDirectory(new File(checkpointDir, name));
                FutureUtils.complete(future, null);
            } catch (IOException e) {
                FutureUtils.completeExceptionally(future, e);
            }
        });
        return future;
    }

    CompletableFuture<Void> restoreCheckpoint(String checkpointName, File destDir) {
        RocksCheckpoint checkpoint;
        synchronized (this) {
            checkpoint = completedCheckpoints.get(checkpointName);
        }
        if (null == checkpoint) {
            return FutureUtils.exception(
                new IOException("Checkpoint " + checkpointName + " is not found"));
        }
        // we got a checkpoint, restore it
        CompletableFuture<Void> future = FutureUtils.createFuture();
        executor.submit(() -> doRestoreCheckpoint(future, checkpointName, checkpoint, destDir));
        return future;
    }

    void doRestoreCheckpoint(CompletableFuture<Void> future,
                             String checkpointName,
                             RocksCheckpoint checkpoint,
                             File destDir) {

        if (!destDir.mkdir()) {
            future.completeExceptionally(new IOException(
                "Dir " + destDir + " already exists"));
            return;
        }

        File srcDir = new File(checkpointDir, checkpointName);
        if (srcDir.exists()) {
            File[] srcFiles = srcDir.listFiles();
            String[] srcFileNames = new String[srcFiles.length];
            int idx = 0;
            for (File file : srcFiles) {
                srcFileNames[idx++] = file.getName();
            }
            try {
                HardLink.createHardLinkMult(
                    srcDir,
                    srcFileNames,
                    destDir
                );
            } catch (IOException ioe) {
                future.completeExceptionally(ioe);
                return;
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("local checkpoint {} dir doesn't exist, so do a fresh restore", srcDir);
            }
        }
        // restore the checkpoint
        RocksRestoreInprogress inprogress = new RocksRestoreInprogress(
            rocksFiles.getBk(),
            checkpoint,
            destDir,
            executor);
        executor.submit(inprogress);
        FutureUtils.proxyTo(
            inprogress.future(),
            future);
    }

    @Override
    public void close() {
        synchronized (this) {
            for (CompletableFuture<RocksCheckpoint> inprogress : inprogressCheckpoints.values()) {
                inprogress.cancel(true);
            }
            for (CompletableFuture<Void> deleteTask : deleteCheckpoints.values()) {
                deleteTask.cancel(true);
            }
        }

        RocksUtils.close(checkpoint);
    }


}
