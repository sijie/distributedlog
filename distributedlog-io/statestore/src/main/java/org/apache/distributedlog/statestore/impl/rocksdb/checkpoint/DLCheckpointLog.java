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

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.distributedlog.LogRecord;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.statestore.impl.rocksdb.RocksUtils;
import org.apache.distributedlog.statestore.proto.CheckpointCommand;
import org.apache.distributedlog.util.Utils;

/**
 * A checkpoint log implemented using distributedlog.
 */
@Slf4j
class DLCheckpointLog implements CheckpointLog {

    private final DistributedLogManager logManager;
    private final AsyncLogWriter writer;
    private final ScheduledExecutorService executor;
    private long lastTxId;

    DLCheckpointLog(DistributedLogManager logManager,
                    AsyncLogWriter writer,
                    ScheduledExecutorService executor) {
        this.logManager = logManager;
        this.writer = writer;
        this.executor = executor;
        this.lastTxId = writer.getLastTxId();
        if (lastTxId < 0L) {
            lastTxId = 0L;
        }
        log.info("Initialized the checkpoint log writer : last txId = {}", lastTxId);
    }


    @Override
    public synchronized CompletableFuture<Long> writeCommand(CheckpointCommand command) {
        ByteBuf recordBuf;
        try {
            recordBuf = RocksUtils.newLogRecordBuf(command);
        } catch (IOException ioe) {
            return FutureUtils.exception(ioe);
        }

        long txId = ++lastTxId;
        return FutureUtils.ensure(
            writer.write(new LogRecord(txId, recordBuf))
                .thenApply(dlsn -> txId),
            () -> recordBuf.release());
    }

    @Override
    public CompletableFuture<Void> asyncClose() {
        return Utils.closeSequence(
            executor,
            writer,
            logManager);
    }
}
