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
import io.netty.buffer.PooledByteBufAllocator;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.distributedlog.util.Utils;

/**
 * A task to copy a file to a ledger.
 */
@Slf4j
class LedgerCopier implements Runnable {

    private static final int BUFFER_SIZE = 128 * 1024;

    private final File srcFile;
    private final BookKeeper bkc;
    private final ScheduledExecutorService ioScheduler;
    private final CompletableFuture<RocksFileInfo> future;
    private final int ensembleSize;
    private final int writeQuorumSize;
    private final int ackQuorumSize;

    // state
    private AtomicReference<Throwable> lastException = new AtomicReference<>(null);
    private FileChannel srcChannel;
    private WriteHandle destLh;

    LedgerCopier(BookKeeper bkc,
                 File srcFile,
                 ScheduledExecutorService ioScheduler,
                 int ensmebleSize,
                 int writeQuorumSize,
                 int ackQuorumSize) {
        this.bkc = bkc;
        this.srcFile = srcFile;
        this.ioScheduler = ioScheduler;
        this.future = FutureUtils.createFuture();
        this.ensembleSize = ensmebleSize;
        this.writeQuorumSize = writeQuorumSize;
        this.ackQuorumSize = ackQuorumSize;

        // register an ensure block to clean up resources
        FutureUtils.ensure(this.future, () -> cleanup());
    }

    CompletableFuture<RocksFileInfo> future() {
        return future;
    }

    @Override
    public void run() {
        if (this.srcFile.length() == 0) {
            RocksFileInfo info = new RocksFileInfo(
                srcFile.getName(),
                -1L,
                srcFile.length());
            // nothing to copy
            future.complete(info);
            return;
        }

        // 1. open the src file
        try {
            this.srcChannel = new RandomAccessFile(srcFile, "r").getChannel();
            this.srcChannel.position(0L);
        } catch (IOException e) {
            future.completeExceptionally(e);
            return;
        }
        // 2. create the dest ledger
        CompletableFuture<WriteHandle> openFuture = this.bkc.newCreateLedgerOp()
            .withPassword(new byte[0])
            .withDigestType(DigestType.CRC32)
            .withEnsembleSize(ensembleSize)
            .withWriteQuorumSize(writeQuorumSize)
            .withAckQuorumSize(ackQuorumSize)
            .execute();
        openFuture.whenCompleteAsync(new FutureEventListener<WriteHandle>() {
            @Override
            public void onSuccess(WriteHandle wh) {
                if (log.isDebugEnabled()) {
                    log.debug("Successfully open ledger {} for copying data from file {}",
                        wh.getId(), srcFile);
                }

                copyData(wh);
            }

            @Override
            public void onFailure(Throwable throwable) {
                log.error("Failed to open a ledger for copying data from file {}",
                    srcFile, throwable);

                future.completeExceptionally(throwable);
            }
        }, ioScheduler);
    }

    // 3. copy the data from src stream to dest lh
    private void copyData(WriteHandle wh) {
        this.destLh = wh;
        long length = srcFile.length();

        // loop to read data and send data to the bookies
        CompletableFuture<Long> lastWriteFuture = null;
        while (length > 0) {
            if (null != lastException.get()) {
                deleteLedgerAndFail(wh.getId(), lastException.get());
                return;
            }

            int numBytesToRead = length >= BUFFER_SIZE ? BUFFER_SIZE : (int) length;
            ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(numBytesToRead);
            buf.writerIndex(numBytesToRead);
            ByteBuffer nioBuf = buf.nioBuffer();
            try {
                while (nioBuf.hasRemaining()) {
                    this.srcChannel.read(nioBuf);
                }
            } catch (IOException ioe) {
                buf.release();
                deleteLedgerAndFail(wh.getId(), ioe);
                return;
            }
            lastWriteFuture = wh.append(buf).whenCompleteAsync(new FutureEventListener<Long>() {
                @Override
                public void onSuccess(Long entryId) {
                    buf.release();
                }

                @Override
                public void onFailure(Throwable throwable) {
                    buf.release();
                    if (lastException.compareAndSet(null, throwable)) {
                        log.error("Encountered error on copying data from file {} to ledger {}",
                            new Object[] { srcFile, wh.getId(), throwable });
                    }
                }
            }, ioScheduler);

            length -= numBytesToRead;
        }

        RocksFileInfo info = new RocksFileInfo(
                srcFile.getName(),
                wh.getId(),
                srcFile.length());
        // when reach here, we have sent all the data to a given ledger. we wait until the writes are completed.
        if (null == lastWriteFuture) { // nothing to copy
            future.complete(info);
            return;
        }
        lastWriteFuture.whenCompleteAsync(new FutureEventListener<Long>() {
            @Override
            public void onSuccess(Long eid) {
                future.complete(info);
            }

            @Override
            public void onFailure(Throwable throwable) {
                deleteLedgerAndFail(wh.getId(), throwable);
            }
        }, ioScheduler);
    }

    // 4. delete ledger on failure
    private void deleteLedgerAndFail(long ledgerId, Throwable cause) {
        FutureUtils.ensure(
            bkc.newDeleteLedgerOp().withLedgerId(ledgerId).execute(),
            () -> future.completeExceptionally(cause));
    }

    // 5. close ledger to release resources
    private void cleanup() {
        if (null != this.srcChannel) {
            Utils.close(this.srcChannel);
        }
        if (null != destLh) {
            destLh.asyncClose();
        }
    }

}
