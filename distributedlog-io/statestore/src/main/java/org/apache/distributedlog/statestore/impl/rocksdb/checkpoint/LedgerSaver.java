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
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.distributedlog.util.Utils;

/**
 * A task to copy a ledger to a file
 */
@Slf4j
class LedgerSaver implements Runnable, FutureEventListener<Iterable<LedgerEntry>> {

    private static final int MAX_OUTSTANDING_READS = 16;

    private final BookKeeper bkc;
    private final RocksFileInfo rocksFileInfo;
    private final File destFile;
    private final ScheduledExecutorService ioScheduler;
    private final CompletableFuture<RocksFileInfo> future;

    // state
    private AtomicReference<Throwable> lastException = new AtomicReference<>(null);
    private FileChannel destChannel;
    private ReadHandle srcLh;

    private boolean canComplete = false;
    private int pendingReads = 0;
    private long nextEntryId = 0L;
    private long lac = -1L;
    private CompletableFuture<Iterable<LedgerEntry>> lastReadFuture;

    LedgerSaver(BookKeeper bkc,
                RocksFileInfo fileInfo,
                File destDir,
                ScheduledExecutorService ioScheduler) {
        this.bkc = bkc;
        this.rocksFileInfo = fileInfo;
        this.destFile = new File(destDir, fileInfo.getName());
        this.ioScheduler = ioScheduler;
        this.future = FutureUtils.createFuture();

        // register an ensure block to clean up resources
        FutureUtils.ensure(this.future, () -> cleanup());
    }

    CompletableFuture<RocksFileInfo> future() {
        return future;
    }

    @Override
    public void run() {
        // 1. open the dest file
        try {
            this.destChannel = new RandomAccessFile(destFile, "rw").getChannel();
            this.destChannel.position(0L);
        } catch (IOException e) {
            future.completeExceptionally(e);
            return;
        }

        // nothing to copy
        if (rocksFileInfo.getLength() == 0 || rocksFileInfo.getLedgerId() == -1L) {
            complete();
            return;
        }

        // 2. create the src ledger
        CompletableFuture<ReadHandle> openFuture = this.bkc.newOpenLedgerOp()
            .withLedgerId(rocksFileInfo.getLedgerId())
            .withDigestType(DigestType.CRC32)
            .withPassword(new byte[0])
            .withRecovery(true)
            .execute();
        openFuture.whenCompleteAsync(new FutureEventListener<ReadHandle>() {
            @Override
            public void onSuccess(ReadHandle rh) {
                copyDataFrom(rh);
            }

            @Override
            public void onFailure(Throwable throwable) {
                future.completeExceptionally(throwable);
            }
        }, ioScheduler);
    }

    // 3. copy the data from src lh to dest file
    private void copyDataFrom(ReadHandle rh) {
        this.srcLh = rh;
        this.lac = rh.getLastAddConfirmed();

        readNextBatches(rh);
    }

    private void readNextBatches(ReadHandle rh) {
        // if nextEntryId is already larger than lac, we return immediately
        // avoid to attach a completion block on the last read again and again
        if (nextEntryId > lac) {
            return;
        }

        // too many outstanding read requests already
        if (pendingReads >= MAX_OUTSTANDING_READS) {
            return;
        }

        while (nextEntryId <= lac) {
            CompletableFuture<Iterable<LedgerEntry>> readFuture =
                rh.read(nextEntryId, nextEntryId);
            readFuture.whenCompleteAsync(this, ioScheduler);
            int numPendingReads = ++pendingReads;
            ++nextEntryId;
            lastReadFuture = readFuture;

            if (numPendingReads >= MAX_OUTSTANDING_READS) {
                break;
            }
        }

        // reach end of the ledger after issues all the reads
        if (nextEntryId > lac) {
            if (null == lastReadFuture) { // no entries to read
                complete();
                return;
            }
            lastReadFuture.thenAcceptAsync(ignored -> {
                canComplete = true;
                complete();
            }, ioScheduler);
        }
    }

    @Override
    public void onSuccess(Iterable<LedgerEntry> ledgerEntries) {
        --pendingReads;

        // already hit exception, stop writing
        if (null != lastException.get()) {
            return;
        }

        // issue next read first before writing the data, so we can pipeline I/O
        readNextBatches(srcLh);

        Iterator<LedgerEntry> iter = ledgerEntries.iterator();
        try {
            while (iter.hasNext()) {
                LedgerEntry entry = iter.next();
                ByteBuf buf = entry.getEntryBuffer();
                try {
                    ByteBuffer nioBuf = buf.nioBuffer();
                    while (nioBuf.hasRemaining()) {
                        destChannel.write(nioBuf);
                    }
                } catch (IOException ioe) {
                    fail(ioe);
                    return;
                } finally {
                    buf.release();
                }
            }
        } finally {
            while (iter.hasNext()) {
                LedgerEntry entry = iter.next();
                entry.getEntryBuffer().release();
            }
        }
        if (canComplete) {
            complete();
        }
    }

    @Override
    public void onFailure(Throwable throwable) {
        --pendingReads;

        fail(throwable);
    }

    private void complete() {
        if (pendingReads > 0) {
            return;
        }
        try {
            destChannel.force(true);
            future.complete(rocksFileInfo);
        } catch (IOException e) {
            log.error("Failed to flush file {} while copying data from ledger {}",
                new Object[] { destFile, });
            fail(e);
        }

    }

    private void fail(Throwable cause) {
        if (lastException.compareAndSet(null, cause)) {
            log.error("Encountered error on copying data from ledger {} to file {}",
                new Object[] { rocksFileInfo.getLedgerId(), destFile, cause });
        }
        future.completeExceptionally(cause);
    }

    private void cleanup() {
        if (null != this.destChannel) {
            Utils.close(this.destChannel);
        }
        if (null != srcLh) {
            srcLh.asyncClose();
        }
    }
}
