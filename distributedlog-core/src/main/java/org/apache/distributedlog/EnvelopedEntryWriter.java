/**
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
package org.apache.distributedlog;

import static org.apache.distributedlog.LogRecord.MAX_LOGRECORDSET_SIZE;
import static org.apache.distributedlog.LogRecord.MAX_LOGRECORD_SIZE;
import static org.apache.distributedlog.buffer.BufferPool.ALLOCATOR;

import com.twitter.util.Promise;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.distributedlog.Entry.Writer;
import org.apache.distributedlog.exceptions.InvalidEnvelopedEntryException;
import org.apache.distributedlog.exceptions.LogRecordTooLongException;
import org.apache.distributedlog.exceptions.WriteException;
import org.apache.distributedlog.io.CompressionCodec;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.distributedlog.io.CompressionUtils;
import org.apache.distributedlog.util.ReferenceCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ByteBuf} based log record set writer.
 */
class EnvelopedEntryWriter implements Writer, ReferenceCounted {

    static final Logger logger = LoggerFactory.getLogger(EnvelopedEntryWriter.class);

    private static class WriteRequest {

        private final int numRecords;
        private final Promise<DLSN> promise;

        WriteRequest(int numRecords, Promise<DLSN> promise) {
            this.numRecords = numRecords;
            this.promise = promise;
        }

    }

    private final String logName;
    private ByteBuf buffer;
    private LogRecord.Writer writer;
    private final List<WriteRequest> writeRequests;
    private final CompressionCodec.Type codec;
    private final StatsLogger statsLogger;
    private int count = 0;
    private boolean hasUserData = false;
    private long maxTxId = Long.MIN_VALUE;

    EnvelopedEntryWriter(String logName,
                         int initialBufferSize,
                         CompressionCodec.Type codec,
                         StatsLogger statsLogger) {
        this.logName = logName;
        this.writeRequests = new LinkedList<WriteRequest>();
        this.codec = codec;
        this.statsLogger = statsLogger;
        newBufferAndWriter(initialBufferSize);
    }

    private void newBufferAndWriter(int initialBufferSize) {
        /**
         * TODO: use direct buffer when bookkeeper's api support ByteBuf or ByteBuffer.
         */
        this.buffer = ALLOCATOR.heapBuffer(
                Math.max(initialBufferSize + EnvelopedEntry.PAYLOAD_OFFSET, EnvelopedEntry.PAYLOAD_OFFSET),
                MAX_LOGRECORDSET_SIZE + EnvelopedEntry.PAYLOAD_OFFSET);
        // write the version
        buffer.writeByte(EnvelopedEntry.CURRENT_VERSION);
        // flags
        buffer.writeInt(EnvelopedEntry.Header.getFlag(codec));
        // original length
        buffer.writeInt(0);
        this.writer = new LogRecord.Writer(buffer);
    }

    @Override
    public boolean canWriteRecord(int logRecordSize) {
        if (logRecordSize <= buffer.writableBytes()) {
            return true;
        } else if (count > 0) {
            // there is already more than one record and if the current buffer doesn't fit the record
            // return false. so the writer can transmit this entry.
            return false;
        } else {
            // no record in the buffer, try to enlarge the buffer
            this.buffer.ensureWritable(logRecordSize, true);
            if (logRecordSize > buffer.writableBytes()) {
                this.buffer.release();
                newBufferAndWriter(logRecordSize);
            }
            return true;
        }
    }

    @Override
    public synchronized void writeRecord(LogRecord record,
                                         Promise<DLSN> transmitPromise)
            throws LogRecordTooLongException, WriteException {
        int logRecordSize = record.getPersistentSize();
        if (logRecordSize > MAX_LOGRECORD_SIZE) {
            throw new LogRecordTooLongException(
                    "Log Record of size " + logRecordSize + " written when only "
                            + MAX_LOGRECORD_SIZE + " is allowed");
        }

        try {
            this.writer.writeOp(record);
            int numRecords = 1;
            if (!record.isControl()) {
                hasUserData = true;
            }
            if (record.isRecordSet()) {
                numRecords = LogRecordSet.numRecords(record);
            }
            count += numRecords;
            writeRequests.add(new WriteRequest(numRecords, transmitPromise));
            maxTxId = Math.max(maxTxId, record.getTransactionId());
        } catch (IOException e) {
            logger.error("Failed to append record to record set of {} : ",
                    logName, e);
            throw new WriteException(logName, "Failed to append record to record set of "
                    + logName);
        }
    }

    private synchronized void satisfyPromises(long lssn, long entryId) {
        long nextSlotId = 0;
        for (WriteRequest request : writeRequests) {
            request.promise.setValue(new DLSN(lssn, entryId, nextSlotId));
            nextSlotId += request.numRecords;
        }
        writeRequests.clear();
    }

    private synchronized void cancelPromises(Throwable reason) {
        for (WriteRequest request : writeRequests) {
            request.promise.setException(reason);
        }
        writeRequests.clear();
    }

    @Override
    public synchronized long getMaxTxId() {
        return maxTxId;
    }

    @Override
    public synchronized boolean hasUserRecords() {
        return hasUserData;
    }

    @Override
    public int getNumBytes() {
        return writer.getPendingBytes() - EnvelopedEntry.PAYLOAD_OFFSET;
    }

    @Override
    public synchronized int getNumRecords() {
        return count;
    }

    @Override
    public synchronized ByteBuf getBuffer() throws InvalidEnvelopedEntryException, IOException {
        // We can't escape this allocation because things need to be read from one byte array
        // and then written to another. This is the destination.
        int length = getNumBytes();

        if (CompressionCodec.Type.NONE == codec) {
            buffer.markWriterIndex();
            buffer.setIndex(0, EnvelopedEntry.PAYLOAD_LEN_OFFSET);
            buffer.writeInt(length);
            buffer.resetWriterIndex();
            return buffer;
        }

        CompressionCodec compressor = CompressionUtils.getCompressionCodec(codec);
        // compress the data
        buffer.setIndex(EnvelopedEntry.PAYLOAD_LEN_OFFSET, buffer.writerIndex());
        ByteBuf compressedBuf = compressor.compress(buffer, statsLogger.getOpStatsLogger("compression_time"));
        int compressedDataLen = compressedBuf.readableBytes();
        // reset the index for the buffer
        buffer.setIndex(0, EnvelopedEntry.PAYLOAD_LEN_OFFSET);
        buffer.writeInt(compressedDataLen);
        buffer = Unpooled.compositeBuffer(2)
                .addComponent(buffer)
                .addComponent(compressedBuf)
                .setIndex(0, EnvelopedEntry.PAYLOAD_OFFSET + compressedDataLen);

        return buffer;
    }

    @Override
    public void retain() {
        if (null != buffer) {
            buffer.retain();
        }
    }

    @Override
    public void release() {
        if (null != buffer) {
            buffer.release();
        }
    }

    @Override
    public synchronized DLSN finalizeTransmit(long lssn, long entryId) {
        return new DLSN(lssn, entryId, count - 1);
    }

    @Override
    public void completeTransmit(long lssn, long entryId) {
        satisfyPromises(lssn, entryId);
        release();
    }

    @Override
    public void abortTransmit(Throwable reason) {
        cancelPromises(reason);
        release();
    }
}
