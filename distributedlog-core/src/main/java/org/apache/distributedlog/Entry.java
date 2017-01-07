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

import org.apache.distributedlog.exceptions.LogRecordTooLongException;
import org.apache.distributedlog.exceptions.WriteException;
import org.apache.distributedlog.io.CompressionCodec;
import org.apache.distributedlog.util.ReferenceCounted;
import com.twitter.util.Promise;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

import static org.apache.distributedlog.buffer.BufferPool.ALLOCATOR;

/**
 * A set of {@link LogRecord}s.
 */
public class Entry {

    private static final Logger logger = LoggerFactory.getLogger(Entry.class);

    /**
     * Create a new log record set.
     *
     * @param logName
     *          name of the log
     * @param initialBufferSize
     *          initial buffer size
     * @param codec
     *          compression codec
     * @param statsLogger
     *          stats logger to receive stats
     * @return writer to build a log record set.
     */
    public static Writer newEntry(
            String logName,
            int initialBufferSize,
            CompressionCodec.Type codec,
            StatsLogger statsLogger) {
        return new EnvelopedEntryWriter(
                logName,
                initialBufferSize,
                codec,
                statsLogger);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Build the record set object.
     */
    public static class Builder {

        private long logSegmentSequenceNumber = -1;
        private long entryId = -1;
        private long startSequenceId = Long.MIN_VALUE;
        private ByteBuf inBuf = null;
        private boolean deserializeRecordSet = true;

        private Builder() {}

        /**
         * Reset the builder.
         *
         * @return builder
         */
        public Builder reset() {
            logSegmentSequenceNumber = -1;
            entryId = -1;
            startSequenceId = Long.MIN_VALUE;
            inBuf = null;
            return this;
        }

        /**
         * Set the segment info of the log segment that this record
         * set belongs to.
         *
         * @param lssn
         *          log segment sequence number
         * @param startSequenceId
         *          start sequence id of this log segment
         * @return builder
         */
        public Builder setLogSegmentInfo(long lssn, long startSequenceId) {
            this.logSegmentSequenceNumber = lssn;
            this.startSequenceId = startSequenceId;
            return this;
        }

        /**
         * Set the entry id of this log record set.
         *
         * @param entryId
         *          entry id assigned for this log record set.
         * @return builder
         */
        public Builder setEntryId(long entryId) {
            this.entryId = entryId;
            return this;
        }

        /**
         * Set the serialized bytes data of this record set.
         *
         * @param data
         *          serialized bytes data of this record set.
         * @param offset
         *          offset of the bytes data
         * @param length
         *          length of the bytes data
         * @return builder
         */
        public Builder setData(byte[] data, int offset, int length) {
            this.inBuf = Unpooled.wrappedBuffer(data, offset, length);
            return this;
        }

        /**
         * Set the input stream of the serialized bytes data of this record set.
         *
         * @param in
         *          input stream
         * @return builder
         */
        public Builder setInputStream(InputStream in) {
            try {
                int size = in.available();
                this.inBuf = ALLOCATOR.buffer(size);
                this.inBuf.writeBytes(in, size);
            } catch (IOException e) {
                logger.error("Unexpected IOException while reading from input stream", e);
                this.inBuf = ALLOCATOR.buffer(0);
            }
            return this;
        }

        public Builder setData(ByteBuf buf) {
            this.inBuf = buf;
            return this;
        }

        /**
         * Enable/disable deserialize record set.
         *
         * @param enabled
         *          flag to enable/disable dserialize record set.
         * @return builder
         */
        public Builder deserializeRecordSet(boolean enabled) {
            this.deserializeRecordSet = enabled;
            return this;
        }

        public Entry.Reader buildReader() throws IOException {
            return new EnvelopedEntryReader(
                    logSegmentSequenceNumber,
                    entryId,
                    startSequenceId,
                    inBuf,
                    deserializeRecordSet,
                    NullStatsLogger.INSTANCE);
        }

    }

    /**
     * Writer to append {@link LogRecord}s to {@link Entry}.
     */
    public interface Writer extends EntryBuffer {

        /**
         * Whether the writer can write the record.
         *
         * @param logRecordSize the size of the log record.
         * @return true if the writer can accept the record; otherwise false.
         */
        boolean canWriteRecord(int logRecordSize);

        /**
         * Write a {@link LogRecord} to this record set.
         *
         * @param record
         *          record to write
         * @param transmitPromise
         *          callback for transmit result. the promise is only
         *          satisfied when this record set is transmitted.
         * @throws LogRecordTooLongException if the record is too long
         * @throws WriteException when encountered exception writing the record
         */
        void writeRecord(LogRecord record, Promise<DLSN> transmitPromise)
                throws LogRecordTooLongException, WriteException;

    }

    /**
     * Reader to read {@link LogRecord}s from this record set.
     */
    public interface Reader extends ReferenceCounted {

        /**
         * Get the log segment sequence number.
         *
         * @return the log segment sequence number.
         */
        long getLSSN();

        /**
         * Return the entry id.
         *
         * @return the entry id.
         */
        long getEntryId();

        /**
         * Read next log record from this record set.
         *
         * <p>The flag {@code allocateBuffer} controls whether to allocate new buffer for the returned log record.
         * If the log record is returned to user directly, the caller should set {@code allocateBuffer}
         * to true. If the log record is used internally, the caller can set the {@code allocateBuffer}
         * to false.
         *
         * @return next log record from this record set.
         */
        LogRecordWithDLSN nextRecord()
                throws IOException;

        /**
         * Skip the reader to the record whose DLSN is <code>dlsn</code>.
         *
         * @param dlsn
         *          DLSN to skip to.
         * @return true if skip succeeds, otherwise false.
         * @throws IOException
         */
        boolean skipTo(DLSN dlsn) throws IOException;

    }

}
