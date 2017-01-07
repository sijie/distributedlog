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

import org.apache.distributedlog.io.CompressionCodec;
import org.apache.distributedlog.io.CompressionUtils;
import org.apache.distributedlog.util.BitMaskUtils;
import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.stats.StatsLogger;

import java.io.IOException;

import static org.apache.distributedlog.EnvelopedEntry.*;

/**
 * Record reader to read records from an enveloped entry buffer.
 */
class EnvelopedEntryReader implements Entry.Reader, RecordStream {

    private final long logSegmentSeqNo;
    private final long entryId;
    private final ByteBuf inBuf;
    private final LogRecord.Reader reader;

    // slot id
    private long slotId = 0;

    EnvelopedEntryReader(long logSegmentSeqNo,
                         long entryId,
                         long startSequenceId,
                         ByteBuf in,
                         boolean deserializeRecordSet,
                         StatsLogger statsLogger)
            throws IOException {
        this.logSegmentSeqNo = logSegmentSeqNo;
        this.entryId = entryId;

        // read the data
        try {
            byte version = in.readByte();
            if (version != CURRENT_VERSION) {
                throw new IOException(String.format("Version mismatch while reading. Received: %d"
                    + " Required: %d", version, CURRENT_VERSION));
            }
            int flags = in.readInt();
            int decompressedSize = in.readInt();
            int compressionType = (int) BitMaskUtils.get(flags, Header.COMPRESSION_CODEC_MASK);
            if (compressionType == Header.COMPRESSION_CODEC_NONE) {
                if (in.readableBytes() != decompressedSize) {
                    throw new IOException("Inconsistent data length found for a non-compressed record set : original = "
                            + decompressedSize + ", actual = " + in.readableBytes());
                }
                in.retain();
                inBuf = in;
            } else if (compressionType == Header.COMPRESSION_CODEC_LZ4) {
                CompressionCodec codec = CompressionUtils.getCompressionCodec(CompressionCodec.Type.LZ4);
                inBuf = codec.decompress(in, decompressedSize, statsLogger.getOpStatsLogger("decompression_time"));
            } else {
                throw new IOException(String.format("Unsupported Compression Type: %s",
                                                    compressionType));
            }
        } finally {
            in.release();
        }
        this.reader = new LogRecord.Reader(
                this,
                inBuf,
                startSequenceId,
                deserializeRecordSet);
    }

    @Override
    public long getLSSN() {
        return logSegmentSeqNo;
    }

    @Override
    public long getEntryId() {
        return entryId;
    }

    @Override
    public LogRecordWithDLSN nextRecord() throws IOException {
        return reader.readOp();
    }

    @Override
    public boolean skipTo(long txId) throws IOException {
        return reader.skipTo(txId, true);
    }

    @Override
    public boolean skipTo(DLSN dlsn) throws IOException {
        return reader.skipTo(dlsn);
    }

    //
    // Record Stream
    //

    @Override
    public void advance(int numRecords) {
        slotId += numRecords;
    }

    @Override
    public DLSN getCurrentPosition() {
        return new DLSN(logSegmentSeqNo, entryId, slotId);
    }

    @Override
    public String getName() {
        return "EnvelopedReader";
    }

    @Override
    public void retain() {
        inBuf.retain();
    }

    @Override
    public void release() {
        inBuf.release();
    }
}
