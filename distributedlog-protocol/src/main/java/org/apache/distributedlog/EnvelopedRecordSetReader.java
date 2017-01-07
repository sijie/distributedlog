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

import static org.apache.distributedlog.LogRecordSet.COMPRESSION_CODEC_LZ4;
import static org.apache.distributedlog.LogRecordSet.METADATA_COMPRESSION_MASK;
import static org.apache.distributedlog.LogRecordSet.METADATA_VERSION_MASK;
import static org.apache.distributedlog.LogRecordSet.NULL_OP_STATS_LOGGER;
import static org.apache.distributedlog.LogRecordSet.VERSION;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.distributedlog.io.CompressionCodec;
import org.apache.distributedlog.io.CompressionUtils;
import org.apache.distributedlog.util.ReferenceCounted;

/**
 * Record reader to read records from an enveloped entry buffer.
 */
class EnvelopedRecordSetReader implements LogRecordSet.Reader, ReferenceCounted {

    private final long logSegmentSeqNo;
    private final long entryId;
    private final long transactionId;
    private final long startSequenceId;
    private final long parentMetadata;
    private int numRecords;
    private final ByteBuf reader;

    // slot id
    private long slotId;
    private int position;

    EnvelopedRecordSetReader(long logSegmentSeqNo,
                             long entryId,
                             long transactionId,
                             long startSlotId,
                             int startPositionWithinLogSegment,
                             long startSequenceId,
                             long parentMetadata,
                             ByteBuf in)
            throws IOException {
        this.logSegmentSeqNo = logSegmentSeqNo;
        this.entryId = entryId;
        this.transactionId = transactionId;
        this.slotId = startSlotId;
        this.position = startPositionWithinLogSegment;
        this.startSequenceId = startSequenceId;
        this.parentMetadata = parentMetadata;

        // read data
        try {
            int metadata = in.readInt();
            int version = metadata & METADATA_VERSION_MASK;
            if (version != VERSION) {
                throw new IOException(String.format("Version mismatch while reading. Received: %d,"
                        + " Required: %d", version, VERSION));
            }
            int codecCode = metadata & METADATA_COMPRESSION_MASK;
            this.numRecords = in.readInt();
            int originDataLen = in.readInt();
            int actualDataLen = in.readInt();

            if (COMPRESSION_CODEC_LZ4 == codecCode) {
                CompressionCodec codec = CompressionUtils.getCompressionCodec(CompressionCodec.Type.LZ4);
                this.reader = codec.decompress(
                        in,
                        originDataLen,
                        NULL_OP_STATS_LOGGER);
            } else {
                if (originDataLen != actualDataLen) {
                    throw new IOException("Inconsistent data length found for a non-compressed record set : original = "
                            + originDataLen + ", actual = " + actualDataLen);
                }
                in.retain();
                this.reader = in;
            }
        } finally {
            in.release();
       }
    }

    @Override
    public LogRecordWithDLSN nextRecord() throws IOException {
        if (numRecords <= 0) {
            return null;
        }

        DLSN dlsn = new DLSN(logSegmentSeqNo, entryId, slotId);

        ByteBuffer payload = LogRecord.Reader.readPayload(reader);

        long metadata = LogRecord.setPositionWithinLogSegment(parentMetadata, position);
        LogRecordWithDLSN record = new LogRecordWithDLSN(
                dlsn,
                startSequenceId,
                transactionId,
                payload,
                metadata);

        ++slotId;
        ++position;
        --numRecords;

        return record;
    }

    @Override
    public void retain() {
        reader.retain();
    }

    @Override
    public void release() {
        reader.release();
    }
}
