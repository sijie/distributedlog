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

import static org.apache.distributedlog.LogRecord.MAX_LOGRECORD_SIZE;
import static org.apache.distributedlog.LogRecordSet.COMPRESSION_CODEC_LZ4;
import static org.apache.distributedlog.LogRecordSet.COMPRESSION_CODEC_NONE;
import static org.apache.distributedlog.LogRecordSet.HEADER_LEN;
import static org.apache.distributedlog.LogRecordSet.METADATA_COMPRESSION_MASK;
import static org.apache.distributedlog.LogRecordSet.METADATA_VERSION_MASK;
import static org.apache.distributedlog.LogRecordSet.NULL_OP_STATS_LOGGER;
import static org.apache.distributedlog.LogRecordSet.VERSION;
import static org.apache.distributedlog.buffer.BufferPool.ALLOCATOR;

import com.twitter.util.Promise;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.distributedlog.exceptions.LogRecordTooLongException;
import org.apache.distributedlog.io.CompressionCodec;
import org.apache.distributedlog.io.CompressionUtils;
import org.apache.distributedlog.util.ReferenceCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ByteBuf} based log record set writer.
 */
class EnvelopedRecordSetWriter implements LogRecordSet.Writer, ReferenceCounted {

    private static final Logger logger = LoggerFactory.getLogger(EnvelopedRecordSetWriter.class);

    private ByteBuf buffer;
    private final List<Promise<DLSN>> promiseList;
    private final CompressionCodec.Type codec;
    private final int codecCode;
    private int count = 0;
    private ByteBuffer recordSetBuffer = null;

    EnvelopedRecordSetWriter(int initialBufferSize,
                             CompressionCodec.Type codec) {
        this.buffer = ALLOCATOR.buffer(Math.max(initialBufferSize, HEADER_LEN));
        this.promiseList = new LinkedList<Promise<DLSN>>();
        this.codec = codec;
        switch (codec) {
            case LZ4:
                this.codecCode = COMPRESSION_CODEC_LZ4;
                break;
            default:
                this.codecCode = COMPRESSION_CODEC_NONE;
                break;
        }
        this.buffer.writeInt((VERSION & METADATA_VERSION_MASK)
                | (codecCode & METADATA_COMPRESSION_MASK));
        this.buffer.writeInt(0); // count
        this.buffer.writeInt(0); // original len
        this.buffer.writeInt(0); // actual len
    }

    synchronized List<Promise<DLSN>> getPromiseList() {
        return promiseList;
    }

    @Override
    public synchronized void writeRecord(ByteBuffer record,
                                         Promise<DLSN> transmitPromise)
            throws LogRecordTooLongException {
        int logRecordSize = record.remaining();
        if (logRecordSize > MAX_LOGRECORD_SIZE) {
            throw new LogRecordTooLongException(
                    "Log Record of size " + logRecordSize + " written when only "
                            + MAX_LOGRECORD_SIZE + " is allowed");
        }
        buffer.writeInt(record.remaining());
        buffer.writeBytes(record);
        ++count;
        promiseList.add(transmitPromise);
    }

    private synchronized void satisfyPromises(long lssn, long entryId, long startSlotId) {
        long nextSlotId = startSlotId;
        for (Promise<DLSN> promise : promiseList) {
            promise.setValue(new DLSN(lssn, entryId, nextSlotId));
            nextSlotId++;
        }
        promiseList.clear();
    }

    private synchronized void cancelPromises(Throwable reason) {
        for (Promise<DLSN> promise : promiseList) {
            promise.setException(reason);
        }
        promiseList.clear();
    }

    @Override
    public int getNumBytes() {
        return buffer.readableBytes();
    }

    @Override
    public synchronized int getNumRecords() {
        return count;
    }

    @Override
    public synchronized ByteBuffer getBuffer() {
        if (null == recordSetBuffer) {
            recordSetBuffer = createBuffer();
        }
        return recordSetBuffer.duplicate();
    }

    @Override
    public void retain() {
        buffer.retain();
    }

    @Override
    public void release() {
        buffer.release();
    }

    ByteBuffer createBuffer() {
        int dataLen = buffer.readableBytes() - HEADER_LEN;
        if (COMPRESSION_CODEC_LZ4 != codecCode) {
            ByteBuffer recordSetBuffer = buffer.nioBuffer();
            // update count
            recordSetBuffer.putInt(4, count);
            // update data len
            recordSetBuffer.putInt(8, dataLen);
            recordSetBuffer.putInt(12, dataLen);
            return recordSetBuffer;
        }

        int dataOffset = HEADER_LEN;

        // compression
        ByteBuf uncompressedBuf = buffer.slice(dataOffset, dataLen);
        CompressionCodec compressor =
                    CompressionUtils.getCompressionCodec(codec);
        ByteBuf compressedBuf =
                compressor.compress(uncompressedBuf, NULL_OP_STATS_LOGGER);
        int compressedLen = compressedBuf.readableBytes();

        ByteBuffer recordSetBuffer;
        buffer.setIndex(0, HEADER_LEN);
        if (compressedLen + HEADER_LEN > buffer.capacity()) {
            buffer = Unpooled.compositeBuffer(2)
                    .addComponent(buffer)
                    .addComponent(compressedBuf)
                    .setIndex(0, compressedLen + HEADER_LEN);
        } else {
            buffer.writeBytes(compressedBuf);
        }
        // version
        recordSetBuffer = buffer.nioBuffer();
        // update count
        recordSetBuffer.putInt(4, count);
        // update data len
        recordSetBuffer.putInt(8, dataLen);
        recordSetBuffer.putInt(12, compressedLen);
        return recordSetBuffer;
    }

    @Override
    public void completeTransmit(long lssn, long entryId, long startSlotId) {
        satisfyPromises(lssn, entryId, startSlotId);
    }

    @Override
    public void abortTransmit(Throwable reason) {
        cancelPromises(reason);
    }
}
