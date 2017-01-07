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
package org.apache.distributedlog.io;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.distributedlog.buffer.BufferPool.ALLOCATOR;

import com.google.common.base.Stopwatch;
import java.util.concurrent.TimeUnit;
import io.netty.buffer.ByteBuf;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.bookkeeper.stats.OpStatsLogger;

/**
 * An {@code lz4} based {@link CompressionCodec} implementation.
 *
 * <p>All functions are thread safe.
 */
public class LZ4CompressionCodec implements CompressionCodec {

    // Used for compression
    private final LZ4Compressor compressor;
    // Used to decompress when the size of the output is known
    private final LZ4FastDecompressor decompressor;

    public LZ4CompressionCodec() {
        this.compressor = LZ4Factory.fastestInstance().fastCompressor();
        this.decompressor = LZ4Factory.fastestInstance().fastDecompressor();
    }

    @Override
    public ByteBuf compress(ByteBuf decompressedBuf, OpStatsLogger compressionStat) {
        checkNotNull(decompressedBuf);
        checkNotNull(compressionStat);

        Stopwatch watch = Stopwatch.createStarted();

        ByteBuf tempBuf = null;
        byte[] decompressedArray;
        int decompressedArrayOffset;
        int decompressedArrayLength = decompressedBuf.readableBytes();
        try {
            ByteBuf buf;
            if (decompressedBuf.hasArray()) {
                buf = decompressedBuf;
            } else {
                tempBuf = ALLOCATOR.heapBuffer(decompressedArrayLength);
                decompressedBuf.readBytes(tempBuf);
                buf = tempBuf;
            }
            decompressedArray = buf.array();
            decompressedArrayOffset = buf.arrayOffset() + buf.readerIndex();

            // Allocate the dest
            int maxCompressedLength = compressor.maxCompressedLength(decompressedArrayLength);
            ByteBuf compressedBuf = ALLOCATOR.heapBuffer(maxCompressedLength);
            byte[] compressedArray = compressedBuf.array();
            int compressedArrayOffset =
                    compressedBuf.arrayOffset() + compressedBuf.readerIndex();
            int compressedLength = compressor.compress(
                    decompressedArray, decompressedArrayOffset, decompressedArrayLength,
                    compressedArray, compressedArrayOffset);
            compressedBuf.setIndex(
                    compressedBuf.readerIndex(),
                    compressedBuf.readerIndex() + compressedLength);
            return compressedBuf;
        } finally {
            compressionStat.registerSuccessfulEvent(watch.elapsed(TimeUnit.MICROSECONDS));
            if (null != tempBuf) {
                tempBuf.release();
            }
        }
    }

    @Override
    // length parameter is ignored here because of the way the fastDecompressor works.
    public ByteBuf decompress(ByteBuf compressedBuf,
                              int decompressedSize,
                              OpStatsLogger decompressionStat) {
        checkNotNull(compressedBuf);
        checkArgument(decompressedSize >= 0);
        checkNotNull(decompressionStat);

        Stopwatch watch = Stopwatch.createStarted();

        ByteBuf tempBuf = null;
        byte[] compressedArray;
        int compressedArrayOffset;
        int compressedArrayLength = compressedBuf.readableBytes();
        try {
            ByteBuf buf;
            if (compressedBuf.hasArray()) {
                buf = compressedBuf;
            } else {
                tempBuf = ALLOCATOR.heapBuffer(compressedArrayLength);
                compressedBuf.readBytes(tempBuf);
                buf = tempBuf;
            }
            compressedArray = buf.array();
            compressedArrayOffset = buf.arrayOffset() + buf.readerIndex();

            // Allocate the dest
            ByteBuf decompressedBuf = ALLOCATOR.heapBuffer(decompressedSize);
            byte[] decompressedArray = decompressedBuf.array();
            int decompressedArrayOffset =
                    decompressedBuf.arrayOffset() + decompressedBuf.readerIndex();
            decompressor.decompress(
                    compressedArray, compressedArrayOffset,
                    decompressedArray, decompressedArrayOffset, decompressedSize);
            decompressedBuf.setIndex(0, decompressedSize);
            return decompressedBuf;
        } finally {
            decompressionStat.registerSuccessfulEvent(watch.elapsed(TimeUnit.MICROSECONDS));
            if (null != tempBuf) {
                tempBuf.release();
            }
        }
    }
}
