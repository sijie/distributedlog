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
import org.apache.distributedlog.util.BitMaskUtils;

/**
 * An enveloped entry written to BookKeeper.
 *
 * Data type in brackets. Interpretation should be on the basis of data types and not individual
 * bytes to honor Endianness.
 *
 * Entry Structure:
 * ---------------
 * Bytes 0                                  : Version (Byte)
 * Bytes 1 - (DATA = 1+Header.length-1)     : Header (Integer)
 * Bytes DATA - DATA+3                      : Payload Length (Integer)
 * BYTES DATA+4 - DATA+4+payload.length-1   : Payload (Byte[])
 *
 * V1 Header Structure: // Offsets relative to the start of the header.
 * -------------------
 * Bytes 0 - 3                              : Flags (Integer)
 * Bytes 4 - 7                              : Original payload size before compression (Integer)
 *
 *      Flags: // 32 Bits
 *      -----
 *      0 ... 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
 *                                      |_|
 *                                       |
 *                               Compression Type
 *
 *      Compression Type: // 2 Bits (Least significant)
 *      ----------------
 *      00      : No Compression
 *      01      : LZ4 Compression
 *      10      : Unused
 *      11      : Unused
 */
public class EnvelopedEntry {

    public static final int VERSION_LENGTH = 1; // One byte long
    public static final byte VERSION_ONE = 1;

    public static final byte LOWEST_SUPPORTED_VERSION = VERSION_ONE;
    public static final byte HIGHEST_SUPPORTED_VERSION = VERSION_ONE;
    public static final byte CURRENT_VERSION = VERSION_ONE;

    public static final int HEADER_LEN = 2 * (Integer.SIZE / Byte.SIZE);
    public static final int PAYLOAD_OFFSET = VERSION_LENGTH + HEADER_LEN;
    public static final int PAYLOAD_LEN_OFFSET = VERSION_LENGTH + (Integer.SIZE / Byte.SIZE);

    public static class Header {
        public static final int COMPRESSION_CODEC_MASK = 0x3;
        public static final int COMPRESSION_CODEC_NONE = 0x0;
        public static final int COMPRESSION_CODEC_LZ4 = 0x1;

        public static int getFlag(CompressionCodec.Type compressionType) {
            int flags = 0;
            switch (compressionType) {
                case NONE:
                    flags = (int) BitMaskUtils.set(flags, COMPRESSION_CODEC_MASK,
                            COMPRESSION_CODEC_NONE);
                    break;
                case LZ4:
                    flags = (int) BitMaskUtils.set(flags, COMPRESSION_CODEC_MASK,
                            COMPRESSION_CODEC_LZ4);
                    break;
                default:
                    throw new RuntimeException(String.format("Unknown Compression Type: %s",
                                                             compressionType));
            }
            return flags;
        }
    }

}
