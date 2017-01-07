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
package org.apache.distributedlog.benchmark;

import static org.apache.distributedlog.buffer.BufferPool.ALLOCATOR;

import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.util.Random;

/**
 * Utils for generating and parsing messages.
 */
public class Utils {

    static final Random RAND = new Random(System.currentTimeMillis());
    static final byte[] DATA;

    static {
        DATA = new byte[512];
        RAND.nextBytes(DATA);
    }

    public static ByteBuf generateMessage(long requestMillis, int payLoadSize) {
        ByteBuf buf = ALLOCATOR.buffer(8 + payLoadSize);
        buf.writeLong(requestMillis);

        while (buf.writableBytes() > 0) {
            int bytesToWrite = Math.min(DATA.length, buf.writableBytes());
            buf.writeBytes(DATA, 0, bytesToWrite);
        }
        return buf;
    }

    public static long parseMessage(ByteBuffer buffer) {
        return buffer.getLong();
    }

}
