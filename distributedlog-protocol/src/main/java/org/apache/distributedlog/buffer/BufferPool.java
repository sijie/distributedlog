package org.apache.distributedlog.buffer;

import io.netty.buffer.PooledByteBufAllocator;

/**
 * Buffer Pool.
 */
public class BufferPool {

    public static final PooledByteBufAllocator ALLOCATOR;

    static {
        int pageSize;
        try {
            pageSize = Integer.parseInt(System.getProperty("dl.buffer.allocator.pageSize", "8192"));
        } catch (NumberFormatException nfe) {
            pageSize = 8192;
        }
        ALLOCATOR = new PooledByteBufAllocator(
                true,   // prefer direct
                0,      // nHeapArenas,
                1,      // nDirectArena
                pageSize,   // pageSize
                11,     // maxOrder
                64,     // tinyCacheSize
                32,     // smallCacheSize
                8       // normalCacheSize
        );
    }
}
