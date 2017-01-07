package org.apache.distributedlog.buffer;

import io.netty.buffer.PooledByteBufAllocator;

/**
 * Buffer Pool.
 */
public class BufferPool {

    public static final PooledByteBufAllocator ALLOCATOR = new PooledByteBufAllocator(
            true,   // prefer direct
            0,      // nHeapArenas,
            1,      // nDirectArena
            8192,   // pageSize
            11,     // maxOrder
            64,     // tinyCacheSize
            32,     // smallCacheSize
            8       // normalCacheSize
    );

}
