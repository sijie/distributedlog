package org.apache.distributedlog.buffer;

import io.netty.buffer.PooledByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Buffer Pool.
 */
public class BufferPool {

    private static final Logger logger = LoggerFactory.getLogger(BufferPool.class);

    public static final PooledByteBufAllocator ALLOCATOR;

    static {
        logger.info("Initializing buffer pool ...");
        int pageSize;
        try {
            try {
                pageSize = Integer.parseInt(System.getProperty("dl.buffer.allocator.pageSize", "8192"));
            } catch (NumberFormatException nfe) {
                pageSize = 8192;
            }
        } catch (Throwable t) {
            logger.info("Encountered exception on initializing buffer pool", t);
            throw t;
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
        logger.info("Initialized buffer pool.");
    }
}
