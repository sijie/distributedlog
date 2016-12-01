package com.twitter.distributedlog.metadata;

import com.google.common.annotations.Beta;
import com.google.common.base.Optional;
import com.twitter.distributedlog.impl.metadata.ZKLogMetadataForReader;
import com.twitter.distributedlog.impl.metadata.ZKLogMetadataForWriter;
import com.twitter.distributedlog.lock.DistributedLock;
import com.twitter.distributedlog.logsegment.LogSegmentMetadataStore;
import com.twitter.distributedlog.util.PermitManager;
import com.twitter.distributedlog.util.Transaction;
import com.twitter.util.Future;

import java.io.Closeable;
import java.net.URI;

/**
 * The interface to manage the log stream metadata. The implementation is responsible
 * for creating the metadata layout.
 */
@Beta
public interface LogStreamMetadataStore extends Closeable {

    /**
     * Create a transaction for the metadata operations happening in the metadata store.
     *
     * @return transaction for the metadata operations
     */
    Transaction<Object> newTransaction();

    /**
     * Ensure the existence of a log stream
     *
     * @param uri the location of the log stream
     * @param logName the name of the log stream
     * @return future represents the existence of a log stream. {@link com.twitter.distributedlog.LogNotFoundException}
     *         is thrown if the log doesn't exist
     */
    Future<Void> logExists(URI uri, String logName);

    /**
     * Create the read lock for the log stream.
     *
     * @param metadata the metadata for a log stream
     * @param readerId the reader id used for lock
     * @return the read lock
     */
    Future<DistributedLock> createReadLock(ZKLogMetadataForReader metadata,
                                           Optional<String> readerId);

    /**
     * Create the write lock for the log stream.
     *
     * @param metadata the metadata for a log stream
     * @return the write lock
     */
    DistributedLock createWriteLock(ZKLogMetadataForWriter metadata);

    /**
     * Create the metadata of a log.
     *
     * @param uri the location to store the metadata of the log
     * @param streamName the name of the log stream
     * @param ownAllocator whether to use its own allocator or external allocator
     * @param createIfNotExists flag to create the stream if it doesn't exist
     * @return the metadata of the log
     */
    Future<ZKLogMetadataForWriter> getLog(URI uri,
                                          String streamName,
                                          boolean ownAllocator,
                                          boolean createIfNotExists);

    /**
     * Delete the metadata of a log.
     *
     * @param uri the location to store the metadata of the log
     * @param streamName the name of the log stream
     * @return future represents the result of the deletion.
     */
    Future<Void> deleteLog(URI uri, String streamName);

    /**
     * Get the log segment metadata store.
     *
     * @return the log segment metadata store.
     */
    LogSegmentMetadataStore getLogSegmentMetadataStore();

    /**
     * Get the permit manager for this metadata store. It can be used for limiting the concurrent
     * metadata operations. The implementation can disable handing over the permits when the metadata
     * store is unavailable (for example zookeeper session expired).
     *
     * @return the permit manager
     */
    PermitManager getPermitManager();



}
