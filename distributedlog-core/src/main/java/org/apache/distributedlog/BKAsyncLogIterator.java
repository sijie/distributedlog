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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.distributedlog.exceptions.DLIllegalStateException;
import org.apache.distributedlog.exceptions.DLInterruptedException;
import org.apache.distributedlog.exceptions.LogNotFoundException;
import org.apache.distributedlog.exceptions.ReadCancelledException;
import org.apache.distributedlog.util.FutureUtils;
import org.apache.distributedlog.util.OrderedScheduler;
import org.apache.distributedlog.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * BookKeeper based {@link AsyncLogReader} implementation.
 *
 * <h3>Metrics</h3>
 * All the metrics are exposed under `async_reader`.
 * <ul>
 * <li> `async_reader`/future_set: opstats. time spent on satisfying futures of read requests.
 * if it is high, it means that the caller takes time on processing the result of read requests.
 * The side effect is blocking consequent reads.
 * <li> `async_reader`/schedule: opstats. time spent on scheduling next reads.
 * <li> `async_reader`/background_read: opstats. time spent on background reads.
 * <li> `async_reader`/read_next_exec: opstats. time spent on executing {@link #readNext()}.
 * <li> `async_reader`/time_between_read_next: opstats. time spent on between two consequent {@link #readNext()}.
 * if it is high, it means that the caller is slowing down on calling {@link #readNext()}.
 * <li> `async_reader`/delay_until_promise_satisfied: opstats. total latency for the read requests.
 * <li> `async_reader`/idle_reader_error: counter. the number idle reader errors.
 * </ul>
 */
class BKAsyncLogIterator implements AsyncLogIterator, Runnable, AsyncNotification {
    static final Logger LOG = LoggerFactory.getLogger(BKAsyncLogIterator.class);

    private static final Function1<List<LogRecordWithDLSN>, LogRecordWithDLSN> READ_NEXT_MAP_FUNCTION =
            new AbstractFunction1<List<LogRecordWithDLSN>, LogRecordWithDLSN>() {
                @Override
                public LogRecordWithDLSN apply(List<LogRecordWithDLSN> records) {
                    return records.get(0);
                }
            };

    private final String streamName;
    protected final BKDistributedLogManager bkDistributedLogManager;
    protected final BKLogReadHandler readHandler;
    private final AtomicReference<Throwable> lastException = new AtomicReference<Throwable>();
    private final OrderedScheduler scheduler;
    private final ConcurrentLinkedQueue<PendingReadRequest> pendingRequests = new ConcurrentLinkedQueue<PendingReadRequest>();
    private final Object scheduleLock = new Object();
    private final AtomicLong scheduleCount = new AtomicLong(0);
    private DLSN startDLSN;
    private ReadAheadEntryReader readAheadReader = null;
    private ScheduledFuture<?> backgroundScheduleTask = null;

    protected Promise<Void> closeFuture = null;

    // State
    private Entry.Reader currentEntry = null;

    private class PendingReadRequest {
        private final Promise<Entry.Reader> promise;

        PendingReadRequest() {
            this.promise = new Promise<Entry.Reader>();
        }

        Promise<Entry.Reader> getPromise() {
            return promise;
        }

        void setException(Throwable throwable) {
            FutureUtils.setException(promise, throwable);
        }

        void complete(Entry.Reader entry) {
            FutureUtils.setValue(promise, entry);
        }
    }

    BKAsyncLogIterator(BKDistributedLogManager bkdlm,
                       OrderedScheduler scheduler,
                       DLSN startDLSN,
                       Optional<String> subscriberId) {
        this.streamName = bkdlm.getStreamName();
        this.bkDistributedLogManager = bkdlm;
        this.scheduler = scheduler;
        this.readHandler = bkDistributedLogManager.createReadHandler(subscriberId,
                this, true);
        LOG.debug("Starting async reader at {}", startDLSN);
        this.startDLSN = startDLSN;
    }

    synchronized ReadAheadEntryReader getReadAheadReader() {
        return readAheadReader;
    }

    @VisibleForTesting
    public synchronized DLSN getStartDLSN() {
        return startDLSN;
    }

    private boolean checkClosedOrInError(String operation) {
        if (null == lastException.get()) {
            try {
                if (null != readHandler && null != getReadAheadReader()) {
                    getReadAheadReader().checkLastException();
                }

                bkDistributedLogManager.checkClosedOrInError(operation);
            } catch (IOException exc) {
                setLastException(exc);
            }
        }

        if (null != lastException.get()) {
            LOG.trace("Cancelling pending reads");
            cancelAllPendingReads(lastException.get());
            return true;
        }

        return false;
    }

    private void setLastException(IOException exc) {
        lastException.compareAndSet(null, exc);
    }

    /**
     * @return A promise that when satisfied will contain the Log Record with its DLSN.
     */
    @Override
    public Future<Entry.Reader> readNext() {
        PendingReadRequest readRequest = new PendingReadRequest();
        readInternal(readRequest);
        return readRequest.getPromise();
    }

    private synchronized void readInternal(PendingReadRequest request) {
        if (null == readAheadReader) {
            final ReadAheadEntryReader readAheadEntryReader = this.readAheadReader = new ReadAheadEntryReader(
                    streamName,
                    getStartDLSN(),
                    bkDistributedLogManager.getConf(),
                    readHandler,
                    bkDistributedLogManager.getReaderEntryStore(),
                    bkDistributedLogManager.getScheduler(),
                    Ticker.systemTicker(),
                    bkDistributedLogManager.alertStatsLogger);
            readHandler.checkLogStreamExists().addEventListener(new FutureEventListener<Void>() {
                @Override
                public void onSuccess(Void value) {
                    try {
                        readHandler.registerListener(readAheadEntryReader);
                        readHandler.asyncStartFetchLogSegments()
                                .map(new AbstractFunction1<Versioned<List<LogSegmentMetadata>>, BoxedUnit>() {
                                    @Override
                                    public BoxedUnit apply(Versioned<List<LogSegmentMetadata>> logSegments) {
                                        readAheadEntryReader.addStateChangeNotification(BKAsyncLogIterator.this);
                                        readAheadEntryReader.start(logSegments.getValue());
                                        return BoxedUnit.UNIT;
                                    }
                                });
                    } catch (Exception exc) {
                        notifyOnError(exc);
                    }
                }

                @Override
                public void onFailure(Throwable cause) {
                    notifyOnError(cause);
                }
            });
        }

        if (checkClosedOrInError("readNext")) {
            request.setException(lastException.get());
        } else {
            boolean queueEmpty = pendingRequests.isEmpty();
            pendingRequests.add(request);

            if (queueEmpty) {
                scheduleBackgroundRead();
            }
        }
    }

    public synchronized void scheduleBackgroundRead() {
        // if the reader is already closed, we don't need to schedule background read again.
        if (null != closeFuture) {
            return;
        }

        long prevCount = scheduleCount.getAndIncrement();
        if (0 == prevCount) {
            scheduler.submit(streamName, this);
        }
    }

    @Override
    public Future<Void> asyncClose() {
        // Cancel the idle reader timeout task, interrupting if necessary
        ReadCancelledException exception;
        Promise<Void> closePromise;
        synchronized (this) {
            if (null != closeFuture) {
                return closeFuture;
            }
            closePromise = closeFuture = new Promise<Void>();
            exception = new ReadCancelledException(readHandler.getFullyQualifiedName(), "Reader was closed");
            setLastException(exception);
        }

        synchronized (scheduleLock) {
            if (null != backgroundScheduleTask) {
                backgroundScheduleTask.cancel(true);
            }
        }

        cancelAllPendingReads(exception);

        ReadAheadEntryReader readAheadReader = getReadAheadReader();
        if (null != readAheadReader) {
            readHandler.unregisterListener(readAheadReader);
            readAheadReader.removeStateChangeNotification(this);
        }
        Utils.closeSequence(bkDistributedLogManager.getScheduler(), true,
                readAheadReader,
                readHandler
        ).proxyTo(closePromise);
        return closePromise;
    }

    private void cancelAllPendingReads(Throwable throwExc) {
        for (PendingReadRequest promise : pendingRequests) {
            promise.setException(throwExc);
        }
        pendingRequests.clear();
    }

    private synchronized Entry.Reader readNextEntry() throws IOException {
        if (null == readAheadReader) {
            return null;
        }
        if (null == currentEntry) {
            currentEntry = readAheadReader.getNextReadAheadEntry(0L, TimeUnit.MILLISECONDS);
            // no entry after reading from read ahead then return null
            if (null == currentEntry) {
                return null;
            }
        }
        return currentEntry;
    }

    private synchronized void clearCurrentEntry() {
        currentEntry = null;
    }

    @Override
    public void run() {
        synchronized(scheduleLock) {
            Stopwatch runTime = Stopwatch.createStarted();
            int iterations = 0;
            long scheduleCountLocal = scheduleCount.get();
            LOG.debug("{}: Scheduled Background Reader", readHandler.getFullyQualifiedName());
            while(true) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("{}: Executing Iteration: {}", readHandler.getFullyQualifiedName(), iterations++);
                }

                PendingReadRequest nextRequest = null;
                synchronized(this) {
                    nextRequest = pendingRequests.peek();

                    // Queue is empty, nothing to read, return
                    if (null == nextRequest) {
                        LOG.trace("{}: Queue Empty waiting for Input", readHandler.getFullyQualifiedName());
                        scheduleCount.set(0);
                        return;
                    }
                }

                // If the oldest pending promise is interrupted then we must mark
                // the reader in error and abort all pending reads since we dont
                // know the last consumed read
                if (null == lastException.get()) {
                    if (nextRequest.getPromise().isInterrupted().isDefined()) {
                        setLastException(new DLInterruptedException("Interrupted on reading " + readHandler.getFullyQualifiedName() + " : ",
                                nextRequest.getPromise().isInterrupted().get()));
                    }
                }

                if (checkClosedOrInError("readNext")) {
                    if (!(lastException.get().getCause() instanceof LogNotFoundException)) {
                        LOG.warn("{}: Exception", readHandler.getFullyQualifiedName(), lastException.get());
                    }
                    return;
                }

                Entry.Reader entry = null;
                try {
                    entry = readNextEntry();
                } catch (IOException exc) {
                    setLastException(exc);
                    if (!(exc instanceof LogNotFoundException)) {
                        LOG.warn("{} : read with skip Exception", readHandler.getFullyQualifiedName(), lastException.get());
                    }
                    continue;
                }

                if (null != entry) {
                    PendingReadRequest request = pendingRequests.poll();
                    if (null != request && nextRequest == request) {
                        clearCurrentEntry();
                        request.complete(entry);
                        if (null != backgroundScheduleTask) {
                            backgroundScheduleTask.cancel(true);
                            backgroundScheduleTask = null;
                        }
                    } else {
                        DLIllegalStateException ise = new DLIllegalStateException("Unexpected condition");
                        nextRequest.setException(ise);
                        if (null != request) {
                            request.setException(ise);
                        }
                        // We should never get here as we should have exited the loop if
                        // pendingRequests were empty
                        bkDistributedLogManager.raiseAlert("Unexpected condition");
                        setLastException(ise);
                    }
                } else {
                    if (0 == scheduleCountLocal) {
                        LOG.trace("Schedule count dropping to zero", lastException.get());
                        return;
                    }
                    scheduleCountLocal = scheduleCount.decrementAndGet();
                }
            }
        }
    }

    /**
     * Triggered when the background activity encounters an exception
     */
    @Override
    public void notifyOnError(Throwable cause) {
        if (cause instanceof IOException) {
            setLastException((IOException) cause);
        } else {
            setLastException(new IOException(cause));
        }
        scheduleBackgroundRead();
    }

    /**
     * Triggered when the background activity completes an operation
     */
    @Override
    public void notifyOnOperationComplete() {
        scheduleBackgroundRead();
    }

}

