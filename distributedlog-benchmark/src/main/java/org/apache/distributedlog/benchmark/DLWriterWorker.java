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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.distributedlog.LogRecord.MAX_LOGRECORDSET_SIZE;
import static org.apache.distributedlog.LogRecord.MAX_LOGRECORD_SIZE;

import com.twitter.util.Future;
import com.twitter.util.Promise;
import io.netty.buffer.ByteBuf;
import org.apache.distributedlog.AsyncLogWriter;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.DistributedLogManager;
import org.apache.distributedlog.LogRecord;
import org.apache.distributedlog.LogRecordSet;
import org.apache.distributedlog.benchmark.utils.ShiftableRateLimiter;
import org.apache.distributedlog.benchmark.utils.StatsReporter;
import org.apache.distributedlog.exceptions.LogRecordTooLongException;
import org.apache.distributedlog.io.CompressionCodec;
import org.apache.distributedlog.namespace.DistributedLogNamespace;
import org.apache.distributedlog.namespace.DistributedLogNamespaceBuilder;
import org.apache.distributedlog.util.FutureUtils;
import org.apache.distributedlog.util.OrderedScheduler;
import org.apache.distributedlog.util.SchedulerUtils;
import com.twitter.util.FutureEventListener;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The benchmark for core library writer.
 */
public class DLWriterWorker implements Worker {

    private static final Logger LOG = LoggerFactory.getLogger(DLWriterWorker.class);

    static final int BACKOFF_MS = 200;

    final String streamPrefix;
    final int startStreamId;
    final int endStreamId;
    final int writeConcurrency;
    final int messageSizeBytes;
    final OrderedScheduler scheduler;
    final ExecutorService executorService;
    final ScheduledExecutorService rescueService;
    final StatsReporter statsReporter;
    final ShiftableRateLimiter rateLimiter;
    final Random random;
    final DistributedLogNamespace namespace;
    final List<DistributedLogManager> dlms;
    final List<AsyncLogWriter> streamWriters;
    final int numStreams;

    volatile boolean running = true;

    final StatsLogger statsLogger;
    final OpStatsLogger requestStat;

    public DLWriterWorker(DistributedLogConfiguration conf,
                          URI uri,
                          String streamPrefix,
                          int startStreamId,
                          int endStreamId,
                          ShiftableRateLimiter rateLimiter,
                          int writeConcurrency,
                          int messageSizeBytes,
                          StatsLogger statsLogger) throws IOException {
        checkArgument(startStreamId <= endStreamId);
        this.streamPrefix = streamPrefix;
        this.startStreamId = startStreamId;
        this.endStreamId = endStreamId;
        this.rateLimiter = rateLimiter;
        this.writeConcurrency = writeConcurrency;
        this.messageSizeBytes = messageSizeBytes;
        this.statsLogger = statsLogger;
        this.requestStat = this.statsLogger.getOpStatsLogger("requests");
        this.scheduler = OrderedScheduler.newBuilder()
                .corePoolSize(Runtime.getRuntime().availableProcessors())
                .name("scheduler")
                .build();
        this.executorService = Executors.newCachedThreadPool();
        this.rescueService = Executors.newSingleThreadScheduledExecutor();
        this.random = new Random(System.currentTimeMillis());

        this.statsReporter = new StatsReporter(this.requestStat);

        this.namespace = DistributedLogNamespaceBuilder.newBuilder()
                .conf(conf)
                .uri(uri)
                .statsLogger(statsLogger.scope("dl"))
                .build();
        this.numStreams = endStreamId - startStreamId;
        dlms = new ArrayList<DistributedLogManager>(numStreams);
        streamWriters = new ArrayList<AsyncLogWriter>(numStreams);
        final ConcurrentMap<String, AsyncLogWriter> writers = new ConcurrentHashMap<String, AsyncLogWriter>();
        final CountDownLatch latch = new CountDownLatch(this.numStreams);
        for (int i = startStreamId; i < endStreamId; i++) {
            final String streamName = String.format("%s_%d", streamPrefix, i);
            final DistributedLogManager dlm = namespace.openLog(streamName);
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        AsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();
                        if (null != writers.putIfAbsent(streamName, writer)) {
                            FutureUtils.result(writer.asyncClose());
                        }
                        latch.countDown();
                    } catch (IOException e) {
                        LOG.error("Failed to intialize writer for stream : {}", streamName, e);
                    }

                }
            });
            dlms.add(dlm);
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new IOException("Interrupted on initializing writers for streams.", e);
        }
        for (int i = startStreamId; i < endStreamId; i++) {
            final String streamName = String.format("%s_%d", streamPrefix, i);
            AsyncLogWriter writer = writers.get(streamName);
            if (null == writer) {
                throw new IOException("Writer for " + streamName + " never initialized.");
            }
            streamWriters.add(writer);
        }
        LOG.info("Writing to {} streams.", numStreams);
    }

    void rescueWriter(int idx, AsyncLogWriter writer) {
        if (streamWriters.get(idx) == writer) {
            try {
                FutureUtils.result(writer.asyncClose());
            } catch (IOException e) {
                LOG.error("Failed to close writer for stream {}.", idx);
            }
            AsyncLogWriter newWriter = null;
            try {
                newWriter = dlms.get(idx).startAsyncLogSegmentNonPartitioned();
            } catch (IOException e) {
                LOG.error("Failed to create new writer for stream {}, backoff for {} ms.",
                          idx, BACKOFF_MS);
                scheduleRescue(idx, writer, BACKOFF_MS);
            }
            streamWriters.set(idx, newWriter);
        } else {
            LOG.warn("AsyncLogWriter for stream {} was already rescued.", idx);
        }
    }

    void scheduleRescue(final int idx, final AsyncLogWriter writer, int delayMs) {
        Runnable r = new Runnable() {
            @Override
            public void run() {
                rescueWriter(idx, writer);
            }
        };
        if (delayMs > 0) {
            rescueService.schedule(r, delayMs, TimeUnit.MILLISECONDS);
        } else {
            rescueService.submit(r);
        }
    }

    @Override
    public void close() throws IOException {
        this.running = false;
        SchedulerUtils.shutdownScheduler(this.scheduler, 2, TimeUnit.MINUTES);
        SchedulerUtils.shutdownScheduler(this.executorService, 2, TimeUnit.MINUTES);
        SchedulerUtils.shutdownScheduler(this.rescueService, 2, TimeUnit.MINUTES);
        for (AsyncLogWriter writer : streamWriters) {
            FutureUtils.result(writer.asyncClose());
        }
        for (DistributedLogManager dlm : dlms) {
            dlm.close();
        }
        namespace.close();
        statsReporter.shutdown();
    }

    @Override
    public void run() {
        LOG.info("Starting dlwriter (concurrency = {}, prefix = {}, numStreams = {})",
                 new Object[] { writeConcurrency, streamPrefix, numStreams });
        for (int i = 0; i < writeConcurrency; i++) {
            executorService.submit(new Writer(i));
        }
    }

    class Writer implements Runnable {

        static final int BUFFER_SIZE = 16 * 1024;

        final int idx;
        final int[] streams;
        LogRecordSet.Writer recordSetWriter;
        final Runnable flusher = new Runnable() {
            @Override
            public void run() {
                flush();
            }
        };

        Writer(int idx) {
            this.idx = idx;
            this.recordSetWriter = newRecordSetWriter();
            int numStreamsPerWriter = numStreams / writeConcurrency;
            int startStreamIdx = idx * numStreamsPerWriter;
            int endStreamIdx = (idx + 1) * numStreamsPerWriter;
            if (idx == writeConcurrency - 1) {
                endStreamIdx = numStreams;
            }
            streams = new int[endStreamIdx - startStreamIdx];
            for (int i = 0; i < streams.length; i++) {
                streams[i] = startStreamIdx + i;
            }
            scheduler.scheduleAtFixedRate(
                    idx,
                    flusher,
                    1,
                    1,
                    TimeUnit.MILLISECONDS);
        }

        private LogRecordSet.Writer newRecordSetWriter() {
            return LogRecordSet.newWriter(
                    BUFFER_SIZE,
                    CompressionCodec.Type.NONE);
        }

        @Override
        public void run() {
            LOG.info("Started writer {}.", idx);
            while (running) {
                // rateLimiter.getLimiter().acquire();
                final long requestMillis = System.currentTimeMillis();
                final ByteBuf buf = Utils.generateMessage(requestMillis, messageSizeBytes);
                write(buf.nioBuffer()).addEventListener(new FutureEventListener<DLSN>() {
                    @Override
                    public void onSuccess(DLSN value) {
                        requestStat.registerSuccessfulEvent(System.currentTimeMillis() - requestMillis);
                        buf.release();
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        requestStat.registerFailedEvent(System.currentTimeMillis() - requestMillis);
                        buf.release();
                    }
                });
            }
        }

        public Future<DLSN> write(ByteBuffer buffer) {
            int logRecordSize = buffer.remaining();
            if (logRecordSize > MAX_LOGRECORD_SIZE) {
                return Future.exception(new LogRecordTooLongException(
                        "Log record of size " + logRecordSize + " written when only "
                                + MAX_LOGRECORD_SIZE + " is allowed"));
            }
            // if exceed max number of bytes
            if ((recordSetWriter.getNumBytes() + logRecordSize) > MAX_LOGRECORDSET_SIZE) {
                flush();
            }
            Promise<DLSN> writePromise = new Promise<DLSN>();
            try {
                recordSetWriter.writeRecord(buffer, writePromise);
            } catch (LogRecordTooLongException e) {
                return Future.exception(e);
            }
            if (recordSetWriter.getNumBytes() >= BUFFER_SIZE) {
                flush();
            }
            return writePromise;
        }

        private void flush() {
            final LogRecordSet.Writer recordSetToFlush;
            synchronized (this) {
                if (recordSetWriter.getNumRecords() == 0) {
                    return;
                }
                recordSetToFlush = recordSetWriter;
                recordSetWriter = newRecordSetWriter();
            }
            final int streamIdx = random.nextInt(streams.length);
            final AsyncLogWriter writer = streamWriters.get(streams[streamIdx]);
            final long requestMillis = System.currentTimeMillis();
            ByteBuffer buffer = recordSetToFlush.getBuffer();
            LogRecord recordSet = new LogRecord(requestMillis, buffer);
            recordSet.setControl();
            writer.write(recordSet)
                    .addEventListener(new FutureEventListener<DLSN>() {
                @Override
                public void onFailure(Throwable cause) {
                    recordSetToFlush.abortTransmit(cause);
                    LOG.error("Failed to publish, rescue it : ", cause);
                    scheduleRescue(streamIdx, writer, 0);
                }

                @Override
                public void onSuccess(DLSN value) {
                    recordSetToFlush.completeTransmit(
                            value.getLogSegmentSequenceNo(),
                            value.getEntryId(),
                            value.getSlotId());
                }
            });
        }
    }

}
