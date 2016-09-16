package com.twitter.distributedlog.benchmark.kafka;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.benchmark.Utils;
import com.twitter.distributedlog.benchmark.Worker;
import com.twitter.distributedlog.benchmark.utils.ShiftableRateLimiter;
import com.twitter.distributedlog.util.SchedulerUtils;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Worker to publish records to kafka
 */
public class KafkaPublisherWorker implements Worker {

    private static final Logger logger = LoggerFactory.getLogger(KafkaPublisherWorker.class);

    final String topic;
    final Properties properties;
    final int numPartitions;
    final int writeConcurrency;
    final int messageSizeBytes;
    final ExecutorService executorService;
    final ExecutorService callbackExecutor;
    final ScheduledExecutorService statsReportService;
    final ShiftableRateLimiter rateLimiter;
    final Random random;

    volatile boolean running = true;

    final StatsLogger statsLogger;
    final OpStatsLogger requestStat;

    public KafkaPublisherWorker(String topic,
                                int numPartitions,
                                Properties properties,
                                ShiftableRateLimiter rateLimiter,
                                int writeConcurrency,
                                int messageSizeBytes,
                                StatsLogger statsLogger) {
        this.topic = topic;
        this.numPartitions = numPartitions;
        this.rateLimiter = rateLimiter;
        this.writeConcurrency = writeConcurrency;
        this.messageSizeBytes = messageSizeBytes;
        this.statsLogger = statsLogger;
        this.requestStat = statsLogger.getOpStatsLogger("requests");
        this.executorService = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder().setNameFormat("KafkaPublisher-Worker-%d").build());
        this.callbackExecutor = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder().setNameFormat("KafkaPublisher-Callback-%d").build());
        this.statsReportService = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("KafkaPublisher-StatsReporter-%d").build());
        this.random = new Random(System.currentTimeMillis());

        // construct the kafka client
        this.properties = properties;
        this.properties.setProperty("key.serializer", IntegerSerializer.class.getName());
        this.properties.setProperty("value.serializer", ByteArraySerializer.class.getName());

        logger.info("Writing to kafka topic {}.", topic);
    }

    byte[] buildBuffer(long requestMillis, int messageSizeBytes) {
        try {
            return Utils.generateMessage(requestMillis, messageSizeBytes);
        } catch (TException e) {
            logger.error("Error generating message : ", e);
            return null;
        }
    }

    class TimedCallback implements Callback, Runnable {

        final String streamName;
        final long requestMillis;
        Exception cause = null;

        TimedCallback(String streamName,
                      long requestMillis) {
            this.streamName = streamName;
            this.requestMillis = requestMillis;
        }

        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            this.cause = e;
            callbackExecutor.submit(this);
        }

        @Override
        public void run() {
            if (null == cause) {
                requestStat.registerSuccessfulEvent(System.currentTimeMillis() - requestMillis);
            } else {
                logger.error("Failed to publish to {} : ", streamName, cause);
                requestStat.registerFailedEvent(System.currentTimeMillis() - requestMillis);
            }
        }
    }

    class Publisher implements Runnable {

        final int idx;

        Publisher(int idx) {
            this.idx = idx;
        }

        @Override
        public void run() {
            logger.info("Started publisher {}.", idx);
            KafkaProducer<Integer, byte[]> producer = new KafkaProducer<Integer, byte[]>(properties);
            try {
                while (running) {
                    rateLimiter.getLimiter().acquire();
                    final long requestMillis = System.currentTimeMillis();
                    final byte[] data = buildBuffer(requestMillis, messageSizeBytes);
                    if (null == data) {
                        break;
                    }
                    int pid = random.nextInt(numPartitions);

                    ProducerRecord<Integer, byte[]> producerRecord =
                            new ProducerRecord<Integer, byte[]>(topic, pid, requestMillis, pid, data);
                    TimedCallback callback = new TimedCallback(topic, requestMillis);
                    producer.send(producerRecord, callback);
                }
            } catch (Throwable t) {
                logger.error("Encountered exception on producing records : ", t);
            }
            producer.close();
        }
    }


    @Override
    public void run() {
        for (int i = 0; i < writeConcurrency; i++) {
            Runnable publisher = new Publisher(i);
            executorService.submit(publisher);
        }
    }

    @Override
    public void close() throws IOException {
        this.running = false;
        SchedulerUtils.shutdownScheduler(this.executorService, 2, TimeUnit.MINUTES);
        SchedulerUtils.shutdownScheduler(this.callbackExecutor, 2, TimeUnit.MINUTES);
        SchedulerUtils.shutdownScheduler(this.statsReportService, 2, TimeUnit.MINUTES);

    }
}
