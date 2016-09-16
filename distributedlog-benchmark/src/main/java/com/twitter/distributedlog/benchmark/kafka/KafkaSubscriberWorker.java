package com.twitter.distributedlog.benchmark.kafka;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.benchmark.Utils;
import com.twitter.distributedlog.benchmark.Worker;
import com.twitter.distributedlog.benchmark.thrift.Message;
import com.twitter.distributedlog.util.SchedulerUtils;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Worker to read records from Kafka
 */
public class KafkaSubscriberWorker implements Worker {

    private static final Logger logger = LoggerFactory.getLogger(KafkaSubscriberWorker.class);

    final String topic;
    final Properties properties;
    final ExecutorService executorService;
    final ExecutorService callbackExecutor;
    final ScheduledExecutorService statsReportService;
    final boolean readFromHead;
    final Random random;

    volatile boolean running = true;

    final KafkaConsumer<Integer, byte[]> consumer;

    final StatsLogger statsLogger;
    final OpStatsLogger e2eLatencyStat;

    public KafkaSubscriberWorker(String topic,
                                 Properties properties,
                                 boolean readFromHead,
                                 StatsLogger statsLogger) {
        this.topic = topic;
        this.properties = properties;
        this.readFromHead = readFromHead;
        this.executorService = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder().setNameFormat("KafkaSubscriber-Worker-%d").build());
        this.callbackExecutor = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder().setNameFormat("KafkaSubscriber-Callback-%d").build());
        this.statsReportService = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("KafkaSubscriber-StatsReporter-%d").build());
        this.random = new Random(System.currentTimeMillis());
        this.consumer = new KafkaConsumer<Integer, byte[]>(properties);
        this.statsLogger = statsLogger;
        this.e2eLatencyStat = this.statsLogger.getOpStatsLogger("e2e");

        this.properties.setProperty("key.deserializer", IntegerDeserializer.class.getName());
        this.properties.setProperty("value.deserializer", ByteArrayDeserializer.class.getName());
    }

    @Override
    public void close() throws IOException {
        running = false;
        SchedulerUtils.shutdownScheduler(this.executorService, 2, TimeUnit.MINUTES);
        SchedulerUtils.shutdownScheduler(this.callbackExecutor, 2, TimeUnit.MINUTES);
        SchedulerUtils.shutdownScheduler(this.statsReportService, 2, TimeUnit.MINUTES);
        consumer.close();
    }

    @Override
    public void run() {
        long joinTimeout = 10000L;
        final AtomicBoolean isAssigned = new AtomicBoolean(false);
        consumer.subscribe(Lists.newArrayList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                isAssigned.set(false);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                isAssigned.set(true);
            }
        });
        long joinStart = System.currentTimeMillis();
        while (!isAssigned.get()) {
            if (System.currentTimeMillis() - joinStart >= joinTimeout) {
                throw new RuntimeException("Timed out waiting for initial group join.");
            }
            consumer.poll(100);
        }
        if (readFromHead) {
            consumer.seekToBeginning(Lists.<TopicPartition>newArrayList());
        } else {
            consumer.seekToEnd(Lists.<TopicPartition>newArrayList());
        }

        while (running) {
            final ConsumerRecords<Integer, byte[]> records = consumer.poll(100);
            callbackExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    processRecords(records);
                }
            });
        }
    }

    private void processRecords(ConsumerRecords<Integer, byte[]> records) {
        for (ConsumerRecord<Integer, byte[]> record : records) {
            processRecord(record);
        }
    }

    private void processRecord(ConsumerRecord<Integer, byte[]> record) {
        Message message;
        try {
            message = Utils.parseMessage(record.value());
        } catch (TException e) {
            logger.warn("Failed to parse record {} from topic {}",
                    record, topic);
            return;
        }

        long curTimeMillis = System.currentTimeMillis();
        long e2eLatency = curTimeMillis - message.getPublishTime();
        e2eLatencyStat.registerSuccessfulEvent(e2eLatency);
    }

}
