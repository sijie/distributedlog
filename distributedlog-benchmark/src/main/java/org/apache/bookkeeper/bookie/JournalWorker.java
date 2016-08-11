package org.apache.bookkeeper.bookie;

import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.benchmark.Worker;
import com.twitter.distributedlog.benchmark.utils.ShiftableRateLimiter;
import com.twitter.distributedlog.util.SchedulerUtils;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.stats.CodahaleUtils;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsData;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class JournalWorker implements Worker {

    private static final Logger LOG = LoggerFactory.getLogger(JournalWorker.class);
    private static final Random random = new Random(System.currentTimeMillis());

    private final Journal journal;
    private final ShiftableRateLimiter rateLimiter;
    private final ExecutorService executorService;
    private final ScheduledExecutorService statsReportService;
    private final int messageSizeBytes;
    private final int writeConcurrency;

    volatile boolean running = true;

    private final StatsLogger statsLogger;
    private final OpStatsLogger requestStat;
    private final AtomicLong writeCnt;

    public JournalWorker(ServerConfiguration conf,
                         ShiftableRateLimiter rateLimiter,
                         int writeConcurrency,
                         int messageSizeBytes,
                         StatsLogger statsLogger) {
        this.rateLimiter = rateLimiter;
        this.writeConcurrency = writeConcurrency;
        this.messageSizeBytes = messageSizeBytes;
        this.executorService = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder().setNameFormat("JournalWriter-%d").build());
        this.statsReportService = Executors.newSingleThreadScheduledExecutor();

        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                statsLogger.scope("bookie").scope("ledger"));
        this.journal = new Journal(conf, ledgerDirsManager, statsLogger.scope("journal"));
        this.journal.start();

        this.statsLogger = statsLogger;
        this.requestStat = this.statsLogger.getOpStatsLogger("requests");
        this.writeCnt = new AtomicLong(0L);

        this.statsReportService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                OpStatsData opStatsData = requestStat.toOpStatsData();
                LOG.info("Write : count = {}, success = {}, failed = {}, latency = [ p50 = {}, p99 = {}, p999 = {}, p9999 = {}, max = {} ]",
                        new Object[] {
                                writeCnt.get(),
                                opStatsData.getNumSuccessfulEvents(),
                                opStatsData.getNumFailedEvents(),
                                opStatsData.getP50Latency(),
                                opStatsData.getP99Latency(),
                                opStatsData.getP999Latency(),
                                opStatsData.getP9999Latency()
                        });
                Timer successTimer = CodahaleUtils.getSuccessTimer(requestStat);
                Timer failureTimer = CodahaleUtils.getFailureTimer(requestStat);
                LOG.info("Write : success - 1min = {}, mean = {}; failure - 1min = {}, mean = {}",
                        new Object[] {
                                successTimer.getOneMinuteRate(), successTimer.getMeanRate(),
                                failureTimer.getOneMinuteRate(), failureTimer.getMeanRate()
                        });
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    @Override
    public void close() throws IOException {
        this.running = false;
        SchedulerUtils.shutdownScheduler(this.executorService, 2, TimeUnit.MINUTES);
        SchedulerUtils.shutdownScheduler(this.statsReportService, 2, TimeUnit.MINUTES);
        journal.shutdown();
    }

    ByteBuffer buildBuffer(int messageSizeBytes) {
        byte[] payload = new byte[2 * 8 + messageSizeBytes];
        random.nextBytes(payload);
        ByteBuffer buf = ByteBuffer.wrap(payload);
        buf.putLong(0, random.nextLong());
        buf.putLong(8, random.nextLong());
        return buf;
    }

    class Writer implements Runnable, BookkeeperInternalCallbacks.WriteCallback {

        final int idx;

        Writer(int idx) {
            this.idx = idx;
        }

        @Override
        public void run() {
            LOG.info("Started writer {}.", idx);
            while (running) {
                try {
                    rateLimiter.getLimiter().acquire();
                    final ByteBuffer data = buildBuffer(messageSizeBytes);
                    if (null == data) {
                        break;
                    }
                    writeCnt.incrementAndGet();
                    long requestNanos = System.nanoTime();
                    journal.logAddEntry(data, this, requestNanos);
                } catch (Throwable t) {
                    LOG.error("Caught unhandled exception", t);
                }
            }
        }

        @Override
        public void writeComplete(int rc, long lid, long eid, BookieSocketAddress bookieSocketAddress, Object ctx) {
            long requestNanos = (Long) ctx;
            requestStat.registerSuccessfulEvent(System.nanoTime() - requestNanos);
        }
    }

    @Override
    public void run() {
        for (int i = 0; i < writeConcurrency; i++) {
            Runnable writer = new Writer(i);
            executorService.submit(writer);
        }
    }
}
