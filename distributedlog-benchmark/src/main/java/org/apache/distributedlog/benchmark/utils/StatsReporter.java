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
package org.apache.distributedlog.benchmark.utils;

import org.apache.bookkeeper.stats.OpStatsData;
import org.apache.bookkeeper.stats.OpStatsLogger;

import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Stats Reporter.
 */
public class StatsReporter implements Runnable {

    private final ScheduledExecutorService executorService;
    private final OpStatsLogger[] statsLoggers;
    private final long[] successEvents;
    private final long[] failureEvents;

    public StatsReporter(OpStatsLogger... statsLoggers) {
        this.executorService = Executors.newSingleThreadScheduledExecutor();
        this.statsLoggers = statsLoggers;
        this.successEvents = new long[statsLoggers.length];
        this.failureEvents = new long[statsLoggers.length];
        Arrays.fill(successEvents, 0L);
        Arrays.fill(failureEvents, 0L);
        this.executorService.scheduleAtFixedRate(
                this,
                1,
                1,
                TimeUnit.MINUTES);
    }

    @Override
    public void run() {
        for (int i = 0; i < statsLoggers.length; i++) {
            report(i, statsLoggers[i]);
        }
    }

    private void report(int idx, OpStatsLogger statsLogger) {
        OpStatsData statsData = statsLogger.toOpStatsData();
        long prevSuccessEvents = successEvents[idx];
        long prevFailureEvents = failureEvents[idx];
        long curSuccessEvents = statsData.getNumSuccessfulEvents();
        long curFailureEvents = statsData.getNumFailedEvents();

        double successRate = ((double) (curSuccessEvents - prevSuccessEvents)) / 60;
        double failureRate = ((double) (curFailureEvents - prevFailureEvents)) / 60;

        System.out.println("Rate - success = " + successRate + ", failure = " + failureRate
                + ", latency = [p50 = " + statsData.getP50Latency()
                + ", p99 = " + statsData.getP99Latency()
                + ", p999 = " + statsData.getP9999Latency());

        successEvents[idx] = curSuccessEvents;
        failureEvents[idx] = curFailureEvents;
    }

    public void shutdown() {
        executorService.shutdown();
    }
}
