package org.apache.bookkeeper.stats;

import com.codahale.metrics.Timer;

/**
 * Utils to access codahale op stats
 */
public class CodahaleUtils {

    public static Timer getSuccessTimer(OpStatsLogger opStatsLogger) {
        return ((CodahaleOpStatsLogger) opStatsLogger).success;
    }

    public static Timer getFailureTimer(OpStatsLogger opStatsLogger) {
        return ((CodahaleOpStatsLogger) opStatsLogger).fail;
    }

}
