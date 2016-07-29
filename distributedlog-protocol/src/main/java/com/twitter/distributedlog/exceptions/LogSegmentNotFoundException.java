package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

/**
 * Exception on log segment not found
 */
public class LogSegmentNotFoundException extends DLException {

    private static final long serialVersionUID = -2482324226595903864L;

    public LogSegmentNotFoundException(String logSegmentPath) {
        super(StatusCode.LOG_SEGMENT_NOT_FOUND, "Log Segment " + logSegmentPath + " not found");
    }
}
