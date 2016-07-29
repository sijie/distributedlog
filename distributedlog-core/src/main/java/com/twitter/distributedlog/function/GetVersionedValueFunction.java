package com.twitter.distributedlog.function;

import com.twitter.distributedlog.LogSegmentMetadata;
import org.apache.bookkeeper.versioning.Versioned;
import scala.Function1;
import scala.runtime.AbstractFunction1;

import java.util.List;

/**
 * Function to get the versioned value from {@link org.apache.bookkeeper.versioning.Versioned}
 */
public class GetVersionedValueFunction<T> extends AbstractFunction1<Versioned<T>, T> {

    public static final Function1<Versioned<List<LogSegmentMetadata>>, List<LogSegmentMetadata>>
            GET_LOGSEGMENT_LIST_FUNC = new GetVersionedValueFunction<List<LogSegmentMetadata>>();

    @Override
    public T apply(Versioned<T> versionedValue) {
        return versionedValue.getValue();
    }
}
