package org.apache.distributedlog;

import com.twitter.util.Future;
import org.apache.distributedlog.io.AsyncCloseable;

/**
 * Iterator over a log.
 */
public interface AsyncLogIterator extends AsyncCloseable {

    Future<Entry.Reader> readNext();
}
