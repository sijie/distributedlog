package com.twitter.distributedlog.function;

import com.twitter.distributedlog.io.AsyncCloseable;
import scala.Function0;
import scala.runtime.AbstractFunction0;
import scala.runtime.BoxedUnit;

/**
 * Function to close {@link com.twitter.distributedlog.io.AsyncCloseable}
 */
public class CloseAsyncCloseableFunction extends AbstractFunction0<BoxedUnit> {

    /**
     * Return a function to close an {@link AsyncCloseable}.
     *
     * @param closeable closeable to close
     * @return function to close an {@link AsyncCloseable}
     */
    public static Function0<BoxedUnit> of(AsyncCloseable closeable) {
        return new CloseAsyncCloseableFunction(closeable);
    }

    private final AsyncCloseable closeable;

    private CloseAsyncCloseableFunction(AsyncCloseable closeable) {
        this.closeable = closeable;
    }

    @Override
    public BoxedUnit apply() {
        closeable.asyncClose();
        return BoxedUnit.UNIT;
    }
}
