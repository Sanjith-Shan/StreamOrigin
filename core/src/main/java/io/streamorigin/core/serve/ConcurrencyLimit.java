package io.streamorigin.core.serve;

/** How many requests may be in service at once. */
public interface ConcurrencyLimit {

    int limit();

    /** One completed request: its service time and the in-flight count when it started. */
    default void onSample(long rttNanos, int inflight) {
    }

    static ConcurrencyLimit fixed(int limit) {
        return () -> limit;
    }
}
