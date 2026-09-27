package io.streamorigin.core.serve;

import java.util.function.LongSupplier;

/** A classic token bucket, refilled lazily from a nanosecond clock. */
public final class TokenBucket {

    private final double ratePerNano;
    private final double capacity;
    private final LongSupplier nanoClock;
    private double tokens;
    private long lastNanos;

    public TokenBucket(double ratePerSecond, double burst, LongSupplier nanoClock) {
        this.ratePerNano = ratePerSecond / 1e9;
        this.capacity = burst;
        this.nanoClock = nanoClock;
        this.tokens = burst;
        this.lastNanos = nanoClock.getAsLong();
    }

    public synchronized boolean tryAcquire() {
        long now = nanoClock.getAsLong();
        tokens = Math.min(capacity, tokens + (now - lastNanos) * ratePerNano);
        lastNanos = now;
        if (tokens >= 1) {
            tokens -= 1;
            return true;
        }
        return false;
    }
}
