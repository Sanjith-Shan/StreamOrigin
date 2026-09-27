package io.streamorigin.core.serve;

import io.streamorigin.core.metrics.OriginMetrics;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Priority admission. Each class has a token bucket, and both share one concurrency limit, but
 * replay traffic may only use a fraction of it. When the origin is saturated, DVR requests are
 * refused first while live-edge requests still find room.
 */
public final class AdmissionController {

    /** A granted slot; hand it back exactly once with {@link #release}. */
    public static final class Permit {
        final RequestClass cls;
        final long startNanos = System.nanoTime();
        final int inflightAtStart;
        private boolean released;

        Permit(RequestClass cls, int inflightAtStart) {
            this.cls = cls;
            this.inflightAtStart = inflightAtStart;
        }
    }

    private final boolean enabled;
    private final ConcurrencyLimit limit;
    private final double dvrShare;
    private final Map<RequestClass, TokenBucket> buckets = new EnumMap<>(RequestClass.class);
    private final AtomicInteger inflight = new AtomicInteger();
    private final OriginMetrics metrics;

    public AdmissionController(boolean enabled, ConcurrencyLimit limit, double dvrShare,
                               double liveRate, double dvrRate, OriginMetrics metrics) {
        this.enabled = enabled;
        this.limit = limit;
        this.dvrShare = dvrShare;
        this.metrics = metrics;
        buckets.put(RequestClass.LIVE_EDGE, new TokenBucket(liveRate, Math.max(1, liveRate / 5), System::nanoTime));
        buckets.put(RequestClass.DVR, new TokenBucket(dvrRate, Math.max(1, dvrRate / 5), System::nanoTime));
        metrics.gauge("admission.inflight", inflight::get);
        metrics.gauge("admission.limit", limit::limit);
    }

    public boolean enabled() {
        return enabled;
    }

    /** Returns a permit, or null if the request should be shed. */
    public Permit tryAcquire(RequestClass cls) {
        if (!enabled) {
            return new Permit(cls, inflight.incrementAndGet());
        }
        int lim = limit.limit();
        int cap = cls == RequestClass.LIVE_EDGE ? lim : Math.max(1, (int) (lim * dvrShare));
        while (true) {
            int current = inflight.get();
            if (current >= cap) {
                metrics.increment("shed." + cls.metricName() + ".limit");
                return null;
            }
            if (inflight.compareAndSet(current, current + 1)) {
                break;
            }
        }
        if (!buckets.get(cls).tryAcquire()) {
            inflight.decrementAndGet();
            metrics.increment("shed." + cls.metricName() + ".rate");
            return null;
        }
        return new Permit(cls, inflight.get());
    }

    public void release(Permit permit, boolean sample) {
        synchronized (permit) {
            if (permit.released) {
                return;
            }
            permit.released = true;
        }
        inflight.decrementAndGet();
        if (sample) {
            long rtt = System.nanoTime() - permit.startNanos;
            limit.onSample(rtt, permit.inflightAtStart);
        }
    }

    public int inflight() {
        return inflight.get();
    }
}
