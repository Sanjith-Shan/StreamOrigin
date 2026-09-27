package io.streamorigin.core.serve;

/**
 * An adaptive concurrency limit in the style of the gradient algorithm from Netflix's open-source
 * concurrency-limits library (Gradient2Limit), reimplemented here. It compares a short-window
 * average service time against a slow-moving long-term average: when recent requests take longer
 * than usual, queueing has started, and the limit shrinks in proportion; when they do not, the
 * limit grows by a queue allowance. No capacity number is configured by hand.
 */
public final class GradientLimit implements ConcurrencyLimit {

    private final int minLimit;
    private final int maxLimit;
    private final double tolerance;
    private final double smoothing;
    private final long windowNanos;
    private final double longWindow;

    private volatile int limit;
    private double estimated;
    private double longRtt = -1;
    private long windowStart = System.nanoTime();
    private long windowSum;
    private int windowCount;
    private int windowMaxInflight;

    public GradientLimit(int initial, int minLimit, int maxLimit, double tolerance, long windowMillis) {
        this.limit = initial;
        this.estimated = initial;
        this.minLimit = minLimit;
        this.maxLimit = maxLimit;
        this.tolerance = tolerance;
        this.smoothing = 0.2;
        this.windowNanos = windowMillis * 1_000_000;
        this.longWindow = 600;
    }

    @Override
    public int limit() {
        return limit;
    }

    @Override
    public synchronized void onSample(long rttNanos, int inflight) {
        windowSum += rttNanos;
        windowCount++;
        windowMaxInflight = Math.max(windowMaxInflight, inflight);
        long now = System.nanoTime();
        if (now - windowStart < windowNanos || windowCount < 10) {
            return;
        }
        double shortRtt = (double) windowSum / windowCount;
        int maxInflight = windowMaxInflight;
        windowStart = now;
        windowSum = 0;
        windowCount = 0;
        windowMaxInflight = 0;

        if (longRtt < 0) {
            longRtt = shortRtt;
        } else {
            longRtt = longRtt + (shortRtt - longRtt) / longWindow;
        }
        // Recover quickly after a long overload so the baseline does not stay inflated.
        if (longRtt / shortRtt > 2) {
            longRtt *= 0.95;
        }
        // Only grow when the limit is actually being used; an idle server proves nothing.
        if (maxInflight < estimated / 2 && shortRtt <= longRtt * tolerance) {
            return;
        }
        double gradient = Math.max(0.5, Math.min(1.0, tolerance * longRtt / shortRtt));
        double queue = Math.sqrt(estimated);
        double next = estimated * gradient + queue;
        next = estimated * (1 - smoothing) + next * smoothing;
        estimated = Math.max(minLimit, Math.min(maxLimit, next));
        limit = (int) estimated;
    }
}
