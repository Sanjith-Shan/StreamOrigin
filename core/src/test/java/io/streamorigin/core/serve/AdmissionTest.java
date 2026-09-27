package io.streamorigin.core.serve;

import io.streamorigin.core.metrics.OriginMetrics;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.LongRange;
import net.jqwik.api.constraints.Size;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/** GradientLimit, TokenBucket and AdmissionController. */
final class AdmissionTest {

    private static final long MS = 1_000_000;

    /** windowMillis 0: every 10 samples close a window, so the test drives it by sample count. */
    private static void feed(GradientLimit g, int windows, long rttNanos, int inflight) {
        for (int i = 0; i < windows * 10; i++) {
            g.onSample(rttNanos, inflight);
        }
    }

    @Test
    void gradientGrowsTowardMaxUnderSteadyRtt() {
        GradientLimit g = new GradientLimit(20, 5, 200, 1.5, 0);
        int last = g.limit();
        for (int round = 0; round < 50; round++) {
            feed(g, 10, 10 * MS, g.limit());
            assertThat(g.limit()).isGreaterThanOrEqualTo(last);
            last = g.limit();
        }
        assertThat(g.limit()).isGreaterThanOrEqualTo(190).isLessThanOrEqualTo(200);
    }

    @Test
    void gradientDoesNotGrowWhenIdle() {
        GradientLimit g = new GradientLimit(40, 5, 200, 1.5, 0);
        feed(g, 200, 10 * MS, 1);
        assertThat(g.limit()).isEqualTo(40);
    }

    @Test
    void gradientShrinksWhenRttRisesAboveBaseline() {
        GradientLimit g = new GradientLimit(100, 5, 1000, 1.5, 0);
        feed(g, 30, 10 * MS, 100);
        int before = g.limit();
        feed(g, 30, 40 * MS, before);
        int after = g.limit();
        assertThat(after).isLessThan(before / 2);
        assertThat(after).isGreaterThanOrEqualTo(5);
    }

    @Property(tries = 200)
    void gradientStaysWithinBounds(@ForAll @Size(min = 1, max = 400) List<@LongRange(min = 1, max = 500) Long> rttsMs,
                                   @ForAll @IntRange(min = 0, max = 2_000) int inflight,
                                   @ForAll @IntRange(min = 1, max = 50) int min,
                                   @ForAll @IntRange(min = 0, max = 500) int span) {
        int max = min + span;
        GradientLimit g = new GradientLimit(min + span / 2, min, max, 1.5, 0);
        for (long rtt : rttsMs) {
            for (int i = 0; i < 10; i++) {
                g.onSample(rtt * MS, inflight);
            }
            assertThat(g.limit()).isBetween(min, max);
        }
    }

    @Test
    void tokenBucketBurstThenRefill() {
        AtomicLong clock = new AtomicLong(1_000_000_000L);
        TokenBucket bucket = new TokenBucket(10, 5, clock::get);
        for (int i = 0; i < 5; i++) {
            assertThat(bucket.tryAcquire()).as("burst token %d", i).isTrue();
        }
        assertThat(bucket.tryAcquire()).isFalse();

        clock.addAndGet(50 * MS); // half a token at 10/s
        assertThat(bucket.tryAcquire()).isFalse();
        clock.addAndGet(60 * MS);
        assertThat(bucket.tryAcquire()).isTrue();
        assertThat(bucket.tryAcquire()).isFalse();

        clock.addAndGet(10_000 * MS); // refill is capped at the burst size
        int granted = 0;
        while (bucket.tryAcquire()) {
            granted++;
        }
        assertThat(granted).isEqualTo(5);
    }

    private static AdmissionController fixed(boolean enabled, int limit) {
        return new AdmissionController(enabled, ConcurrencyLimit.fixed(limit), 0.5, 0, 1e9, 1e9, new OriginMetrics(null));
    }

    @Test
    void dvrIsShedBeforeLiveEdge() {
        OriginMetrics metrics = new OriginMetrics(null);
        AdmissionController ac = new AdmissionController(true, ConcurrencyLimit.fixed(10), 0.5, 0, 1e9, 1e9, metrics);
        List<AdmissionController.Permit> held = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            AdmissionController.Permit p = ac.tryAcquire(RequestClass.DVR);
            assertThat(p).as("dvr %d", i).isNotNull();
            held.add(p);
        }
        assertThat(ac.inflight()).isEqualTo(5);
        assertThat(ac.tryAcquire(RequestClass.DVR)).isNull();
        for (int i = 5; i < 10; i++) {
            AdmissionController.Permit p = ac.tryAcquire(RequestClass.LIVE_EDGE);
            assertThat(p).as("live %d", i).isNotNull();
            held.add(p);
        }
        assertThat(ac.tryAcquire(RequestClass.LIVE_EDGE)).isNull();
        assertThat(ac.tryAcquire(RequestClass.DVR)).isNull();
        assertThat(ac.inflight()).isEqualTo(10);
        assertThat(metrics.count("shed.dvr.limit")).isEqualTo(2);
        assertThat(metrics.count("shed.live.limit")).isEqualTo(1);

        held.forEach(p -> ac.release(p, false));
        assertThat(ac.inflight()).isZero();
        assertThat(ac.tryAcquire(RequestClass.DVR)).isNotNull();
    }

    @Test
    void liveFloorAdmitsLiveEdgeBeyondTheLimitWhileDvrStaysCapped() {
        AdmissionController ac = new AdmissionController(true, ConcurrencyLimit.fixed(10), 0.5, 100, 1e9, 1e9,
                new OriginMetrics(null));
        for (int i = 0; i < 5; i++) {
            assertThat(ac.tryAcquire(RequestClass.DVR)).isNotNull();
        }
        assertThat(ac.tryAcquire(RequestClass.DVR)).isNull();
        for (int i = 5; i < 100; i++) {
            assertThat(ac.tryAcquire(RequestClass.LIVE_EDGE)).as("live %d", i).isNotNull();
        }
        assertThat(ac.inflight()).isEqualTo(100);
        assertThat(ac.tryAcquire(RequestClass.LIVE_EDGE)).isNull();
        assertThat(ac.tryAcquire(RequestClass.DVR)).isNull();
    }

    @Test
    void releaseIsIdempotent() {
        AdmissionController ac = fixed(true, 10);
        AdmissionController.Permit a = ac.tryAcquire(RequestClass.LIVE_EDGE);
        AdmissionController.Permit b = ac.tryAcquire(RequestClass.LIVE_EDGE);
        ac.release(a, true);
        ac.release(a, true);
        ac.release(a, false);
        assertThat(ac.inflight()).isEqualTo(1);
        ac.release(b, false);
        assertThat(ac.inflight()).isZero();
    }

    @Test
    void disabledControllerAdmitsEverything() {
        AdmissionController ac = fixed(false, 1);
        assertThat(ac.enabled()).isFalse();
        List<AdmissionController.Permit> held = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            AdmissionController.Permit p = ac.tryAcquire(i % 2 == 0 ? RequestClass.DVR : RequestClass.LIVE_EDGE);
            assertThat(p).isNotNull();
            held.add(p);
        }
        assertThat(ac.inflight()).isEqualTo(100);
        held.forEach(p -> ac.release(p, false));
        assertThat(ac.inflight()).isZero();
    }

    @Test
    void rateLimitShedsAndReturnsTheSlot() {
        OriginMetrics metrics = new OriginMetrics(null);
        // DVR rate 5/s gives a burst of 1 token.
        AdmissionController ac = new AdmissionController(true, ConcurrencyLimit.fixed(100), 0.5, 0, 1e9, 5, metrics);
        assertThat(ac.tryAcquire(RequestClass.DVR)).isNotNull();
        assertThat(ac.tryAcquire(RequestClass.DVR)).isNull();
        assertThat(ac.inflight()).isEqualTo(1);
        assertThat(metrics.count("shed.dvr.rate")).isEqualTo(1);
    }
}
