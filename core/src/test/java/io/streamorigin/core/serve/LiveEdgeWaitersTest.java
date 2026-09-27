package io.streamorigin.core.serve;

import io.streamorigin.core.Fixtures;
import io.streamorigin.core.metrics.OriginMetrics;
import io.streamorigin.core.model.Segment;
import io.streamorigin.core.model.SegmentKey;
import io.streamorigin.core.store.ChunkedSegmentStore;
import io.streamorigin.core.store.MemoryKv;
import io.streamorigin.core.store.StoreListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

final class LiveEdgeWaitersTest {

    private static final SegmentKey KEY = new SegmentKey("demo", "480p", 5);

    private final OriginMetrics metrics = new OriginMetrics(null);
    private final ChunkedSegmentStore store = new ChunkedSegmentStore(new MemoryKv(), 1000, Duration.ZERO, StoreListener.NONE);
    private final SegmentResolver resolver = new SegmentResolver(store, e -> List.of("A", "B"), true, true,
            1 << 20, Duration.ofSeconds(30), metrics);
    private LiveEdgeWaiters waiters;

    private LiveEdgeWaiters waiters(Duration sweepEvery) {
        waiters = new LiveEdgeWaiters(resolver, metrics, System::currentTimeMillis, sweepEvery);
        return waiters;
    }

    @AfterEach
    void close() {
        if (waiters != null) {
            waiters.close();
        }
    }

    @Test
    void onePublishAnswersEveryWaiter() {
        LiveEdgeWaiters w = waiters(Duration.ofSeconds(60));
        long now = System.currentTimeMillis();
        int n = 25;
        List<Mono<Optional<Segment>>> all = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            all.add(w.await(KEY, now + 60_000, now + 5_000));
        }
        assertThat(w.isWaiting(KEY)).isTrue();
        assertThat(w.joinedCount().apply(KEY)).isEqualTo(n);
        assertThat(metrics.count("hold.segments")).isEqualTo(1);
        assertThat(metrics.count("hold.joined")).isEqualTo(n - 1);

        Segment seg = Fixtures.segment(KEY, "A", Fixtures.media(2048), false);
        w.publish(seg);

        List<Optional<Segment>> got = Flux.merge(all).collectList().block(Duration.ofSeconds(2));
        assertThat(got).hasSize(n).allSatisfy(o -> assertThat(o).containsSame(seg));
        assertThat(w.isWaiting(KEY)).isFalse();
        assertThat(w.joinedCount().apply(KEY)).isZero();
        assertThat(metrics.count("hold.woken")).isEqualTo(n);
    }

    @Test
    void awaitTimesOutEmptyAtDeadline() {
        LiveEdgeWaiters w = waiters(Duration.ofSeconds(60));
        long start = System.currentTimeMillis();
        Optional<Segment> got = w.await(KEY, start + 60_000, start + 200).block(Duration.ofSeconds(2));
        long elapsed = System.currentTimeMillis() - start;
        assertThat(got).isEmpty();
        assertThat(elapsed).isBetween(150L, 1_500L);
        assertThat(w.isWaiting(KEY)).isFalse();
        assertThat(metrics.count("hold.timeouts")).isEqualTo(1);
    }

    @Test
    void pastDeadlineReturnsEmptyImmediately() {
        LiveEdgeWaiters w = waiters(Duration.ofSeconds(60));
        long now = System.currentTimeMillis();
        assertThat(w.await(KEY, now - 1_000, now - 1).block(Duration.ofMillis(500))).isEmpty();
        assertThat(w.isWaiting(KEY)).isFalse();
    }

    @Test
    void publishThatLandedInCacheBeforeRegistrationIsDelivered() {
        LiveEdgeWaiters w = waiters(Duration.ofSeconds(60));
        Segment seg = Fixtures.segment(KEY, "B", Fixtures.media(2048), false);
        // The write-through arrived and its publish() found nobody waiting.
        assertThat(resolver.offer(seg)).isTrue();
        w.publish(seg);

        long now = System.currentTimeMillis();
        Optional<Segment> got = w.await(KEY, now + 60_000, now + 5_000).block(Duration.ofSeconds(1));
        assertThat(got).containsSame(seg);
        assertThat(w.isWaiting(KEY)).isFalse();
    }

    @Test
    void sweeperFindsSegmentWrittenWithoutPublish() {
        LiveEdgeWaiters w = waiters(Duration.ofMillis(50));
        long now = System.currentTimeMillis();
        // Expected time already passed, so the sweeper polls the store on every tick.
        Mono<Optional<Segment>> held = w.await(KEY, now - 10, now + 5_000);
        byte[] data = Fixtures.media(2048);
        store.put(KEY, Fixtures.meta("A", data.length, false), data).block();

        long start = System.currentTimeMillis();
        Optional<Segment> got = held.block(Duration.ofSeconds(3));
        assertThat(got).hasValueSatisfying(s -> {
            assertThat(s.meta().pipeline()).isEqualTo("A");
            assertThat(s.data()).isEqualTo(data);
        });
        assertThat(System.currentTimeMillis() - start).isLessThan(2_000);
        assertThat(metrics.count("hold.sweeps")).isPositive();
    }

    @Test
    void sweeperLeavesWaitersAloneBeforeTheirExpectedTime() throws InterruptedException {
        LiveEdgeWaiters w = waiters(Duration.ofMillis(50));
        long now = System.currentTimeMillis();
        Mono<Optional<Segment>> held = w.await(KEY, now + 60_000, now + 5_000);
        Thread.sleep(300);
        assertThat(metrics.count("hold.sweeps")).isZero();
        assertThat(w.isWaiting(KEY)).isTrue();
        w.publish(Fixtures.segment(KEY, "A", Fixtures.media(2048), false));
        assertThat(held.block(Duration.ofSeconds(1))).isPresent();
    }

    @Test
    void wakeReadsStoreOnceAndAnswersWaiters() {
        LiveEdgeWaiters w = waiters(Duration.ofSeconds(60));
        long now = System.currentTimeMillis();
        Mono<Optional<Segment>> a = w.await(KEY, now + 60_000, now + 5_000);
        Mono<Optional<Segment>> b = w.await(KEY, now + 60_000, now + 5_000);
        byte[] data = Fixtures.media(2048);
        store.put(KEY, Fixtures.meta("B", data.length, false), data).block();
        w.wake(KEY);
        assertThat(a.block(Duration.ofSeconds(1))).isPresent();
        assertThat(b.block(Duration.ofSeconds(1))).isPresent();
        assertThat(metrics.count("hold.wakeups")).isEqualTo(1);
    }
}
