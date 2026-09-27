package io.streamorigin.core.serve;

import io.streamorigin.core.metrics.OriginMetrics;
import io.streamorigin.core.model.Segment;
import io.streamorigin.core.model.SegmentKey;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * Requests held open at the live edge. Every request for the same not-yet-published segment
 * joins one waiter, so one publish (or one store read) answers all of them.
 *
 * <p>A publish notification wakes waiters immediately. As a fallback for a lost notification,
 * a sweeper re-checks the store for every waiter whose segment is overdue.
 */
public final class LiveEdgeWaiters implements AutoCloseable {

    private static final class Waiter {
        final Sinks.One<Segment> sink = Sinks.one();
        final AtomicInteger joined = new AtomicInteger();
        final long expectedAtMs;
        final long createdNanos = System.nanoTime();

        Waiter(long expectedAtMs) {
            this.expectedAtMs = expectedAtMs;
        }
    }

    private final Map<SegmentKey, Waiter> waiters = new ConcurrentHashMap<>();
    private final SegmentResolver resolver;
    private final OriginMetrics metrics;
    private final LongSupplier clock;
    private final Disposable sweeper;

    public LiveEdgeWaiters(SegmentResolver resolver, OriginMetrics metrics, LongSupplier clock, Duration sweepEvery) {
        this.resolver = resolver;
        this.metrics = metrics;
        this.clock = clock;
        metrics.gauge("hold.waiting_segments", waiters::size);
        metrics.gauge("hold.waiting_requests", () -> waiters.values().stream().mapToInt(w -> w.joined.get()).sum());
        this.sweeper = Flux.interval(sweepEvery, sweepEvery).subscribe(t -> sweep());
    }

    /**
     * Holds until the segment is published or {@code deadlineMs} passes. {@code expectedAtMs} is
     * when the schedule says it should exist, used to decide when the sweeper polls the store.
     */
    public Mono<Optional<Segment>> await(SegmentKey key, long expectedAtMs, long deadlineMs) {
        long waitMs = deadlineMs - clock.getAsLong();
        if (waitMs <= 0) {
            return Mono.just(Optional.empty());
        }
        Waiter created = new Waiter(expectedAtMs);
        Waiter w = waiters.computeIfAbsent(key, k -> created);
        w.joined.incrementAndGet();
        metrics.increment(w == created ? "hold.segments" : "hold.joined");
        metrics.increment("hold.requests");
        if (w == created) {
            // Close the race with a publish that landed between the caller's miss and this registration.
            resolver.peekCache(key).ifPresent(this::publish);
        }
        return w.sink.asMono()
                .map(Optional::of)
                .timeout(Duration.ofMillis(waitMs), Mono.fromSupplier(() -> {
                    metrics.increment("hold.timeouts");
                    waiters.remove(key, w);
                    return Optional.<Segment>empty();
                }));
    }

    public boolean isWaiting(SegmentKey key) {
        return waiters.containsKey(key);
    }

    /** A valid copy is available: answer everyone waiting for it. */
    public void publish(Segment segment) {
        Waiter w = waiters.remove(segment.key());
        if (w != null) {
            metrics.add("hold.woken", w.joined.get());
            metrics.increment("hold.wakeups");
            metrics.recordNanosSince("hold.wait", w.createdNanos);
            w.sink.tryEmitValue(segment);
        }
    }

    /** A notification without bytes: look the segment up once and wake waiters if it is there. */
    public void wake(SegmentKey key) {
        if (waiters.containsKey(key)) {
            resolver.resolve(key).subscribe(found -> found.ifPresent(r -> publish(r.segment())), e -> {
            });
        }
    }

    private void sweep() {
        long now = clock.getAsLong();
        waiters.forEach((key, w) -> {
            if (now >= w.expectedAtMs) {
                metrics.increment("hold.sweeps");
                resolver.resolve(key).subscribe(found -> found.ifPresent(r -> publish(r.segment())), e -> {
                });
            }
        });
    }

    @Override
    public void close() {
        sweeper.dispose();
    }

    /** Exposed for tests. */
    public Function<SegmentKey, Integer> joinedCount() {
        return k -> Optional.ofNullable(waiters.get(k)).map(w -> w.joined.get()).orElse(0);
    }
}
