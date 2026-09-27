package io.streamorigin.core.serve;

import io.streamorigin.core.Fixtures;
import io.streamorigin.core.metrics.OriginMetrics;
import io.streamorigin.core.model.Segment;
import io.streamorigin.core.model.SegmentKey;
import io.streamorigin.core.model.SegmentMeta;
import io.streamorigin.core.serve.SegmentResolver.Resolved;
import io.streamorigin.core.serve.SegmentResolver.Source;
import io.streamorigin.core.store.ChunkedSegmentStore;
import io.streamorigin.core.store.MemoryKv;
import io.streamorigin.core.store.SegmentStore;
import io.streamorigin.core.store.StoreListener;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

final class SegmentResolverTest {

    private static final SegmentKey KEY = new SegmentKey("demo", "480p", 10);
    private static final List<String> ORDER = List.of("A", "B");

    /** Counts calls and optionally delays metas() so concurrent resolves overlap. */
    static final class CountingStore implements SegmentStore {
        final SegmentStore inner;
        final Duration metaDelay;
        final AtomicInteger metaCalls = new AtomicInteger();
        final AtomicInteger dataCalls = new AtomicInteger();

        CountingStore(SegmentStore inner, Duration metaDelay) {
            this.inner = inner;
            this.metaDelay = metaDelay;
        }

        @Override
        public Mono<Void> put(SegmentKey key, SegmentMeta meta, byte[] data) {
            return inner.put(key, meta, data);
        }

        @Override
        public Mono<List<Optional<SegmentMeta>>> metas(SegmentKey key, List<String> pipelines) {
            metaCalls.incrementAndGet();
            Mono<List<Optional<SegmentMeta>>> op = inner.metas(key, pipelines);
            return metaDelay.isZero() ? op : Mono.delay(metaDelay).then(op);
        }

        @Override
        public Mono<Optional<byte[]>> data(SegmentKey key, SegmentMeta meta) {
            dataCalls.incrementAndGet();
            return inner.data(key, meta);
        }

        @Override
        public String name() {
            return "counting";
        }
    }

    private final OriginMetrics metrics = new OriginMetrics(null);

    private CountingStore store(Duration delay) {
        return new CountingStore(new ChunkedSegmentStore(new MemoryKv(), 1000, Duration.ZERO, StoreListener.NONE), delay);
    }

    private SegmentResolver resolver(SegmentStore store, boolean firstValid, boolean coalescing, long cacheBytes) {
        return new SegmentResolver(store, e -> ORDER, firstValid, coalescing, cacheBytes, Duration.ofSeconds(30), metrics);
    }

    private static void put(SegmentStore store, String pipeline, byte[] data, boolean defect) {
        store.put(KEY, Fixtures.meta(pipeline, data.length, defect), data).block();
    }

    @Test
    void flaggedFirstPipelineFallsThroughToSecond() {
        CountingStore store = store(Duration.ZERO);
        put(store, "A", Fixtures.media(2048), true);
        put(store, "B", Fixtures.media(2048), false);
        Resolved r = resolver(store, true, true, 0).resolve(KEY).block().orElseThrow();
        assertThat(r.segment().meta().pipeline()).isEqualTo("B");
        assertThat(r.source()).isEqualTo(Source.STORE);
        // The flagged copy was skipped on its meta alone, without reading its bytes.
        assertThat(store.dataCalls).hasValue(1);
        assertThat(metrics.count("firstvalid.skipped_flagged")).isEqualTo(1);
    }

    @Test
    void unflaggedGarbageFailsSanityCheckAndFallsThrough() {
        CountingStore store = store(Duration.ZERO);
        put(store, "A", Fixtures.body("junk", 2048), false);
        put(store, "B", Fixtures.media(2048), false);
        Resolved r = resolver(store, true, false, 0).resolve(KEY).block().orElseThrow();
        assertThat(r.segment().meta().pipeline()).isEqualTo("B");
        assertThat(store.dataCalls).hasValue(2);
        assertThat(metrics.count("firstvalid.skipped_sanity")).isEqualTo(1);
    }

    @Test
    void bothInvalidResolvesEmpty() {
        CountingStore store = store(Duration.ZERO);
        put(store, "A", Fixtures.media(2048), true);
        put(store, "B", Fixtures.body("junk", 2048), false);
        assertThat(resolver(store, true, true, 1 << 20).resolve(KEY).block()).isEmpty();
    }

    @Test
    void missingEverywhereResolvesEmpty() {
        assertThat(resolver(store(Duration.ZERO), true, true, 1 << 20).resolve(KEY).block()).isEmpty();
    }

    @Test
    void validFirstPipelineWins() {
        CountingStore store = store(Duration.ZERO);
        put(store, "A", Fixtures.media(2048), false);
        put(store, "B", Fixtures.media(4096), false);
        Resolved r = resolver(store, true, true, 0).resolve(KEY).block().orElseThrow();
        assertThat(r.segment().meta().pipeline()).isEqualTo("A");
        assertThat(store.dataCalls).hasValue(1);
    }

    @Test
    void naiveModeServesFirstPresentCopyEvenIfDefective() {
        CountingStore store = store(Duration.ZERO);
        byte[] bad = Fixtures.body("junk", 100);
        put(store, "A", bad, true);
        put(store, "B", Fixtures.media(2048), false);
        Resolved r = resolver(store, false, false, 0).resolve(KEY).block().orElseThrow();
        assertThat(r.segment().meta().pipeline()).isEqualTo("A");
        assertThat(r.segment().data()).isEqualTo(bad);
    }

    @Test
    void concurrentResolvesOfOneKeyShareOneStoreRead() {
        CountingStore store = store(Duration.ofMillis(300));
        put(store, "A", Fixtures.media(2048), false);
        SegmentResolver resolver = resolver(store, true, true, 0);

        int n = 50;
        List<Mono<Optional<Resolved>>> all = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            all.add(resolver.resolve(KEY));
        }
        List<Optional<Resolved>> results = Flux.merge(all).collectList().block(Duration.ofSeconds(5));

        assertThat(results).hasSize(n).allSatisfy(r -> assertThat(r).isPresent());
        assertThat(store.metaCalls).hasValue(1);
        assertThat(results.stream().filter(r -> r.get().source() == Source.STORE)).hasSize(1);
        assertThat(results.stream().filter(r -> r.get().source() == Source.COALESCED)).hasSize(n - 1);
        assertThat(metrics.count("store.coalesced")).isEqualTo(n - 1);

        // Once the read completes the next resolve goes to the store again (no cache here).
        resolver.resolve(KEY).block();
        assertThat(store.metaCalls).hasValue(2);
    }

    @Test
    void withoutCoalescingEveryResolveReadsTheStore() {
        CountingStore store = store(Duration.ofMillis(100));
        put(store, "A", Fixtures.media(2048), false);
        SegmentResolver resolver = resolver(store, true, false, 0);
        List<Mono<Optional<Resolved>>> all = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            all.add(resolver.resolve(KEY));
        }
        Flux.merge(all).blockLast(Duration.ofSeconds(5));
        assertThat(store.metaCalls).hasValue(5);
    }

    @Test
    void offeredCopyIsServedFromCacheWithoutStoreCalls() {
        CountingStore store = store(Duration.ZERO);
        SegmentResolver resolver = resolver(store, true, true, 1 << 20);
        Segment seg = Fixtures.segment(KEY, "B", Fixtures.media(2048), false);

        assertThat(resolver.offer(seg)).isTrue();
        Resolved r = resolver.resolve(KEY).block().orElseThrow();
        assertThat(r.source()).isEqualTo(Source.CACHE);
        assertThat(r.segment()).isSameAs(seg);
        assertThat(store.metaCalls).hasValue(0);
        assertThat(store.dataCalls).hasValue(0);
        assertThat(resolver.peekCache(KEY)).contains(seg);
    }

    @Test
    void invalidOfferIsRefusedWhenFirstValid() {
        CountingStore store = store(Duration.ZERO);
        SegmentResolver resolver = resolver(store, true, true, 1 << 20);
        assertThat(resolver.offer(Fixtures.segment(KEY, "A", Fixtures.media(2048), true))).isFalse();
        assertThat(resolver.offer(Fixtures.segment(KEY, "A", Fixtures.body("junk", 2048), false))).isFalse();
        assertThat(resolver.peekCache(KEY)).isEmpty();
        assertThat(metrics.count("writethrough.rejected_invalid")).isEqualTo(2);

        // The first valid copy offered wins; a later one does not replace it.
        Segment b = Fixtures.segment(KEY, "B", Fixtures.media(2048), false);
        Segment a = Fixtures.segment(KEY, "A", Fixtures.media(2048), false);
        assertThat(resolver.offer(b)).isTrue();
        assertThat(resolver.offer(a)).isTrue();
        assertThat(resolver.peekCache(KEY)).contains(b);
    }

    @Test
    void storeHitIsCachedForTheNextResolve() {
        CountingStore store = store(Duration.ZERO);
        put(store, "A", Fixtures.media(2048), false);
        SegmentResolver resolver = resolver(store, true, true, 1 << 20);
        assertThat(resolver.resolve(KEY).block().orElseThrow().source()).isEqualTo(Source.STORE);
        assertThat(resolver.resolve(KEY).block().orElseThrow().source()).isEqualTo(Source.CACHE);
        assertThat(store.metaCalls).hasValue(1);
    }
}
