package io.streamorigin.core.serve;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.streamorigin.core.metrics.OriginMetrics;
import io.streamorigin.core.model.Segment;
import io.streamorigin.core.model.SegmentKey;
import io.streamorigin.core.model.SegmentMeta;
import io.streamorigin.core.store.SegmentStore;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Finds the bytes to serve for a segment: the write-through cache first, then the read store,
 * with identical concurrent lookups coalesced into one store read.
 *
 * <p>With first-valid selection on, a store read tries pipelines in the event's configured order
 * and the first copy that passes {@link SegmentValidator} wins. On the write-through path the
 * first valid copy to arrive wins instead: the pipelines share an epoch and produce
 * interchangeable segments, so waiting for a preferred pipeline would only add latency. With it off, the first copy present is
 * served as-is, which is what an origin that simply stores whatever was PUT would do.
 */
public final class SegmentResolver {

    public enum Source { CACHE, STORE, COALESCED }

    public record Resolved(Segment segment, Source source) {
    }

    private final SegmentStore store;
    private final Function<String, List<String>> pipelineOrder;
    private final boolean firstValid;
    private final boolean coalescing;
    private final Cache<SegmentKey, Segment> cache;
    private final OriginMetrics metrics;
    private final ConcurrentHashMap<SegmentKey, CompletableFuture<Optional<Segment>>> inflight = new ConcurrentHashMap<>();

    public SegmentResolver(SegmentStore store, Function<String, List<String>> pipelineOrder, boolean firstValid,
                           boolean coalescing, long cacheMaxBytes, Duration cacheTtl, OriginMetrics metrics) {
        this.store = store;
        this.pipelineOrder = pipelineOrder;
        this.firstValid = firstValid;
        this.coalescing = coalescing;
        this.metrics = metrics;
        this.cache = cacheMaxBytes <= 0 ? null : Caffeine.newBuilder()
                .maximumWeight(cacheMaxBytes)
                .weigher((SegmentKey k, Segment s) -> s.data().length + 256)
                .expireAfterWrite(cacheTtl)
                .build();
        if (cache != null) {
            metrics.gauge("cache.bytes", () -> cache.policy().eviction()
                    .flatMap(e -> e.weightedSize().stream().boxed().findFirst()).orElse(0L));
            metrics.gauge("cache.entries", cache::estimatedSize);
        }
    }

    public boolean cacheEnabled() {
        return cache != null;
    }

    public Optional<Segment> peekCache(SegmentKey key) {
        return cache == null ? Optional.empty() : Optional.ofNullable(cache.getIfPresent(key));
    }

    /**
     * Write-through entry point: a pipeline's copy arriving from the publish path. Returns true
     * if the copy is valid and now servable from the cache.
     */
    public boolean offer(Segment segment) {
        boolean init = segment.key().isInit();
        if (firstValid && !SegmentValidator.valid(segment.data(), segment.meta(), init)) {
            metrics.increment("writethrough.rejected_invalid");
            return false;
        }
        if (cache != null) {
            Segment existing = cache.asMap().putIfAbsent(segment.key(), segment);
            if (existing == null) {
                metrics.increment("writethrough.filled");
            }
        }
        return true;
    }

    public Mono<Optional<Resolved>> resolve(SegmentKey key) {
        return resolve(key, true);
    }

    /**
     * @param fillCache whether a store read may populate the cache. The cache holds the live edge;
     *                  replay reads go to the read store and must not evict it.
     */
    public Mono<Optional<Resolved>> resolve(SegmentKey key, boolean fillCache) {
        if (cache != null) {
            Segment hit = cache.getIfPresent(key);
            if (hit != null) {
                metrics.increment("cache.hits");
                return Mono.just(Optional.of(new Resolved(hit, Source.CACHE)));
            }
            metrics.increment("cache.misses");
        }
        if (!coalescing) {
            return loadFromStore(key, fillCache).map(o -> o.map(s -> new Resolved(s, Source.STORE)));
        }
        CompletableFuture<Optional<Segment>> mine = new CompletableFuture<>();
        CompletableFuture<Optional<Segment>> existing = inflight.putIfAbsent(key, mine);
        if (existing != null) {
            metrics.increment("store.coalesced");
            return Mono.fromFuture(existing, true).map(o -> o.map(s -> new Resolved(s, Source.COALESCED)));
        }
        loadFromStore(key, fillCache).subscribe(
                v -> {
                    inflight.remove(key, mine);
                    mine.complete(v);
                },
                e -> {
                    inflight.remove(key, mine);
                    mine.completeExceptionally(e);
                });
        return Mono.fromFuture(mine, true).map(o -> o.map(s -> new Resolved(s, Source.STORE)));
    }

    private Mono<Optional<Segment>> loadFromStore(SegmentKey key, boolean fillCache) {
        List<String> order = pipelineOrder.apply(key.event());
        if (order.isEmpty()) {
            return Mono.just(Optional.empty());
        }
        boolean init = key.isInit();
        return store.metas(key, order).flatMap(metas -> {
            List<SegmentMeta> candidates = new ArrayList<>();
            for (Optional<SegmentMeta> m : metas) {
                m.ifPresent(candidates::add);
            }
            if (candidates.isEmpty()) {
                return Mono.just(Optional.<Segment>empty());
            }
            if (!firstValid) {
                SegmentMeta first = candidates.get(0);
                return store.data(key, first).map(d -> d.map(bytes -> new Segment(key, first, bytes)));
            }
            return Flux.fromIterable(candidates)
                    .filter(m -> {
                        boolean ok = SegmentValidator.metaValid(m, init);
                        if (!ok) {
                            metrics.increment("firstvalid.skipped_flagged");
                        }
                        return ok;
                    })
                    .concatMap(m -> store.data(key, m)
                            .filter(Optional::isPresent)
                            .map(d -> new Segment(key, m, d.get()))
                            .filter(s -> {
                                boolean ok = SegmentValidator.dataValid(s.data(), s.meta(), init);
                                if (!ok) {
                                    metrics.increment("firstvalid.skipped_sanity");
                                }
                                return ok;
                            }), 1)
                    .next()
                    .map(Optional::of)
                    .defaultIfEmpty(Optional.empty());
        }).doOnNext(found -> found.ifPresent(s -> {
            if (cache != null && fillCache) {
                cache.asMap().putIfAbsent(key, s);
            }
        }));
    }
}
