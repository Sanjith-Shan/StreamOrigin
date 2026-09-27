package io.streamorigin.core.store;

import io.streamorigin.core.Fixtures;
import io.streamorigin.core.model.SegmentKey;
import io.streamorigin.core.model.SegmentMeta;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

final class ChunkedSegmentStoreTest {

    private static final int CHUNK = 1000;
    private static final SegmentKey KEY = new SegmentKey("demo", "480p", 42);

    /** Records every key in the order the store writes it. */
    static final class RecordingKv implements KvBackend {
        final MemoryKv inner = new MemoryKv();
        final List<String> puts = Collections.synchronizedList(new ArrayList<>());

        @Override
        public Mono<Void> put(List<Map.Entry<String, byte[]>> entries, Duration ttl) {
            return Mono.defer(() -> {
                entries.forEach(e -> puts.add(e.getKey()));
                return inner.put(entries, ttl);
            });
        }

        @Override
        public Mono<List<byte[]>> get(List<String> keys) {
            return inner.get(keys);
        }

        @Override
        public String name() {
            return "recording";
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, CHUNK - 1, CHUNK, CHUNK + 1, 3 * CHUNK + CHUNK / 2})
    void roundTripAtChunkBoundaries(int size) {
        MemoryKv kv = new MemoryKv();
        ChunkedSegmentStore store = new ChunkedSegmentStore(kv, CHUNK, Duration.ofMinutes(1), StoreListener.NONE);
        byte[] data = Fixtures.media(size);
        store.put(KEY, Fixtures.meta("A", size, false), data).block();

        SegmentMeta meta = store.metas(KEY, List.of("A")).block().get(0).orElseThrow();
        assertThat(meta.size()).isEqualTo(size);
        assertThat(meta.chunks()).isEqualTo(ChunkedSegmentStore.chunkCount(size, CHUNK));
        assertThat(kv.size()).isEqualTo(meta.chunks() + 1);
        assertThat(store.data(KEY, meta).block()).hasValueSatisfying(b -> assertThat(b).isEqualTo(data));
    }

    @Test
    void chunkCountEdges() {
        assertThat(ChunkedSegmentStore.chunkCount(0, CHUNK)).isEqualTo(1);
        assertThat(ChunkedSegmentStore.chunkCount(1, CHUNK)).isEqualTo(1);
        assertThat(ChunkedSegmentStore.chunkCount(CHUNK, CHUNK)).isEqualTo(1);
        assertThat(ChunkedSegmentStore.chunkCount(CHUNK + 1, CHUNK)).isEqualTo(2);
        assertThat(ChunkedSegmentStore.chunkCount(3500, CHUNK)).isEqualTo(4);
    }

    @Property(tries = 300)
    void roundTripForAnySize(@ForAll @IntRange(min = 0, max = 50_000) int size,
                             @ForAll @IntRange(min = 1, max = 8_192) int chunk) {
        ChunkedSegmentStore store = new ChunkedSegmentStore(new MemoryKv(), chunk, Duration.ZERO, StoreListener.NONE);
        byte[] data = Fixtures.media(size);
        store.put(KEY, Fixtures.meta("B", size, true), data).block();
        SegmentMeta meta = store.metas(KEY, List.of("B")).block().get(0).orElseThrow();
        assertThat(meta.defect()).isTrue();
        assertThat(store.data(KEY, meta).block()).hasValueSatisfying(b -> assertThat(b).isEqualTo(data));
    }

    @Test
    void metaIsWrittenAfterEveryChunk() {
        RecordingKv kv = new RecordingKv();
        ChunkedSegmentStore store = new ChunkedSegmentStore(kv, CHUNK, Duration.ZERO, StoreListener.NONE);
        store.put(KEY, Fixtures.meta("A", 3500, false), Fixtures.media(3500)).block();
        String metaKey = ChunkedSegmentStore.metaKey(KEY, "A");
        assertThat(kv.puts).hasSize(5);
        assertThat(kv.puts.get(kv.puts.size() - 1)).isEqualTo(metaKey);
        assertThat(kv.puts.subList(0, 4)).allSatisfy(k -> assertThat(k).contains(":c"));
    }

    @Test
    void missingChunkYieldsEmpty() {
        MemoryKv kv = new MemoryKv();
        ChunkedSegmentStore store = new ChunkedSegmentStore(kv, CHUNK, Duration.ZERO, StoreListener.NONE);
        store.put(KEY, Fixtures.meta("A", 2500, false), Fixtures.media(2500)).block();
        SegmentMeta meta = store.metas(KEY, List.of("A")).block().get(0).orElseThrow();
        // Pretend the chunks expired: a meta pointing at chunks under another pipeline's keys.
        SegmentMeta orphan = new SegmentMeta("Z", meta.sequence(), meta.pts(), meta.size(), false,
                meta.publishedAtMs(), meta.chunks());
        assertThat(store.data(KEY, orphan).block()).isEmpty();
        // A meta that claims more chunks than exist also yields empty.
        SegmentMeta tooMany = new SegmentMeta("A", 1, 0, 3500, false, 0, 4);
        assertThat(store.data(KEY, tooMany).block()).isEmpty();
        // And one that claims fewer bytes than the chunks hold.
        SegmentMeta tooSmall = new SegmentMeta("A", 1, 0, 2000, false, 0, 3);
        assertThat(store.data(KEY, tooSmall).block()).isEmpty();
    }

    @Test
    void metasFollowRequestedOrderWithEmptyForMissing() {
        ChunkedSegmentStore store = new ChunkedSegmentStore(new MemoryKv(), CHUNK, Duration.ZERO, StoreListener.NONE);
        store.put(KEY, Fixtures.meta("B", 1500, false), Fixtures.media(1500)).block();
        store.put(KEY, Fixtures.meta("C", 10, true), Fixtures.media(10)).block();

        List<Optional<SegmentMeta>> metas = store.metas(KEY, List.of("A", "B", "X", "C")).block();
        assertThat(metas).hasSize(4);
        assertThat(metas.get(0)).isEmpty();
        assertThat(metas.get(1)).hasValueSatisfying(m -> assertThat(m.pipeline()).isEqualTo("B"));
        assertThat(metas.get(2)).isEmpty();
        assertThat(metas.get(3)).hasValueSatisfying(m -> {
            assertThat(m.pipeline()).isEqualTo("C");
            assertThat(m.defect()).isTrue();
        });
        assertThat(store.metas(new SegmentKey("demo", "480p", 43), List.of("B")).block()).containsExactly(Optional.empty());
    }

    @Test
    void initSegmentsHaveTheirOwnKeys() {
        ChunkedSegmentStore store = new ChunkedSegmentStore(new MemoryKv(), CHUNK, Duration.ZERO, StoreListener.NONE);
        SegmentKey init = new SegmentKey("demo", "480p", SegmentKey.INIT);
        byte[] data = Fixtures.body("ftyp", 700);
        store.put(init, Fixtures.meta("A", 700, false), data).block();
        SegmentMeta meta = store.metas(init, List.of("A")).block().get(0).orElseThrow();
        assertThat(store.data(init, meta).block()).hasValueSatisfying(b -> assertThat(b).isEqualTo(data));
        assertThat(store.metas(new SegmentKey("demo", "480p", 0), List.of("A")).block()).containsExactly(Optional.empty());
    }

    @Test
    void metaEncodingRoundTrips() {
        SegmentMeta m = new SegmentMeta("A", 17, 123_456, 2048, true, 1_700_000_000_000L, 3);
        assertThat(SegmentMeta.decode(m.encode())).isEqualTo(m);
    }
}
