package io.streamorigin.core.store;

import io.streamorigin.core.Fixtures;
import io.streamorigin.core.model.SegmentKey;
import io.streamorigin.core.model.SegmentMeta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

final class RocksKvTest {

    @Test
    void chunkedRoundTripAndMissingKeys(@TempDir Path dir) {
        try (RocksKv kv = new RocksKv(dir.resolve("db"), Duration.ofMinutes(10))) {
            ChunkedSegmentStore store = new ChunkedSegmentStore(kv, 1000, Duration.ofMinutes(10), StoreListener.NONE);
            SegmentKey key = new SegmentKey("demo", "480p", 3);
            byte[] data = Fixtures.media(3500);
            store.put(key, Fixtures.meta("A", data.length, false), data).block();

            var metas = store.metas(key, List.of("B", "A")).block();
            assertThat(metas.get(0)).isEmpty();
            SegmentMeta meta = metas.get(1).orElseThrow();
            assertThat(meta.chunks()).isEqualTo(4);
            assertThat(store.data(key, meta).block()).hasValueSatisfying(b -> assertThat(b).isEqualTo(data));

            List<byte[]> raw = kv.get(List.of("nope", ChunkedSegmentStore.chunkKey(key, "A", 3))).block();
            assertThat(raw.get(0)).isNull();
            assertThat(raw.get(1)).hasSize(500);
        }
    }
}
