package io.streamorigin.core.store;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.streamorigin.core.Fixtures;
import io.streamorigin.core.model.SegmentKey;
import io.streamorigin.core.model.SegmentMeta;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
final class RedisKvTest {

    @Container
    private static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine"))
            .withExposedPorts(6379);

    private static RedisKv kv;
    private static String uri;

    @BeforeAll
    static void connect() {
        uri = "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379);
        kv = new RedisKv(uri, "redis-test");
    }

    @AfterAll
    static void disconnect() {
        kv.close();
    }

    @Test
    void largeSegmentRoundTripsWithTtl() {
        int chunk = 1 << 20;
        ChunkedSegmentStore store = new ChunkedSegmentStore(kv, chunk, Duration.ofSeconds(120), StoreListener.NONE);
        SegmentKey key = new SegmentKey("demo", "1080p", 7);
        int size = (int) (2.5 * chunk);
        byte[] data = Fixtures.media(size);

        store.put(key, Fixtures.meta("A", size, false), data).block(Duration.ofSeconds(10));

        SegmentMeta meta = store.metas(key, List.of("A", "B")).block().get(0).orElseThrow();
        assertThat(meta.chunks()).isEqualTo(3);
        assertThat(store.metas(key, List.of("A", "B")).block().get(1)).isEmpty();
        assertThat(store.data(key, meta).block(Duration.ofSeconds(10)))
                .hasValueSatisfying(b -> assertThat(b).isEqualTo(data));

        RedisClient client = RedisClient.create(uri);
        try (StatefulRedisConnection<String, String> c = client.connect()) {
            long metaTtl = c.sync().pttl(ChunkedSegmentStore.metaKey(key, "A"));
            long chunkTtl = c.sync().pttl(ChunkedSegmentStore.chunkKey(key, "A", 2));
            assertThat(metaTtl).isPositive().isLessThanOrEqualTo(120_000);
            assertThat(chunkTtl).isPositive().isLessThanOrEqualTo(120_000);
        } finally {
            client.shutdown();
        }
    }

    @Test
    void mgetReturnsNullForMissingKeys() {
        Map.Entry<String, byte[]> e = new AbstractMap.SimpleImmutableEntry<>("present", new byte[]{1, 2, 3});
        kv.put(List.of(e), Duration.ZERO).block();
        List<byte[]> got = kv.get(List.of("absent-1", "present", "absent-2")).block();
        assertThat(got).hasSize(3);
        assertThat(got.get(0)).isNull();
        assertThat(got.get(1)).containsExactly(1, 2, 3);
        assertThat(got.get(2)).isNull();
        assertThat(kv.get(List.of()).block()).isEmpty();
    }

    @Test
    void emptySegmentRoundTrips() {
        ChunkedSegmentStore store = new ChunkedSegmentStore(kv, 1000, Duration.ofSeconds(60), StoreListener.NONE);
        SegmentKey key = new SegmentKey("demo", "480p", 1);
        store.put(key, Fixtures.meta("B", 0, false), new byte[0]).block();
        SegmentMeta meta = store.metas(key, List.of("B")).block().get(0).orElseThrow();
        assertThat(store.data(key, meta).block()).hasValueSatisfying(b -> assertThat(b).isEmpty());
    }
}
