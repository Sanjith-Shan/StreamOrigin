package io.streamorigin.edge;

import io.streamorigin.core.config.OriginRuntime;
import io.streamorigin.core.model.SegmentKey;
import io.streamorigin.core.schedule.Schedule;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Boots the edge server in combined mode on the in-memory store and drives the publish and serve
 * paths over HTTP. Segment indices are computed from the same schedule the server uses.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
final class EdgeServerEndToEndTest {

    private static final long DURATION_MS = 2000;
    private static final long ENCODE_DELAY_MS = 600;
    private static final long EPOCH_MS = System.currentTimeMillis() - 60_000;
    private static final Schedule SCHEDULE = new Schedule(EPOCH_MS, DURATION_MS, ENCODE_DELAY_MS);

    @DynamicPropertySource
    static void originProperties(DynamicPropertyRegistry registry) {
        Path events;
        try {
            Path dir = Files.createTempDirectory("edge-e2e");
            dir.toFile().deleteOnExit();
            events = dir.resolve("events.yaml");
            Files.writeString(events, """
                    events:
                      - id: demo
                        epoch: %d
                        segmentDurationMs: %d
                        dvrWindowSegments: 150
                        renditions:
                          - { id: 480p, bandwidth: 1000000, width: 854, height: 480 }
                        pipelines:
                          - { id: A, encodeDelayMs: %d }
                          - { id: B, encodeDelayMs: %d }
                    """.formatted(EPOCH_MS, DURATION_MS, ENCODE_DELAY_MS, ENCODE_DELAY_MS));
            events.toFile().deleteOnExit();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        registry.add("origin.role", () -> "combined");
        registry.add("origin.store.backend", () -> "memory");
        registry.add("origin.events-file", events::toString);
    }

    @Autowired
    private WebTestClient client;

    @Autowired
    private OriginRuntime runtime;

    private static long liveEdge() {
        return SCHEDULE.liveEdge(System.currentTimeMillis());
    }

    /** A body the validator accepts: first box {@code styp}, above the media minimum. */
    private static byte[] segmentBody(long k) {
        byte[] b = new byte[4096];
        for (int i = 0; i < b.length; i++) {
            b[i] = (byte) (i + k);
        }
        System.arraycopy("styp".getBytes(StandardCharsets.ISO_8859_1), 0, b, 4, 4);
        return b;
    }

    private void publish(String pipeline, long k, byte[] body, boolean defect) {
        WebTestClient.RequestBodySpec put = client.put().uri("/publish/{p}/demo/480p/{k}.m4s", pipeline, k)
                .contentType(MediaType.APPLICATION_OCTET_STREAM);
        if (defect) {
            put.header("X-Defect", "1");
        }
        put.bodyValue(body).exchange().expectStatus().isCreated();
    }

    @Test
    void publishedSegmentIsServed() {
        long k = liveEdge();
        assertThat(k).isGreaterThan(20);
        byte[] body = segmentBody(k);
        publish("A", k, body, false);

        client.get().uri("/live/demo/480p/{k}.m4s", k).exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-Pipeline", "A")
                .expectHeader().contentType("video/iso.segment")
                .expectHeader().valueEquals("Cache-Control", "public, max-age=86400, immutable")
                .expectBody(byte[].class).isEqualTo(body);
    }

    @Test
    void farFutureSegmentIsRejectedWithCacheableMiss() {
        long k = liveEdge() + 1000;
        client.get().uri("/live/demo/480p/{k}.m4s", k).exchange()
                .expectStatus().isNotFound()
                .expectHeader().value("Cache-Control", v -> assertThat(v).matches("max-age=\\d+"));
    }

    @Test
    void unknownEventIsNotFound() {
        client.get().uri("/live/nope/480p/1.m4s").exchange().expectStatus().isNotFound();
        client.get().uri("/live/nope/manifest.mpd").exchange().expectStatus().isNotFound();
        client.get().uri("/live/demo/4k/1.m4s").exchange().expectStatus().isNotFound();
    }

    @Test
    void requestAheadOfEdgeIsHeldUntilPublished() throws Exception {
        long k = liveEdge() + 1;
        SegmentKey key = new SegmentKey("demo", "480p", k);
        byte[] body = segmentBody(k);
        CompletableFuture<Void> publisher = CompletableFuture.runAsync(() -> {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            try {
                while (!runtime.waiters.isWaiting(key) && System.nanoTime() < deadline) {
                    Thread.sleep(10);
                }
                Thread.sleep(300);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            publish("A", k, body, false);
        });

        client.mutate().responseTimeout(Duration.ofSeconds(10)).build()
                .get().uri("/live/demo/480p/{k}.m4s", k).exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-Served-From", "HOLD")
                .expectHeader().valueEquals("X-Pipeline", "A")
                .expectBody(byte[].class).isEqualTo(body);
        publisher.get(5, TimeUnit.SECONDS);
    }

    @Test
    void defectiveFirstPipelineFailsOverToSecond() {
        long k = liveEdge() - 20;
        publish("A", k, segmentBody(k), true);
        publish("B", k, segmentBody(k), false);

        client.get().uri("/live/demo/480p/{k}.m4s", k).exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-Pipeline", "B");
    }

    @Test
    void manifestsAreServed() {
        client.get().uri("/live/demo/manifest.mpd").exchange()
                .expectStatus().isOk()
                .expectHeader().contentType("application/dash+xml")
                .expectBody(String.class).value(mpd -> assertThat(mpd)
                        .contains("startNumber=\"0\"")
                        .contains("media=\"$RepresentationID$/$Number$.m4s\""));

        client.get().uri("/live/demo/480p/index.m3u8").exchange()
                .expectStatus().isOk()
                .expectHeader().contentType("application/vnd.apple.mpegurl")
                .expectBody(String.class).value(pl -> assertThat(pl).contains("#EXT-X-MAP:URI=\"init.mp4\""));
    }

    @Test
    void statsExposeCounters() {
        publish("B", liveEdge() - 30, segmentBody(1), false);
        client.get().uri("/internal/stats").exchange()
                .expectStatus().isOk()
                .expectHeader().contentType(MediaType.APPLICATION_JSON)
                .expectBody()
                .jsonPath("$.role").isEqualTo("combined")
                .jsonPath("$.backend").isEqualTo("memory")
                .jsonPath("$.features.first_valid").isEqualTo(true)
                .jsonPath("$.counters['publish.ok']").value(v -> assertThat(((Number) v).longValue()).isPositive())
                .jsonPath("$.counters").isMap();
    }
}
