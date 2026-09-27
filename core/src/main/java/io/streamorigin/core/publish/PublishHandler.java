package io.streamorigin.core.publish;

import io.streamorigin.core.control.ControlPlane;
import io.streamorigin.core.metrics.OriginMetrics;
import io.streamorigin.core.model.EventDef;
import io.streamorigin.core.model.Segment;
import io.streamorigin.core.model.SegmentKey;
import io.streamorigin.core.model.SegmentMeta;
import io.streamorigin.core.store.ChunkedSegmentStore;
import io.streamorigin.core.store.SegmentStore;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.BodyExtractors;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * The publish path. A packager PUTs a pipeline's copy; it is chunked into the write store and
 * acknowledged. Only then, off the acknowledgement path, is it replicated to the read store and
 * pushed to the serve side, so a slow serve side cannot slow the encoder's writes.
 */
public final class PublishHandler {

    private final ControlPlane controlPlane;
    private final SegmentStore writeStore;
    private final SegmentStore replicaStore;
    private final EdgeNotifier notifier;
    private final boolean writeThrough;
    private final int chunkBytes;
    private final OriginMetrics metrics;
    private final LongSupplier clock;

    public PublishHandler(ControlPlane controlPlane, SegmentStore writeStore, SegmentStore replicaStore,
                          EdgeNotifier notifier, boolean writeThrough, int chunkBytes, OriginMetrics metrics,
                          LongSupplier clock) {
        this.controlPlane = controlPlane;
        this.writeStore = writeStore;
        this.replicaStore = replicaStore;
        this.notifier = notifier;
        this.writeThrough = writeThrough;
        this.chunkBytes = chunkBytes;
        this.metrics = metrics;
        this.clock = clock;
    }

    public Mono<ServerResponse> put(ServerRequest req) {
        long start = System.nanoTime();
        String pipeline = req.pathVariable("pipeline");
        String eventId = req.pathVariable("event");
        String renditionId = req.pathVariable("rendition");
        String file = req.pathVariable("file");

        Optional<EventDef> event = controlPlane.event(eventId).map(ControlPlane.EventState::def);
        if (event.isEmpty() || event.get().rendition(renditionId).isEmpty() || event.get().pipeline(pipeline).isEmpty()) {
            metrics.increment("publish.rejected");
            return req.bodyToMono(byte[].class).then(ServerResponse.badRequest().build());
        }
        long k;
        if (file.equals("init.mp4")) {
            k = SegmentKey.INIT;
        } else if (file.endsWith(".m4s")) {
            try {
                k = Long.parseLong(file.substring(0, file.length() - 4));
            } catch (NumberFormatException e) {
                return ServerResponse.badRequest().build();
            }
        } else {
            return ServerResponse.badRequest().build();
        }
        SegmentKey key = new SegmentKey(eventId, renditionId, k);
        long seq = header(req, "X-Sequence", k);
        long pts = header(req, "X-PTS", 0);
        boolean defect = "1".equals(req.headers().firstHeader("X-Defect"))
                || "true".equalsIgnoreCase(req.headers().firstHeader("X-Defect"));

        return DataBufferUtils.join(req.body(BodyExtractors.toDataBuffers()))
                .map(PublishHandler::drain)
                .defaultIfEmpty(new byte[0])
                .flatMap(bytes -> {
                    long publishedAt = clock.getAsLong();
                    SegmentMeta meta = new SegmentMeta(pipeline, seq, pts, bytes.length, defect, publishedAt,
                            ChunkedSegmentStore.chunkCount(bytes.length, chunkBytes));
                    return writeStore.put(key, meta, bytes).then(Mono.fromSupplier(() -> {
                        long micros = (System.nanoTime() - start) / 1000;
                        metrics.recordMicros("write", micros);
                        metrics.increment("publish.ok");
                        if (defect) {
                            metrics.increment("publish.flagged_defect");
                        }
                        afterAck(new Segment(key, meta, bytes));
                        return micros;
                    }));
                })
                .flatMap(micros -> ServerResponse.status(HttpStatus.CREATED)
                        .header("X-Write-Micros", Long.toString(micros)).build())
                .onErrorResume(e -> {
                    metrics.increment("publish.errors");
                    return ServerResponse.status(HttpStatus.SERVICE_UNAVAILABLE).build();
                });
    }

    private void afterAck(Segment seg) {
        long ackNanos = System.nanoTime();
        Mono<Void> replicate = replicaStore == null ? Mono.empty()
                : replicaStore.put(seg.key(), seg.meta(), seg.data())
                .doOnSuccess(v -> metrics.recordNanosSince("replication", ackNanos))
                .doOnError(e -> metrics.increment("replication.errors"))
                .onErrorResume(e -> Mono.empty());
        Mono<Void> pipeline = writeThrough
                ? Mono.when(replicate, notifier.notify(seg, true))
                : replicate.then(notifier.notify(seg, false));
        pipeline.doOnSuccess(v -> metrics.recordNanosSince("publish_to_servable", ackNanos)).subscribe(v -> {
        }, e -> metrics.increment("notify.errors"));
    }

    private static byte[] drain(DataBuffer buf) {
        try {
            byte[] out = new byte[buf.readableByteCount()];
            buf.read(out);
            return out;
        } finally {
            DataBufferUtils.release(buf);
        }
    }

    private static long header(ServerRequest req, String name, long fallback) {
        String v = req.headers().firstHeader(name);
        if (v == null) {
            return fallback;
        }
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
