package io.streamorigin.core.store;

import io.streamorigin.core.model.SegmentKey;
import io.streamorigin.core.model.SegmentMeta;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Optional;

/** Stores each pipeline's copy of a segment separately so the serve path can choose between them. */
public interface SegmentStore {

    Mono<Void> put(SegmentKey key, SegmentMeta meta, byte[] data);

    /** One entry per pipeline, in the order given; empty where that pipeline has no copy. */
    Mono<List<Optional<SegmentMeta>>> metas(SegmentKey key, List<String> pipelines);

    /** The copy's bytes, or empty if a chunk is missing (expired or never written). */
    Mono<Optional<byte[]>> data(SegmentKey key, SegmentMeta meta);

    String name();
}
