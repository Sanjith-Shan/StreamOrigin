package io.streamorigin.core.store;

import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * The key-value abstraction the segment store is built on. Values are opaque bytes and are
 * expected to be small (at most one chunk); large objects are split by {@link ChunkedSegmentStore}.
 */
public interface KvBackend extends AutoCloseable {

    /** Writes every entry and completes when all are acknowledged. */
    Mono<Void> put(List<Map.Entry<String, byte[]>> entries, Duration ttl);

    /** Reads the keys in order; the result holds {@code null} for a missing key. */
    Mono<List<byte[]>> get(List<String> keys);

    String name();

    @Override
    default void close() {
    }
}
