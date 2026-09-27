package io.streamorigin.core.store;

import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/** Failure injection: adds a fixed delay to reads and/or writes of the wrapped backend. */
public final class SlowKv implements KvBackend {

    private final KvBackend inner;
    private final Duration readDelay;
    private final Duration writeDelay;

    public SlowKv(KvBackend inner, Duration readDelay, Duration writeDelay) {
        this.inner = inner;
        this.readDelay = readDelay;
        this.writeDelay = writeDelay;
    }

    @Override
    public Mono<Void> put(List<Map.Entry<String, byte[]>> entries, Duration ttl) {
        Mono<Void> op = inner.put(entries, ttl);
        return writeDelay.isZero() ? op : Mono.delay(writeDelay).then(op);
    }

    @Override
    public Mono<List<byte[]>> get(List<String> keys) {
        Mono<List<byte[]>> op = inner.get(keys);
        return readDelay.isZero() ? op : Mono.delay(readDelay).then(op);
    }

    @Override
    public String name() {
        return inner.name() + "+slow";
    }

    @Override
    public void close() {
        inner.close();
    }
}
