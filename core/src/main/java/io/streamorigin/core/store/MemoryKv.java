package io.streamorigin.core.store;

import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** In-process backend for tests. Ignores TTL. */
public final class MemoryKv implements KvBackend {

    private final Map<String, byte[]> map = new ConcurrentHashMap<>();

    @Override
    public Mono<Void> put(List<Map.Entry<String, byte[]>> entries, Duration ttl) {
        return Mono.fromRunnable(() -> entries.forEach(e -> map.put(e.getKey(), e.getValue())));
    }

    @Override
    public Mono<List<byte[]>> get(List<String> keys) {
        return Mono.fromSupplier(() -> {
            List<byte[]> out = new ArrayList<>(keys.size());
            keys.forEach(k -> out.add(map.get(k)));
            return out;
        });
    }

    public int size() {
        return map.size();
    }

    @Override
    public String name() {
        return "memory";
    }
}
