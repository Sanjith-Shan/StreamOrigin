package io.streamorigin.core.store;

import io.streamorigin.core.model.SegmentKey;
import io.streamorigin.core.model.SegmentMeta;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Splits segment bodies into fixed-size chunks over a {@link KvBackend}. Chunks are written
 * first and the meta record last, so any reader that sees the meta sees every chunk.
 */
public final class ChunkedSegmentStore implements SegmentStore {

    private final KvBackend kv;
    private final int chunkBytes;
    private final Duration ttl;
    private final StoreListener listener;

    public ChunkedSegmentStore(KvBackend kv, int chunkBytes, Duration ttl, StoreListener listener) {
        if (chunkBytes <= 0) {
            throw new IllegalArgumentException("chunk size must be positive");
        }
        this.kv = kv;
        this.chunkBytes = chunkBytes;
        this.ttl = ttl;
        this.listener = listener;
    }

    public static int chunkCount(int size, int chunkBytes) {
        return Math.max(1, (size + chunkBytes - 1) / chunkBytes);
    }

    public int chunkBytes() {
        return chunkBytes;
    }

    static String metaKey(SegmentKey key, String pipeline) {
        return key.storageId(pipeline) + ":m";
    }

    static String chunkKey(SegmentKey key, String pipeline, int i) {
        return key.storageId(pipeline) + ":c" + i;
    }

    @Override
    public Mono<Void> put(SegmentKey key, SegmentMeta meta, byte[] data) {
        int chunks = chunkCount(data.length, chunkBytes);
        SegmentMeta stored = new SegmentMeta(meta.pipeline(), meta.sequence(), meta.pts(), data.length,
                meta.defect(), meta.publishedAtMs(), chunks);
        List<Map.Entry<String, byte[]>> body = new ArrayList<>(chunks);
        for (int i = 0; i < chunks; i++) {
            int from = i * chunkBytes;
            int to = Math.min(data.length, from + chunkBytes);
            body.add(new AbstractMap.SimpleImmutableEntry<>(chunkKey(key, meta.pipeline(), i),
                    Arrays.copyOfRange(data, from, to)));
        }
        List<Map.Entry<String, byte[]>> head = List.of(
                new AbstractMap.SimpleImmutableEntry<>(metaKey(key, meta.pipeline()), stored.encode()));
        listener.write(chunks, data.length);
        return kv.put(body, ttl).then(kv.put(head, ttl));
    }

    @Override
    public Mono<List<Optional<SegmentMeta>>> metas(SegmentKey key, List<String> pipelines) {
        List<String> keys = pipelines.stream().map(p -> metaKey(key, p)).toList();
        listener.metaRead(keys.size());
        return kv.get(keys).map(raw -> {
            List<Optional<SegmentMeta>> out = new ArrayList<>(raw.size());
            for (byte[] r : raw) {
                out.add(r == null ? Optional.empty() : Optional.of(SegmentMeta.decode(r)));
            }
            return out;
        });
    }

    @Override
    public Mono<Optional<byte[]>> data(SegmentKey key, SegmentMeta meta) {
        List<String> keys = new ArrayList<>(meta.chunks());
        for (int i = 0; i < meta.chunks(); i++) {
            keys.add(chunkKey(key, meta.pipeline(), i));
        }
        return kv.get(keys).map(parts -> {
            byte[] out = new byte[meta.size()];
            int pos = 0;
            for (byte[] part : parts) {
                if (part == null || pos + part.length > out.length) {
                    return Optional.<byte[]>empty();
                }
                System.arraycopy(part, 0, out, pos, part.length);
                pos += part.length;
            }
            listener.dataRead(parts.size(), pos);
            return pos == out.length ? Optional.of(out) : Optional.<byte[]>empty();
        });
    }

    @Override
    public String name() {
        return "chunked(" + kv.name() + ")";
    }
}
