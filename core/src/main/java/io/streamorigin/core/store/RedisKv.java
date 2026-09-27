package io.streamorigin.core.store;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Redis backend over one Lettuce connection. Lettuce pipelines commands on a single connection,
 * so whoever shares a {@code RedisKv} shares its queue; that is the point of giving the write
 * path and the read path separate instances.
 */
public final class RedisKv implements KvBackend {

    private final RedisClient client;
    private final StatefulRedisConnection<String, byte[]> connection;
    private final RedisReactiveCommands<String, byte[]> commands;
    private final String name;

    public RedisKv(String uri, String name) {
        this.client = RedisClient.create(RedisURI.create(uri));
        this.client.setOptions(ClientOptions.builder().autoReconnect(true).build());
        this.connection = client.connect(RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE));
        this.commands = connection.reactive();
        this.name = name;
    }

    @Override
    public Mono<Void> put(List<Map.Entry<String, byte[]>> entries, Duration ttl) {
        SetArgs args = ttl.isZero() ? new SetArgs() : SetArgs.Builder.px(ttl.toMillis());
        return Flux.fromIterable(entries)
                .flatMap(e -> commands.set(e.getKey(), e.getValue(), args), entries.size())
                .then();
    }

    @Override
    public Mono<List<byte[]>> get(List<String> keys) {
        if (keys.isEmpty()) {
            return Mono.just(List.of());
        }
        return commands.mget(keys.toArray(String[]::new))
                .collectList()
                .map(kvs -> {
                    List<byte[]> out = new ArrayList<>(kvs.size());
                    for (KeyValue<String, byte[]> kv : kvs) {
                        out.add(kv.hasValue() ? kv.getValue() : null);
                    }
                    return out;
                });
    }

    /** Test and bench helper. */
    public Mono<String> flushAll() {
        return commands.flushall();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void close() {
        connection.close();
        client.shutdown();
    }
}
