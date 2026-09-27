package io.streamorigin.core.store;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.TtlDB;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Embedded RocksDB backend for the single-process demo. RocksDB calls block, so they run on a
 * bounded elastic scheduler and never on a Netty event loop. TTL is fixed per database by
 * {@link TtlDB}, so the per-call TTL is ignored.
 */
public final class RocksKv implements KvBackend {

    static {
        RocksDB.loadLibrary();
    }

    private final TtlDB db;
    private final Options options;
    private final WriteOptions writeOptions;
    private final Scheduler scheduler = Schedulers.boundedElastic();

    public RocksKv(Path dir, Duration ttl) {
        try {
            Files.createDirectories(dir);
            options = new Options().setCreateIfMissing(true);
            db = TtlDB.open(options, dir.toString(), (int) ttl.toSeconds(), false);
            writeOptions = new WriteOptions();
        } catch (IOException | RocksDBException e) {
            throw new IllegalStateException("cannot open RocksDB at " + dir, e);
        }
    }

    @Override
    public Mono<Void> put(List<Map.Entry<String, byte[]>> entries, Duration ttl) {
        return Mono.<Void>fromCallable(() -> {
            try (WriteBatch batch = new WriteBatch()) {
                for (Map.Entry<String, byte[]> e : entries) {
                    batch.put(e.getKey().getBytes(StandardCharsets.UTF_8), e.getValue());
                }
                db.write(writeOptions, batch);
            }
            return null;
        }).subscribeOn(scheduler);
    }

    @Override
    public Mono<List<byte[]>> get(List<String> keys) {
        return Mono.fromCallable(() -> {
            List<byte[]> raw = new ArrayList<>(keys.size());
            keys.forEach(k -> raw.add(k.getBytes(StandardCharsets.UTF_8)));
            return new ArrayList<>(db.multiGetAsList(raw));
        }).map(l -> (List<byte[]>) l).subscribeOn(scheduler);
    }

    @Override
    public String name() {
        return "rocksdb";
    }

    @Override
    public void close() {
        db.close();
        writeOptions.close();
        options.close();
    }
}
