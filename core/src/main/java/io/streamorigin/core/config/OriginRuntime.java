package io.streamorigin.core.config;

import io.streamorigin.core.config.OriginProperties.Role;
import io.streamorigin.core.control.ControlPlane;
import io.streamorigin.core.metrics.OriginMetrics;
import io.streamorigin.core.model.EventDef;
import io.streamorigin.core.publish.EdgeNotifier;
import io.streamorigin.core.publish.HttpEdgeNotifier;
import io.streamorigin.core.publish.PublishHandler;
import io.streamorigin.core.serve.AdmissionController;
import io.streamorigin.core.serve.ConcurrencyLimit;
import io.streamorigin.core.serve.EdgeSink;
import io.streamorigin.core.serve.GradientLimit;
import io.streamorigin.core.serve.LiveEdgeWaiters;
import io.streamorigin.core.serve.SegmentResolver;
import io.streamorigin.core.serve.ServeHandler;
import io.streamorigin.core.store.ChunkedSegmentStore;
import io.streamorigin.core.store.KvBackend;
import io.streamorigin.core.store.MemoryKv;
import io.streamorigin.core.store.RedisKv;
import io.streamorigin.core.store.RocksKv;
import io.streamorigin.core.store.SegmentStore;
import io.streamorigin.core.store.SlowKv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;

/**
 * Builds the components one process needs for its role. A publish-server gets the publish path
 * and a connection to each store; an edge-server gets the serve path and a read connection; a
 * combined process gets both, and in naive mode both share one store connection.
 */
public final class OriginRuntime implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OriginRuntime.class);

    public final OriginProperties props;
    public final OriginProperties.Features features;
    public final ControlPlane controlPlane;
    public final OriginMetrics metrics;
    public final PublishHandler publishHandler;
    public final ServeHandler serveHandler;
    public final EdgeSink edgeSink;
    public final AdmissionController admission;
    public final SegmentResolver resolver;
    public final LiveEdgeWaiters waiters;
    private final List<KvBackend> backends = new ArrayList<>();

    public OriginRuntime(OriginProperties props, ControlPlane controlPlane, OriginMetrics metrics,
                         WebClient.Builder webClient, LongSupplier clock) {
        this.props = props;
        this.features = props.effectiveFeatures();
        this.controlPlane = controlPlane;
        this.metrics = metrics;
        Role role = props.getRole();
        OriginProperties.Store sp = props.getStore();
        Duration ttl = Duration.ofSeconds(sp.getTtlSeconds());
        boolean shared = props.sharedStore();

        KvBackend writeKv = null;
        KvBackend replicaKv = null;
        KvBackend readKv = null;
        switch (sp.getBackend()) {
            case "memory" -> {
                requireCombined(role, "memory");
                writeKv = track(new MemoryKv());
                readKv = shared ? writeKv : track(new MemoryKv());
                replicaKv = shared ? null : readKv;
            }
            case "rocksdb" -> {
                requireCombined(role, "rocksdb");
                writeKv = track(new RocksKv(Path.of(sp.getRocksdbPath()), ttl));
                readKv = writeKv;
            }
            case "redis" -> {
                if (role != Role.EDGE) {
                    writeKv = track(new RedisKv(sp.getWriteUri(), "redis-write"));
                    if (!shared) {
                        replicaKv = track(new RedisKv(sp.getReadUri(), "redis-replica"));
                    }
                }
                if (role != Role.PUBLISH) {
                    readKv = shared && writeKv != null ? writeKv
                            : track(new RedisKv(shared ? sp.getWriteUri() : sp.getReadUri(), "redis-read"));
                }
            }
            default -> throw new IllegalArgumentException("unknown store backend " + sp.getBackend());
        }
        if (readKv != null && sp.getReadDelayMs() > 0) {
            readKv = new SlowKv(readKv, Duration.ofMillis(sp.getReadDelayMs()), Duration.ZERO);
        }
        if (writeKv != null && sp.getWriteDelayMs() > 0) {
            writeKv = new SlowKv(writeKv, Duration.ZERO, Duration.ofMillis(sp.getWriteDelayMs()));
        }

        int chunk = sp.getChunkBytes();
        SegmentStore writeStore = writeKv == null ? null : new ChunkedSegmentStore(writeKv, chunk, ttl, metrics);
        SegmentStore replicaStore = replicaKv == null ? null : new ChunkedSegmentStore(replicaKv, chunk, ttl, metrics);
        SegmentStore readStore = readKv == null ? null : new ChunkedSegmentStore(readKv, chunk, ttl, metrics);

        if (role != Role.PUBLISH) {
            long cacheBytes = features.isWriteThrough() ? props.getCache().getMaxMb() * 1024 * 1024 : 0;
            resolver = new SegmentResolver(readStore, this::pipelineOrder, features.isFirstValid(),
                    features.isCoalescing(), cacheBytes, Duration.ofSeconds(props.getCache().getSeconds()), metrics);
            waiters = features.isHoldOpen()
                    ? new LiveEdgeWaiters(resolver, metrics, clock, Duration.ofMillis(props.getHold().getSweepMs()))
                    : null;
            OriginProperties.Admission ap = props.getAdmission();
            ConcurrencyLimit limit = "fixed".equals(ap.getMode())
                    ? ConcurrencyLimit.fixed(ap.getFixedLimit())
                    : new GradientLimit(ap.getInitialLimit(), ap.getMinLimit(), ap.getMaxLimit(), ap.getTolerance(), ap.getWindowMs());
            admission = new AdmissionController(features.isPriority(), limit, ap.getDvrShare(), ap.getLiveRate(),
                    ap.getDvrRate(), metrics);
            serveHandler = new ServeHandler(controlPlane, props, resolver, waiters, admission, metrics, clock);
            edgeSink = new EdgeSink(resolver, waiters, metrics);
        } else {
            resolver = null;
            waiters = null;
            admission = null;
            serveHandler = null;
            edgeSink = null;
        }

        if (role != Role.EDGE) {
            EdgeNotifier notifier = role == Role.COMBINED
                    ? (seg, withBytes) -> Mono.fromRunnable(() -> edgeSink.accept(seg, withBytes))
                    : new HttpEdgeNotifier(webClient, props.getEdges(), metrics);
            // Combined and not shared: the replica store is the one the serve path reads.
            publishHandler = new PublishHandler(controlPlane, writeStore, replicaStore, notifier,
                    features.isWriteThrough(), chunk, metrics, clock);
        } else {
            publishHandler = null;
        }
        log.info("origin role={} naive={} backend={} sharedStore={} features: cp={} firstValid={} hold={} negCache={} priority={} writeThrough={} coalescing={}",
                role, props.isNaive(), sp.getBackend(), shared, features.isControlPlane(), features.isFirstValid(),
                features.isHoldOpen(), features.isNegativeCache(), features.isPriority(), features.isWriteThrough(),
                features.isCoalescing());
    }

    private List<String> pipelineOrder(String eventId) {
        return controlPlane.event(eventId).map(s -> s.def().pipelineOrder()).orElseGet(() -> {
            // Without a known event (naive origin, bogus request) fall back to every known pipeline id.
            return controlPlane.events().stream().flatMap(s -> s.def().pipelineOrder().stream()).distinct().toList();
        });
    }

    private static void requireCombined(Role role, String backend) {
        if (role != Role.COMBINED) {
            throw new IllegalArgumentException(backend + " backend is in-process and needs role COMBINED");
        }
    }

    private KvBackend track(KvBackend kv) {
        backends.add(kv);
        return kv;
    }

    public EventDef event(String id) {
        return controlPlane.event(id).map(ControlPlane.EventState::def).orElse(null);
    }

    @Override
    public void close() {
        if (waiters != null) {
            waiters.close();
        }
        backends.forEach(KvBackend::close);
    }
}
