package io.streamorigin.core.serve;

import io.streamorigin.core.config.OriginProperties;
import io.streamorigin.core.control.ControlPlane;
import io.streamorigin.core.control.ControlPlane.EventState;
import io.streamorigin.core.control.Verdict;
import io.streamorigin.core.manifest.Manifests;
import io.streamorigin.core.metrics.OriginMetrics;
import io.streamorigin.core.model.EventDef;
import io.streamorigin.core.model.Notification;
import io.streamorigin.core.model.Segment;
import io.streamorigin.core.model.SegmentKey;
import io.streamorigin.core.schedule.Schedule;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * The serve path, in the order the design calls for:
 * control plane, request class and admission, first valid copy (cache, then coalesced store read),
 * live-edge hold, negative caching with a TTL computed from the schedule, then the bytes.
 */
public final class ServeHandler {

    public static final String PERMIT_ATTR = "origin.permit";
    public static final String CLASS_ATTR = "origin.class";
    public static final String START_ATTR = "origin.start";

    static final MediaType SEGMENT_TYPE = MediaType.parseMediaType("video/iso.segment");
    static final MediaType MP4 = MediaType.parseMediaType("video/mp4");
    static final MediaType DASH = MediaType.parseMediaType("application/dash+xml");
    static final MediaType HLS = MediaType.parseMediaType("application/vnd.apple.mpegurl");
    static final int MAX_NOTIFICATIONS_IN_HEADER = 8;

    private final ControlPlane controlPlane;
    private final OriginProperties.Features features;
    private final OriginProperties props;
    private final SegmentResolver resolver;
    private final LiveEdgeWaiters waiters;
    private final AdmissionController admission;
    private final OriginMetrics metrics;
    private final LongSupplier clock;

    public ServeHandler(ControlPlane controlPlane, OriginProperties props, SegmentResolver resolver,
                        LiveEdgeWaiters waiters, AdmissionController admission, OriginMetrics metrics,
                        LongSupplier clock) {
        this.controlPlane = controlPlane;
        this.props = props;
        this.features = props.effectiveFeatures();
        this.resolver = resolver;
        this.waiters = waiters;
        this.admission = admission;
        this.metrics = metrics;
        this.clock = clock;
    }

    public Mono<ServerResponse> dashManifest(ServerRequest req) {
        return controlPlane.event(req.pathVariable("event"))
                .map(s -> ServerResponse.ok().contentType(DASH).header("Cache-Control", "max-age=60")
                        .header("Access-Control-Allow-Origin", "*").bodyValue(Manifests.dash(s.def())))
                .orElseGet(() -> ServerResponse.notFound().build());
    }

    public Mono<ServerResponse> hlsMaster(ServerRequest req) {
        return controlPlane.event(req.pathVariable("event"))
                .map(s -> ServerResponse.ok().contentType(HLS).header("Cache-Control", "max-age=60")
                        .header("Access-Control-Allow-Origin", "*").bodyValue(Manifests.hlsMaster(s.def())))
                .orElseGet(() -> ServerResponse.notFound().build());
    }

    public Mono<ServerResponse> time(ServerRequest req) {
        return ServerResponse.ok().contentType(MediaType.TEXT_PLAIN).header("Cache-Control", "no-store")
                .header("Access-Control-Allow-Origin", "*").bodyValue(Instant.ofEpochMilli(clock.getAsLong()).toString());
    }

    public Mono<ServerResponse> media(ServerRequest req) {
        String eventId = req.pathVariable("event");
        String renditionId = req.pathVariable("rendition");
        String file = req.pathVariable("file");
        long now = clock.getAsLong();
        req.exchange().getAttributes().put(START_ATTR, System.nanoTime());
        Optional<EventState> state = controlPlane.event(eventId);

        if (file.equals("index.m3u8")) {
            return hlsMedia(state, renditionId, now, req.queryParam("_HLS_msn"));
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
            if (k < 0) {
                return ServerResponse.badRequest().build();
            }
        } else {
            return ServerResponse.notFound().build();
        }

        // 1. Control plane: reject what cannot exist, from memory.
        if (features.isControlPlane()) {
            Verdict verdict = controlPlane.check(eventId, renditionId, k, now);
            if (verdict.rejected()) {
                metrics.increment("cp.reject");
                metrics.increment("cp.reject." + verdict.name().toLowerCase());
                return notFound(rejectMaxAge(verdict, state, k, now));
            }
            metrics.increment("cp.pass");
        }

        // 2. Class and admission.
        RequestClass cls = classify(state, k, now);
        req.exchange().getAttributes().put(CLASS_ATTR, cls);
        AdmissionController.Permit permit = admission.tryAcquire(cls);
        if (permit == null) {
            metrics.increment("status.503");
            return ServerResponse.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .header("Cache-Control", "max-age=" + props.getAdmission().getShedMaxAgeSeconds())
                    .header("Retry-After", Integer.toString(props.getAdmission().getShedMaxAgeSeconds()))
                    .header("Access-Control-Allow-Origin", "*")
                    .build();
        }
        req.exchange().getAttributes().put(PERMIT_ATTR, permit);

        SegmentKey key = new SegmentKey(eventId, renditionId, k);
        Schedule schedule = state.map(EventState::schedule).orElse(null);

        // 3. A hold for this segment is already open: join it without touching storage.
        if (features.isHoldOpen() && waiters != null && waiters.isWaiting(key) && schedule != null) {
            return hold(req, key, schedule, permit, state.get().def());
        }

        // 4. First valid copy: write-through cache, then one coalesced store read.
        return resolver.resolve(key, cls == RequestClass.LIVE_EDGE).flatMap(found -> {
            if (found.isPresent()) {
                return ok(found.get().segment(), found.get().source().name(), state);
            }
            if (schedule != null && k >= 0 && features.isHoldOpen() && waiters != null && holdable(schedule, k, now)) {
                return hold(req, key, schedule, permit, state.get().def());
            }
            return notFound(missMaxAge(schedule, k, now));
        });
    }

    private Mono<ServerResponse> hold(ServerRequest req, SegmentKey key, Schedule schedule,
                                      AdmissionController.Permit permit, EventDef def) {
        // Holding costs no service capacity, so the slot goes back while the request waits.
        admission.release(permit, false);
        req.exchange().getAttributes().remove(PERMIT_ATTR);
        long expected = schedule.expectedPublishMs(key.index());
        long deadline = expected + props.getHold().getGraceMs();
        return waiters.await(key, expected, deadline).flatMap(found -> found.isPresent()
                ? ok(found.get(), "HOLD", Optional.of(new EventState(def, schedule)))
                : notFound(missMaxAge(schedule, key.index(), clock.getAsLong())));
    }

    private boolean holdable(Schedule schedule, long k, long now) {
        long expected = schedule.expectedPublishMs(k);
        return expected - now <= props.getHold().getAheadMs() && now < expected + props.getHold().getGraceMs();
    }

    private RequestClass classify(Optional<EventState> state, long k, long now) {
        if (k == SegmentKey.INIT || state.isEmpty()) {
            return state.isEmpty() ? RequestClass.DVR : RequestClass.LIVE_EDGE;
        }
        long edge = state.get().schedule().liveEdge(now);
        return k >= edge - props.getAdmission().getLiveEdgeSegments() ? RequestClass.LIVE_EDGE : RequestClass.DVR;
    }

    /** Seconds a cache may remember a miss: until just before the segment is expected. */
    private long missMaxAge(Schedule schedule, long k, long now) {
        if (!features.isNegativeCache() || schedule == null || k < 0) {
            return -1;
        }
        long until = schedule.expectedPublishMs(k) - now;
        if (until <= 0) {
            return 1; // overdue: a late pipeline may still deliver it
        }
        return until / 1000;
    }

    private long rejectMaxAge(Verdict verdict, Optional<EventState> state, long k, long now) {
        if (!features.isNegativeCache()) {
            return -1;
        }
        return switch (verdict) {
            case TOO_FAR_AHEAD -> state
                    .map(s -> Math.min(3600, Math.max(0, (s.schedule().expectedPublishMs(k) - props.getHold().getAheadMs() - now) / 1000)))
                    .orElse(60L);
            case OUTSIDE_WINDOW -> 3600;
            default -> 60;
        };
    }

    private Mono<ServerResponse> notFound(long maxAge) {
        metrics.increment("status.404");
        ServerResponse.BodyBuilder b = ServerResponse.status(HttpStatus.NOT_FOUND).header("Access-Control-Allow-Origin", "*");
        if (maxAge >= 0) {
            b.header("Cache-Control", maxAge == 0 ? "no-store" : "max-age=" + maxAge);
        }
        return b.build();
    }

    private Mono<ServerResponse> ok(Segment seg, String source, Optional<EventState> state) {
        metrics.increment("status.200");
        metrics.increment("served." + source.toLowerCase());
        metrics.add("served.bytes", seg.data().length);
        List<String> order = state.map(s -> s.def().pipelineOrder()).orElse(List.of());
        if (!order.isEmpty() && !order.get(0).equals(seg.meta().pipeline())) {
            metrics.increment("served.failover");
        }
        metrics.increment("served.pipeline." + seg.meta().pipeline());
        if (!SegmentValidator.valid(seg.data(), seg.meta(), seg.key().isInit())) {
            metrics.increment("served.defective");
        }
        ServerResponse.BodyBuilder b = ServerResponse.ok()
                .contentType(seg.key().isInit() ? MP4 : SEGMENT_TYPE)
                .header("Cache-Control", "public, max-age=86400, immutable")
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Expose-Headers", "X-Pipeline, X-Published-At, X-Served-From, X-Stream-Events")
                .header("X-Pipeline", seg.meta().pipeline())
                .header("X-Published-At", Long.toString(seg.meta().publishedAtMs()))
                .header("X-Served-From", source);
        if (features.isNotifications() && state.isPresent() && !seg.key().isInit()) {
            String events = cumulativeEvents(state.get().def().notifications(), seg.key().index());
            if (!events.isEmpty()) {
                b.header("X-Stream-Events", events);
            }
        }
        return b.bodyValue(seg.data());
    }

    /**
     * Every notification in effect at segment {@code k}, newest last. Cumulative, so a device that
     * joins late or skips segments still learns about an event it missed.
     */
    static String cumulativeEvents(List<Notification> all, long k) {
        List<Notification> active = all.stream().filter(n -> n.fromSegment() <= k).toList();
        int from = Math.max(0, active.size() - MAX_NOTIFICATIONS_IN_HEADER);
        return active.subList(from, active.size()).stream()
                .map(n -> "id=" + n.id() + ";type=" + n.type() + ";from=" + n.fromSegment()
                        + (n.data().isEmpty() ? "" : ";data=" + n.data().replace(',', ' ').replace(';', ' ')))
                .collect(Collectors.joining(", "));
    }

    /**
     * HLS media playlist computed from the clock. With {@code _HLS_msn=N} (LL-HLS blocking reload)
     * the request is held, like a live-edge segment request, until segment N is published, and the
     * playlist that comes back includes it.
     */
    private Mono<ServerResponse> hlsMedia(Optional<EventState> state, String renditionId, long now,
                                          Optional<String> blockingMsn) {
        if (state.isEmpty() || state.get().def().rendition(renditionId).isEmpty()) {
            return ServerResponse.notFound().build();
        }
        Schedule schedule = state.get().schedule();
        if (blockingMsn.isPresent() && features.isHoldOpen() && waiters != null) {
            long msn;
            try {
                msn = Long.parseLong(blockingMsn.get());
            } catch (NumberFormatException e) {
                return ServerResponse.badRequest().build();
            }
            long edge = schedule.liveEdge(now);
            if (msn > edge + props.getLookAheadSegments()) {
                return ServerResponse.badRequest().build();
            }
            if (msn > edge) {
                SegmentKey key = new SegmentKey(state.get().def().id(), renditionId, msn);
                metrics.increment("hls.blocking_reloads");
                Mono<Optional<Segment>> ready = resolver.peekCache(key).isPresent()
                        ? Mono.just(resolver.peekCache(key))
                        : waiters.await(key, schedule.expectedPublishMs(msn), schedule.expectedPublishMs(msn) + props.getHold().getGraceMs());
                return ready.flatMap(found -> hlsResponse(state.get(), renditionId, clock.getAsLong(),
                        found.isPresent() ? msn : -1));
            }
        }
        return hlsResponse(state.get(), renditionId, now, -1);
    }

    private Mono<ServerResponse> hlsResponse(EventState state, String renditionId, long now, long atLeast) {
        long edge = Math.max(state.schedule().liveEdge(now), atLeast);
        String body = Manifests.hlsMedia(state.def(), state.schedule().epochMs(), edge, 6,
                features.isHoldOpen() && waiters != null);
        long untilNext = state.schedule().expectedPublishMs(edge + 1) - now;
        return ServerResponse.ok().contentType(HLS)
                .header("Cache-Control", "max-age=" + Math.max(0, untilNext / 1000))
                .header("Access-Control-Allow-Origin", "*")
                .bodyValue(body);
    }
}
