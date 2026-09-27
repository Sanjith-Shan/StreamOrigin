package io.streamorigin.core.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.streamorigin.core.control.ControlPlane;
import io.streamorigin.core.metrics.OriginMetrics;
import io.streamorigin.core.model.Notification;
import io.streamorigin.core.model.Segment;
import io.streamorigin.core.model.SegmentKey;
import io.streamorigin.core.model.SegmentMeta;
import io.streamorigin.core.serve.AdmissionController;
import io.streamorigin.core.serve.RequestClass;
import io.streamorigin.core.serve.ServeHandler;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.BodyExtractors;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.server.WebFilter;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.springframework.web.reactive.function.server.RequestPredicates.GET;
import static org.springframework.web.reactive.function.server.RequestPredicates.POST;
import static org.springframework.web.reactive.function.server.RequestPredicates.PUT;

@Configuration
@EnableConfigurationProperties(OriginProperties.class)
public class OriginConfiguration {

    @Bean
    OriginMetrics originMetrics(MeterRegistry registry) {
        return new OriginMetrics(registry);
    }

    @Bean(destroyMethod = "close")
    ControlPlane controlPlane(OriginProperties props) {
        return new ControlPlane(Path.of(props.getEventsFile()), props.getLookAheadSegments())
                .start(props.getControlPlaneRefreshMs());
    }

    @Bean(destroyMethod = "close")
    OriginRuntime originRuntime(OriginProperties props, ControlPlane cp, OriginMetrics metrics, WebClient.Builder web) {
        return new OriginRuntime(props, cp, metrics, web, System::currentTimeMillis);
    }

    @Bean
    RouterFunction<ServerResponse> originRoutes(OriginRuntime rt) {
        RouterFunctions.Builder r = RouterFunctions.route()
                .GET("/internal/stats", req -> stats(rt))
                .POST("/internal/stats/reset", req -> {
                    rt.metrics.reset();
                    return ServerResponse.noContent().build();
                })
                .GET("/control/events/{event}", req -> eventInfo(rt, req))
                .POST("/control/events/{event}/notifications", req -> addNotification(rt, req));
        if (rt.publishHandler != null && !separatePublishServer(rt.props)) {
            r.route(PUT("/publish/{pipeline}/{event}/{rendition}/{file}"), rt.publishHandler::put);
        }
        if (rt.serveHandler != null) {
            ServeHandler s = rt.serveHandler;
            r.route(GET("/time"), s::time)
                    .route(GET("/live/{event}/manifest.mpd"), s::dashManifest)
                    .route(GET("/live/{event}/master.m3u8"), s::hlsMaster)
                    .route(GET("/live/{event}/{rendition}/{file}"), s::media)
                    .route(POST("/internal/notify"), req -> notifyEdge(rt, req));
        }
        return r.build();
    }

    static boolean separatePublishServer(OriginProperties props) {
        return props.getRole() == OriginProperties.Role.COMBINED && props.getPublishPort() > 0;
    }

    /**
     * In a combined process, the publish path can run on its own port and its own event loops, the
     * in-process version of separate publish and serve stacks.
     */
    @Bean(destroyMethod = "disposeNow")
    @org.springframework.context.annotation.Conditional(PublishPortCondition.class)
    reactor.netty.DisposableServer publishServer(OriginRuntime rt) {
        RouterFunction<ServerResponse> publish = RouterFunctions.route()
                .route(PUT("/publish/{pipeline}/{event}/{rendition}/{file}"), rt.publishHandler::put)
                .GET("/actuator/health", req -> ServerResponse.ok().bodyValue("{\"status\":\"UP\"}"))
                .build();
        var adapter = new org.springframework.http.server.reactive.ReactorHttpHandlerAdapter(
                RouterFunctions.toHttpHandler(publish));
        return reactor.netty.http.server.HttpServer.create()
                .port(rt.props.getPublishPort())
                .runOn(reactor.netty.resources.LoopResources.create("publish-loop",
                        rt.props.getPublishLoopThreads(), true))
                .handle(adapter)
                .bindNow();
    }

    static final class PublishPortCondition implements org.springframework.context.annotation.Condition {
        @Override
        public boolean matches(org.springframework.context.annotation.ConditionContext ctx,
                               org.springframework.core.type.AnnotatedTypeMetadata md) {
            var env = ctx.getEnvironment();
            String role = env.getProperty("origin.role", "combined");
            int port = env.getProperty("origin.publish-port", Integer.class, -1);
            return "combined".equalsIgnoreCase(role) && port > 0;
        }
    }

    /** Returns the admission slot once the response has been written, and records serve latency. */
    @Bean
    WebFilter admissionReleaseFilter(OriginRuntime rt) {
        return (exchange, chain) -> chain.filter(exchange).doFinally(signal -> {
            AdmissionController.Permit permit = exchange.getAttribute(ServeHandler.PERMIT_ATTR);
            if (permit != null && rt.admission != null) {
                rt.admission.release(permit, signal == SignalType.ON_COMPLETE);
            }
            RequestClass cls = exchange.getAttribute(ServeHandler.CLASS_ATTR);
            Long start = exchange.getAttribute(ServeHandler.START_ATTR);
            if (cls != null && start != null) {
                var status = exchange.getResponse().getStatusCode();
                String outcome = status == null ? "unknown" : Integer.toString(status.value());
                rt.metrics.recordNanosSince("serve." + cls.metricName() + "." + outcome, start);
            }
        });
    }

    private static Mono<ServerResponse> stats(OriginRuntime rt) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("role", rt.props.getRole().name().toLowerCase());
        out.put("naive", rt.props.isNaive());
        out.put("shared_store", rt.props.sharedStore());
        out.put("backend", rt.props.getStore().getBackend());
        OriginProperties.Features f = rt.features;
        Map<String, Boolean> feats = new LinkedHashMap<>();
        feats.put("control_plane", f.isControlPlane());
        feats.put("first_valid", f.isFirstValid());
        feats.put("hold_open", f.isHoldOpen());
        feats.put("negative_cache", f.isNegativeCache());
        feats.put("priority", f.isPriority());
        feats.put("write_through", f.isWriteThrough());
        feats.put("coalescing", f.isCoalescing());
        out.put("features", feats);
        out.putAll(rt.metrics.snapshot());
        return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).bodyValue(out);
    }

    private static Mono<ServerResponse> eventInfo(OriginRuntime rt, ServerRequest req) {
        return rt.controlPlane.event(req.pathVariable("event")).map(s -> {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("id", s.def().id());
            out.put("epochMs", s.def().epochMs());
            out.put("segmentDurationMs", s.def().segmentDurationMs());
            out.put("dvrWindowSegments", s.def().dvrWindowSegments());
            out.put("nominalEncodeDelayMs", s.def().nominalEncodeDelayMs());
            out.put("renditions", s.def().renditions());
            out.put("pipelines", s.def().pipelines());
            out.put("notifications", s.def().notifications());
            out.put("liveEdge", s.schedule().liveEdge(System.currentTimeMillis()));
            return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON)
                    .header("Access-Control-Allow-Origin", "*").bodyValue(out);
        }).orElseGet(() -> ServerResponse.notFound().build());
    }

    private static Mono<ServerResponse> addNotification(OriginRuntime rt, ServerRequest req) {
        String eventId = req.pathVariable("event");
        return req.bodyToMono(Notification.class).flatMap(n -> {
            try {
                rt.controlPlane.addNotification(eventId, n);
                return ServerResponse.accepted().build();
            } catch (IllegalArgumentException e) {
                return ServerResponse.notFound().build();
            }
        });
    }

    private static Mono<ServerResponse> notifyEdge(OriginRuntime rt, ServerRequest req) {
        var h = req.headers();
        SegmentKey key = new SegmentKey(h.firstHeader("X-Event"), h.firstHeader("X-Rendition"),
                Long.parseLong(h.firstHeader("X-Index")));
        SegmentMeta meta = new SegmentMeta(h.firstHeader("X-Pipeline"), Long.parseLong(h.firstHeader("X-Sequence")),
                Long.parseLong(h.firstHeader("X-PTS")), Integer.parseInt(h.firstHeader("X-Size")),
                "1".equals(h.firstHeader("X-Defect")), Long.parseLong(h.firstHeader("X-Published-At")),
                Integer.parseInt(h.firstHeader("X-Chunks")));
        boolean withBytes = "1".equals(h.firstHeader("X-With-Bytes"));
        return DataBufferUtils.join(req.body(BodyExtractors.toDataBuffers()))
                .map(buf -> {
                    byte[] out = new byte[buf.readableByteCount()];
                    buf.read(out);
                    DataBufferUtils.release(buf);
                    return out;
                })
                .defaultIfEmpty(new byte[0])
                .flatMap(bytes -> {
                    rt.edgeSink.accept(new Segment(key, meta, bytes), withBytes && bytes.length == meta.size());
                    return ServerResponse.noContent().build();
                });
    }
}
