package io.streamorigin.core.publish;

import io.streamorigin.core.metrics.OriginMetrics;
import io.streamorigin.core.model.Segment;
import io.streamorigin.core.model.SegmentMeta;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

/** Sends notifications to every edge-server process over HTTP. Failures are counted, never raised. */
public final class HttpEdgeNotifier implements EdgeNotifier {

    private final WebClient client;
    private final List<String> edges;
    private final OriginMetrics metrics;

    public HttpEdgeNotifier(WebClient.Builder builder, List<String> edges, OriginMetrics metrics) {
        this.client = builder.build();
        this.edges = List.copyOf(edges);
        this.metrics = metrics;
    }

    @Override
    public Mono<Void> notify(Segment segment, boolean withBytes) {
        SegmentMeta m = segment.meta();
        return Flux.fromIterable(edges).flatMap(edge -> client.post()
                .uri(edge + "/internal/notify")
                .header("X-Event", segment.key().event())
                .header("X-Rendition", segment.key().rendition())
                .header("X-Index", Long.toString(segment.key().index()))
                .header("X-Pipeline", m.pipeline())
                .header("X-Sequence", Long.toString(m.sequence()))
                .header("X-PTS", Long.toString(m.pts()))
                .header("X-Defect", m.defect() ? "1" : "0")
                .header("X-Published-At", Long.toString(m.publishedAtMs()))
                .header("X-Size", Integer.toString(m.size()))
                .header("X-Chunks", Integer.toString(m.chunks()))
                .header("X-With-Bytes", withBytes ? "1" : "0")
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .bodyValue(withBytes ? segment.data() : new byte[0])
                .retrieve()
                .toBodilessEntity()
                .timeout(Duration.ofSeconds(3))
                .doOnError(e -> metrics.increment("notify.errors"))
                .onErrorResume(e -> Mono.empty())).then();
    }
}
