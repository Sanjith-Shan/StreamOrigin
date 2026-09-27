package io.streamorigin.core.model;

import java.util.List;
import java.util.Optional;

/**
 * A live event: renditions, a shared epoch (wall-clock time of the start of segment 0), a fixed
 * segment duration, and the pipelines producing it in the order the origin prefers them.
 */
public record EventDef(String id, long epochMs, long segmentDurationMs, int dvrWindowSegments,
                       List<Rendition> renditions, List<PipelineDef> pipelines, List<Notification> notifications) {

    public EventDef {
        renditions = List.copyOf(renditions);
        pipelines = List.copyOf(pipelines);
        notifications = notifications == null ? List.of() : List.copyOf(notifications);
    }

    public Optional<Rendition> rendition(String renditionId) {
        return renditions.stream().filter(r -> r.id().equals(renditionId)).findFirst();
    }

    public Optional<PipelineDef> pipeline(String pipelineId) {
        return pipelines.stream().filter(p -> p.id().equals(pipelineId)).findFirst();
    }

    public List<String> pipelineOrder() {
        return pipelines.stream().map(PipelineDef::id).toList();
    }

    /** The earliest any pipeline is expected to publish, which is what the origin plans around. */
    public long nominalEncodeDelayMs() {
        return pipelines.stream().mapToLong(PipelineDef::encodeDelayMs).min().orElse(0);
    }

    public EventDef withNotifications(List<Notification> updated) {
        return new EventDef(id, epochMs, segmentDurationMs, dvrWindowSegments, renditions, pipelines, updated);
    }
}
