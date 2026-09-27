package io.streamorigin.core.model;

/**
 * An encoding pipeline. The encode delay is how long after a segment's media ends the pipeline
 * is expected to have published it.
 */
public record PipelineDef(String id, long encodeDelayMs) {
}
