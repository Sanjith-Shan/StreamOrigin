package io.streamorigin.core.model;

/** A segment's bytes and the metadata of the pipeline copy they came from. */
public record Segment(SegmentKey key, SegmentMeta meta, byte[] data) {
}
