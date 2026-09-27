package io.streamorigin.core.model;

/** Identifies a segment of a rendition, independent of which pipeline produced it. */
public record SegmentKey(String event, String rendition, long index) {

    /** Index used for the rendition's initialization segment. */
    public static final long INIT = -1;

    public boolean isInit() {
        return index == INIT;
    }

    public String storageId(String pipeline) {
        return "seg:" + event + ":" + rendition + ":" + (isInit() ? "init" : Long.toString(index)) + ":" + pipeline;
    }

    @Override
    public String toString() {
        return event + "/" + rendition + "/" + (isInit() ? "init" : Long.toString(index));
    }
}
