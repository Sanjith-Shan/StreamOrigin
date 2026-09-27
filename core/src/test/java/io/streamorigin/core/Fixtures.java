package io.streamorigin.core;

import io.streamorigin.core.model.EventDef;
import io.streamorigin.core.model.PipelineDef;
import io.streamorigin.core.model.Rendition;
import io.streamorigin.core.model.Segment;
import io.streamorigin.core.model.SegmentKey;
import io.streamorigin.core.model.SegmentMeta;

import java.nio.charset.StandardCharsets;
import java.util.List;

/** Shared builders for events and segment bodies. */
public final class Fixtures {

    private Fixtures() {
    }

    public static EventDef event(String id, long epochMs, long durationMs, int dvr) {
        return new EventDef(id, epochMs, durationMs, dvr, List.of(new Rendition("480p", 1_000_000, 854, 480)),
                List.of(new PipelineDef("A", 600), new PipelineDef("B", 600)), List.of());
    }

    /** A body whose first box is {@code box} (4 chars), {@code size} bytes long. */
    public static byte[] body(String box, int size) {
        byte[] b = new byte[size];
        for (int i = 0; i < size; i++) {
            b[i] = (byte) (i * 31 + 7);
        }
        if (size >= 8) {
            byte[] name = box.getBytes(StandardCharsets.ISO_8859_1);
            System.arraycopy(name, 0, b, 4, 4);
        }
        return b;
    }

    public static byte[] media(int size) {
        return body("styp", size);
    }

    public static SegmentMeta meta(String pipeline, int size, boolean defect) {
        return new SegmentMeta(pipeline, 1, 0, size, defect, 1_000, 1);
    }

    public static Segment segment(SegmentKey key, String pipeline, byte[] data, boolean defect) {
        return new Segment(key, meta(pipeline, data.length, defect), data);
    }
}
