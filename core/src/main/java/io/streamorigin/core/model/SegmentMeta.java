package io.streamorigin.core.model;

import java.nio.charset.StandardCharsets;

/**
 * What the packager said about a segment when it published it, plus where the bytes live.
 * Stored next to the chunks and written last, so a visible meta implies complete chunks.
 */
public record SegmentMeta(String pipeline, long sequence, long pts, int size, boolean defect,
                          long publishedAtMs, int chunks) {

    public byte[] encode() {
        return String.join("|", pipeline, Long.toString(sequence), Long.toString(pts), Integer.toString(size),
                defect ? "1" : "0", Long.toString(publishedAtMs), Integer.toString(chunks))
                .getBytes(StandardCharsets.UTF_8);
    }

    public static SegmentMeta decode(byte[] raw) {
        String[] p = new String(raw, StandardCharsets.UTF_8).split("\\|");
        if (p.length != 7) {
            throw new IllegalArgumentException("bad segment meta: " + new String(raw, StandardCharsets.UTF_8));
        }
        return new SegmentMeta(p[0], Long.parseLong(p[1]), Long.parseLong(p[2]), Integer.parseInt(p[3]),
                "1".equals(p[4]), Long.parseLong(p[5]), Integer.parseInt(p[6]));
    }
}
