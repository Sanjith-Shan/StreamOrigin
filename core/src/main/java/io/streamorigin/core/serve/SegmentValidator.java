package io.streamorigin.core.serve;

import io.streamorigin.core.model.SegmentMeta;

import java.nio.charset.StandardCharsets;

/**
 * Decides whether a pipeline's copy is fit to serve. This is not media inspection: it trusts the
 * packager's own defect flag, then checks the size and that the first ISO BMFF box is one a CMAF
 * file can start with.
 */
public final class SegmentValidator {

    static final int MIN_MEDIA_BYTES = 1024;
    static final int MIN_INIT_BYTES = 64;

    private SegmentValidator() {
    }

    public static boolean metaValid(SegmentMeta meta, boolean init) {
        return !meta.defect() && meta.size() >= (init ? MIN_INIT_BYTES : MIN_MEDIA_BYTES);
    }

    public static boolean dataValid(byte[] data, SegmentMeta meta, boolean init) {
        if (data == null || data.length != meta.size() || data.length < 8) {
            return false;
        }
        String box = new String(data, 4, 4, StandardCharsets.ISO_8859_1);
        return init ? box.equals("ftyp") : (box.equals("styp") || box.equals("moof") || box.equals("sidx"));
    }

    public static boolean valid(byte[] data, SegmentMeta meta, boolean init) {
        return metaValid(meta, init) && dataValid(data, meta, init);
    }
}
