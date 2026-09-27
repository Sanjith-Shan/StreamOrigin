package io.streamorigin.core.serve;

import io.streamorigin.core.Fixtures;
import io.streamorigin.core.model.SegmentMeta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

final class SegmentValidatorTest {

    private static boolean media(byte[] data, boolean defect) {
        return SegmentValidator.valid(data, Fixtures.meta("A", data.length, defect), false);
    }

    @Test
    void flaggedDefectIsRejectedEvenWithGoodBytes() {
        byte[] data = Fixtures.media(4096);
        assertThat(media(data, false)).isTrue();
        assertThat(media(data, true)).isFalse();
        assertThat(SegmentValidator.metaValid(Fixtures.meta("A", 4096, true), false)).isFalse();
    }

    @Test
    void tooSmallIsRejected() {
        assertThat(media(Fixtures.media(SegmentValidator.MIN_MEDIA_BYTES - 1), false)).isFalse();
        assertThat(media(Fixtures.media(SegmentValidator.MIN_MEDIA_BYTES), false)).isTrue();
        byte[] init = Fixtures.body("ftyp", SegmentValidator.MIN_INIT_BYTES - 1);
        assertThat(SegmentValidator.valid(init, Fixtures.meta("A", init.length, false), true)).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"styp", "moof", "sidx"})
    void cmafMediaBoxesAreAccepted(String box) {
        assertThat(media(Fixtures.body(box, 2048), false)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"ftyp", "mdat", "free", "\0\0\0\0"})
    void wrongFirstBoxIsRejectedForMedia(String box) {
        assertThat(media(Fixtures.body(box, 2048), false)).isFalse();
    }

    @Test
    void initNeedsFtyp() {
        byte[] ftyp = Fixtures.body("ftyp", 700);
        byte[] styp = Fixtures.body("styp", 700);
        assertThat(SegmentValidator.valid(ftyp, Fixtures.meta("A", 700, false), true)).isTrue();
        assertThat(SegmentValidator.valid(styp, Fixtures.meta("A", 700, false), true)).isFalse();
    }

    @Test
    void sizeMismatchWithMetaIsRejected() {
        byte[] data = Fixtures.media(2048);
        SegmentMeta claims = Fixtures.meta("A", 4096, false);
        assertThat(SegmentValidator.metaValid(claims, false)).isTrue();
        assertThat(SegmentValidator.dataValid(data, claims, false)).isFalse();
        assertThat(SegmentValidator.dataValid(null, claims, false)).isFalse();
    }
}
