package io.streamorigin.core.manifest;

import io.streamorigin.core.Fixtures;
import io.streamorigin.core.model.EventDef;
import io.streamorigin.core.schedule.Schedule;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.LongRange;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

final class ManifestsTest {

    private static final long EPOCH = Instant.parse("2026-03-04T05:06:07.890Z").toEpochMilli();
    private static final EventDef EVENT = Fixtures.event("demo", EPOCH, 2000, 150);
    private static final Schedule SCHEDULE = Schedule.of(EVENT);

    @Test
    void dashIsAStaticTemplate() {
        String mpd = Manifests.dash(EVENT);
        assertThat(mpd).contains("type=\"dynamic\"");
        assertThat(mpd).contains("<SegmentTemplate ");
        assertThat(mpd).contains("startNumber=\"0\"");
        assertThat(mpd).contains("media=\"$RepresentationID$/$Number$.m4s\"");
        assertThat(mpd).contains("initialization=\"$RepresentationID$/init.mp4\"");
        assertThat(mpd).contains("duration=\"2000\"").contains("timescale=\"1000\"");
        assertThat(mpd).contains("timeShiftBufferDepth=\"PT300.000S\"");
        assertThat(mpd).contains("<Representation id=\"480p\"");

        Matcher m = Pattern.compile("availabilityStartTime=\"([^\"]+)\"").matcher(mpd);
        assertThat(m.find()).isTrue();
        assertThat(m.group(1)).isEqualTo("2026-03-04T05:06:07.890Z");
        assertThat(Instant.parse(m.group(1)).toEpochMilli()).isEqualTo(EPOCH);
    }

    @Test
    void hlsMasterListsEveryRendition() {
        String master = Manifests.hlsMaster(EVENT);
        assertThat(master).startsWith("#EXTM3U\n");
        assertThat(master).contains("BANDWIDTH=1000000,RESOLUTION=854x480").contains("480p/index.m3u8");
    }

    @Test
    void hlsMediaExample() {
        long now = SCHEDULE.expectedPublishMs(20) + 5;
        String pl = Manifests.hlsMedia(EVENT, SCHEDULE, now, 6);
        assertThat(pl).contains("#EXT-X-MEDIA-SEQUENCE:15\n");
        assertThat(pl).contains("#EXT-X-MAP:URI=\"init.mp4\"");
        assertThat(pl).contains("#EXT-X-TARGETDURATION:2\n");
        assertThat(segments(pl)).containsExactly(15L, 16L, 17L, 18L, 19L, 20L);
        assertThat(pl).contains("#EXT-X-PROGRAM-DATE-TIME:2026-03-04T05:06:37.890Z\n#EXTINF:2.000,\n15.m4s");
    }

    @Test
    void hlsMediaBeforeFirstSegmentIsEmpty() {
        String pl = Manifests.hlsMedia(EVENT, SCHEDULE, EPOCH, 6);
        assertThat(pl).contains("#EXT-X-MEDIA-SEQUENCE:0\n").contains("#EXT-X-MAP:URI=\"init.mp4\"");
        assertThat(segments(pl)).isEmpty();
    }

    @Property(tries = 300)
    void hlsMediaListsLastWindowUpToEdge(@ForAll @LongRange(min = 0, max = 1_000_000) long offset,
                                        @ForAll @IntRange(min = 1, max = 20) int window) {
        long now = EPOCH + offset;
        long edge = SCHEDULE.liveEdge(now);
        String pl = Manifests.hlsMedia(EVENT, SCHEDULE, now, window);
        List<Long> listed = segments(pl);
        long first = Math.max(0, edge - window + 1);
        assertThat(pl).contains("#EXT-X-MEDIA-SEQUENCE:" + first + "\n");
        assertThat(pl).contains("#EXT-X-MAP:URI=\"init.mp4\"");
        assertThat(listed).hasSize((int) Math.min(window, edge + 1));
        for (int i = 0; i < listed.size(); i++) {
            assertThat(listed.get(i)).isEqualTo(first + i);
        }
        if (!listed.isEmpty()) {
            assertThat(listed.get(listed.size() - 1)).isEqualTo(edge);
        }
    }

    private static List<Long> segments(String playlist) {
        return playlist.lines().filter(l -> l.endsWith(".m4s"))
                .map(l -> Long.parseLong(l.substring(0, l.length() - 4))).toList();
    }
}
