package io.streamorigin.core.manifest;

import io.streamorigin.core.model.EventDef;
import io.streamorigin.core.model.Rendition;
import io.streamorigin.core.schedule.Schedule;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * Manifests generated from the template alone. The DASH manifest never changes while the event
 * runs: a player computes segment numbers from {@code availabilityStartTime}. HLS has no such
 * template, so its media playlists are computed from the clock on each request, still without
 * reading storage.
 */
public final class Manifests {

    private static final DateTimeFormatter ISO = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
            .withZone(ZoneOffset.UTC);

    private Manifests() {
    }

    public static String dash(EventDef event) {
        double seconds = event.segmentDurationMs() / 1000.0;
        long timescale = 1000;
        StringBuilder sb = new StringBuilder();
        sb.append("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n");
        sb.append("<MPD xmlns=\"urn:mpeg:dash:schema:mpd:2011\" profiles=\"urn:mpeg:dash:profile:isoff-live:2011\"")
                .append(" type=\"dynamic\"")
                .append(" availabilityStartTime=\"").append(ISO.format(Instant.ofEpochMilli(event.epochMs()))).append('"')
                .append(" minimumUpdatePeriod=\"PT500S\"")
                .append(" minBufferTime=\"PT").append(fmt(seconds * 2)).append("S\"")
                .append(" suggestedPresentationDelay=\"PT").append(fmt(seconds * 2)).append("S\"")
                .append(" timeShiftBufferDepth=\"PT").append(fmt(seconds * event.dvrWindowSegments())).append("S\"")
                .append(" maxSegmentDuration=\"PT").append(fmt(seconds)).append("S\">\n");
        sb.append("  <UTCTiming schemeIdUri=\"urn:mpeg:dash:utc:http-iso:2014\" value=\"/time\"/>\n");
        sb.append("  <Period id=\"0\" start=\"PT0S\">\n");
        sb.append("    <AdaptationSet id=\"0\" contentType=\"video\" mimeType=\"video/mp4\" segmentAlignment=\"true\" startWithSAP=\"1\">\n");
        sb.append("      <SegmentTemplate timescale=\"").append(timescale).append('"')
                .append(" duration=\"").append(event.segmentDurationMs()).append('"')
                .append(" startNumber=\"0\"")
                .append(" availabilityTimeOffset=\"0\"")
                .append(" initialization=\"$RepresentationID$/init.mp4\"")
                .append(" media=\"$RepresentationID$/$Number$.m4s\"/>\n");
        for (Rendition r : event.renditions()) {
            sb.append("      <Representation id=\"").append(r.id()).append('"')
                    .append(" codecs=\"avc1.4d4028\"")
                    .append(" bandwidth=\"").append(r.bandwidth()).append('"')
                    .append(" width=\"").append(r.width()).append('"')
                    .append(" height=\"").append(r.height()).append("\"/>\n");
        }
        sb.append("    </AdaptationSet>\n  </Period>\n</MPD>\n");
        return sb.toString();
    }

    public static String hlsMaster(EventDef event) {
        StringBuilder sb = new StringBuilder("#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-INDEPENDENT-SEGMENTS\n");
        for (Rendition r : event.renditions()) {
            sb.append("#EXT-X-STREAM-INF:BANDWIDTH=").append(r.bandwidth())
                    .append(",RESOLUTION=").append(r.width()).append('x').append(r.height())
                    .append(",CODECS=\"avc1.4d4028\"\n")
                    .append(r.id()).append("/index.m3u8\n");
        }
        return sb.toString();
    }

    /** A sliding window of the last {@code window} segments up to the live edge. */
    public static String hlsMedia(EventDef event, Schedule schedule, long nowMs, int window) {
        long edge = schedule.liveEdge(nowMs);
        long first = Math.max(0, edge - window + 1);
        double seconds = event.segmentDurationMs() / 1000.0;
        StringBuilder sb = new StringBuilder("#EXTM3U\n#EXT-X-VERSION:7\n");
        sb.append("#EXT-X-TARGETDURATION:").append((long) Math.ceil(seconds)).append('\n');
        sb.append("#EXT-X-MEDIA-SEQUENCE:").append(first).append('\n');
        sb.append("#EXT-X-MAP:URI=\"init.mp4\"\n");
        for (long k = first; k <= edge; k++) {
            long start = schedule.epochMs() + k * event.segmentDurationMs();
            sb.append("#EXT-X-PROGRAM-DATE-TIME:").append(ISO.format(Instant.ofEpochMilli(start))).append('\n');
            sb.append("#EXTINF:").append(fmt(seconds)).append(",\n").append(k).append(".m4s\n");
        }
        return sb.toString();
    }

    private static String fmt(double v) {
        return String.format(Locale.ROOT, "%.3f", v);
    }
}
