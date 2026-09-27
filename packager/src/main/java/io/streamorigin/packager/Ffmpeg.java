package io.streamorigin.packager;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * The one ffmpeg command line this repo uses: a noisy test pattern (so the encoder has real work
 * and segment sizes track the target bitrate), encoded at three renditions as CMAF fragmented MP4
 * in two-second segments, with a keyframe at every segment boundary.
 */
final class Ffmpeg {

    record Rung(String id, int width, int height, int kbps) {
    }

    static final List<Rung> LADDER = List.of(
            new Rung("1080p", 1920, 1080, 4500),
            new Rung("720p", 1280, 720, 2500),
            new Rung("480p", 854, 480, 1000));

    private Ffmpeg() {
    }

    /**
     * @param seconds    how much media to produce, or 0 to run until killed
     * @param realtime   read the source at its native rate (live mode)
     * @param tsOffsetS  added to every timestamp so segment k carries media time k*2s
     */
    static List<String> command(Path outDir, int seconds, boolean realtime, double tsOffsetS) {
        List<String> cmd = new ArrayList<>(List.of("ffmpeg", "-hide_banner", "-loglevel", "error", "-y"));
        if (realtime) {
            cmd.add("-re");
        }
        cmd.addAll(List.of("-f", "lavfi", "-i", "testsrc2=size=1920x1080:rate=30,noise=alls=12:allf=t"));
        if (seconds > 0) {
            cmd.addAll(List.of("-t", Integer.toString(seconds)));
        }
        StringBuilder filter = new StringBuilder("[0:v]split=" + LADDER.size());
        for (int i = 0; i < LADDER.size(); i++) {
            filter.append("[s").append(i).append(']');
        }
        filter.append(';');
        for (int i = 0; i < LADDER.size(); i++) {
            Rung r = LADDER.get(i);
            filter.append("[s").append(i).append("]scale=").append(r.width()).append(':').append(r.height())
                    .append("[v").append(i).append(']');
            if (i < LADDER.size() - 1) {
                filter.append(';');
            }
        }
        cmd.addAll(List.of("-filter_complex", filter.toString()));
        for (int i = 0; i < LADDER.size(); i++) {
            cmd.addAll(List.of("-map", "[v" + i + "]"));
        }
        cmd.addAll(List.of("-c:v", "libx264", "-preset", "veryfast", "-profile:v", "main", "-pix_fmt", "yuv420p",
                "-g", "60", "-keyint_min", "60", "-sc_threshold", "0", "-x264-params", "nal-hrd=cbr"));
        for (int i = 0; i < LADDER.size(); i++) {
            String k = LADDER.get(i).kbps() + "k";
            cmd.addAll(List.of("-b:v:" + i, k, "-maxrate:v:" + i, k, "-bufsize:v:" + i, k));
        }
        if (tsOffsetS != 0) {
            cmd.addAll(List.of("-output_ts_offset", String.format(Locale.ROOT, "%.3f", tsOffsetS)));
        }
        cmd.addAll(List.of("-f", "dash", "-seg_duration", "2", "-use_template", "1", "-use_timeline", "0",
                "-init_seg_name", "init-$RepresentationID$.mp4",
                "-media_seg_name", "seg-$RepresentationID$-$Number%05d$.m4s"));
        if (realtime) {
            cmd.addAll(List.of("-window_size", "10", "-extra_window_size", "10"));
        }
        cmd.add(outDir.resolve("stream.mpd").toString());
        return cmd;
    }
}
