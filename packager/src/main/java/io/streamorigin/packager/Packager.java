package io.streamorigin.packager;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * A simulated packager for one pipeline. It publishes each rendition's segment {@code k} by HTTP
 * PUT at the time the template says the pipeline should, with headers for the pipeline's
 * sequence number, PTS and its own defect flag.
 *
 * <p>Commands:
 * <ul>
 *   <li>{@code render}: run ffmpeg once and keep a loop of pre-encoded segments on disk.</li>
 *   <li>{@code publish}: replay the loop on schedule (experiments: no encoder CPU on the laptop).</li>
 *   <li>{@code live}: run ffmpeg in real time and publish each segment as it completes (demo).</li>
 * </ul>
 * Failure injection, all per pipeline: {@code --drop-rate}, {@code --corrupt-rate} (flagged),
 * {@code --silent-corrupt-rate} (garbage body, no flag), {@code --lag-ms}, {@code --fail-after-s}.
 */
public final class Packager {

    private static final ObjectMapper JSON = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    private final Args args;
    private final String origin;
    private final String event;
    private final String pipeline;
    private final long epochMs;
    private final long durationMs;
    private final long encodeDelayMs;
    private final long lagMs;
    private final double dropRate;
    private final double corruptRate;
    private final double silentCorruptRate;
    private final long seed;
    private final HttpClient http;
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();

    private final ConcurrentHistogram clientMicros = new ConcurrentHistogram(60_000_000L, 3);
    private final ConcurrentHistogram serverMicros = new ConcurrentHistogram(60_000_000L, 3);
    private final AtomicLong puts = new AtomicLong();
    private final AtomicLong errors = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();
    private final AtomicLong corrupted = new AtomicLong();
    private final AtomicLong silentCorrupted = new AtomicLong();
    private final AtomicLong late = new AtomicLong();
    private final AtomicLong bytes = new AtomicLong();
    private final AtomicLong over500ms = new AtomicLong();

    Packager(Args args) {
        this.args = args;
        this.origin = args.str("origin", "http://localhost:27080");
        this.event = args.str("event", "demo");
        this.pipeline = args.str("pipeline", "A");
        this.epochMs = args.lng("epoch-ms", System.currentTimeMillis());
        this.durationMs = args.lng("segment-ms", 2000);
        this.encodeDelayMs = args.lng("encode-delay-ms", 600);
        this.lagMs = args.lng("lag-ms", 0);
        this.dropRate = args.dbl("drop-rate", 0);
        this.corruptRate = args.dbl("corrupt-rate", 0);
        this.silentCorruptRate = args.dbl("silent-corrupt-rate", 0);
        this.seed = args.lng("seed", pipeline.hashCode());
        this.http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(2))
                .executor(Executors.newVirtualThreadPerTaskExecutor()) // never shut down: in-flight requests need it
                .build();
    }

    public static void main(String[] argv) throws Exception {
        Args args = new Args(argv);
        switch (args.command) {
            case "render" -> render(args);
            case "publish" -> new Packager(args).replay();
            case "live" -> new Packager(args).live();
            default -> throw new IllegalArgumentException("commands: render, publish, live");
        }
    }

    // ---------------------------------------------------------------- render

    static void render(Args args) throws Exception {
        Path out = Path.of(args.str("out", "data/segments"));
        int seconds = (int) args.lng("seconds", 60);
        Path tmp = Files.createTempDirectory("so-render");
        List<String> cmd = Ffmpeg.command(tmp, seconds, false, 0);
        long t0 = System.nanoTime();
        Process p = new ProcessBuilder(cmd).inheritIO().start();
        if (p.waitFor() != 0) {
            throw new IllegalStateException("ffmpeg failed: " + String.join(" ", cmd));
        }
        Map<String, Object> summary = new LinkedHashMap<>();
        for (int i = 0; i < Ffmpeg.LADDER.size(); i++) {
            String id = Ffmpeg.LADDER.get(i).id();
            Path dir = out.resolve(id);
            Files.createDirectories(dir);
            Files.copy(tmp.resolve("init-" + i + ".mp4"), dir.resolve("init.mp4"), StandardCopyOption.REPLACE_EXISTING);
            List<Long> sizes = new ArrayList<>();
            for (int n = 1; ; n++) {
                Path seg = tmp.resolve(String.format("seg-%d-%05d.m4s", i, n));
                if (!Files.exists(seg)) {
                    break;
                }
                Path dst = dir.resolve(String.format("%05d.m4s", n - 1));
                Files.copy(seg, dst, StandardCopyOption.REPLACE_EXISTING);
                sizes.add(Files.size(dst));
            }
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("init_bytes", Files.size(dir.resolve("init.mp4")));
            r.put("segments", sizes.size());
            r.put("min_bytes", sizes.stream().mapToLong(Long::longValue).min().orElse(0));
            r.put("max_bytes", sizes.stream().mapToLong(Long::longValue).max().orElse(0));
            r.put("mean_bytes", Math.round(sizes.stream().mapToLong(Long::longValue).average().orElse(0)));
            summary.put(id, r);
        }
        summary.put("render_seconds", Math.round((System.nanoTime() - t0) / 1e7) / 100.0);
        Files.writeString(out.resolve("sizes.json"), JSON.writeValueAsString(summary));
        System.out.println(JSON.writeValueAsString(summary));
    }

    // ---------------------------------------------------------------- replay

    record Loop(String rendition, byte[] init, List<byte[]> segments) {
    }

    void replay() throws Exception {
        Path dir = Path.of(args.str("segments-dir", "data/segments"));
        List<Loop> loops = new ArrayList<>();
        for (String r : args.str("renditions", "1080p,720p,480p").split(",")) {
            Path rd = dir.resolve(r);
            List<byte[]> segs = new ArrayList<>();
            try (Stream<Path> files = Files.list(rd)) {
                for (Path f : files.filter(f -> f.toString().endsWith(".m4s")).sorted().toList()) {
                    segs.add(Files.readAllBytes(f));
                }
            }
            if (segs.isEmpty()) {
                throw new IllegalStateException("no segments in " + rd + "; run the render command first");
            }
            loops.add(new Loop(r, Files.readAllBytes(rd.resolve("init.mp4")), segs));
        }
        long runMs = args.lng("duration-s", 60) * 1000;
        long failAfterMs = args.lng("fail-after-s", -1) * 1000;
        long start = System.currentTimeMillis();

        for (Loop l : loops) {
            put(l.rendition(), "init.mp4", l.init(), -1, false).join();
        }
        // First segment whose publish time is still ahead of us.
        long k = Math.max(0, Math.floorDiv(start - epochMs - encodeDelayMs - lagMs, durationMs));
        while (true) {
            long due = epochMs + (k + 1) * durationMs + encodeDelayMs + lagMs;
            long now = System.currentTimeMillis();
            if (due - start > runMs) {
                break;
            }
            if (due > now) {
                Thread.sleep(due - now);
            } else if (now - due > durationMs) {
                late.incrementAndGet();
            }
            if (failAfterMs >= 0 && System.currentTimeMillis() - start >= failAfterMs) {
                dropped.addAndGet(loops.size());
                k++;
                continue;
            }
            publishAll(loops, k);
            k++;
        }
        workers.shutdown();
        workers.awaitTermination(10, TimeUnit.SECONDS);
        report();
    }

    private void publishAll(List<Loop> loops, long k) {
        SplittableRandom rnd = new SplittableRandom(seed * 1_000_003L + k);
        double roll = rnd.nextDouble();
        boolean drop = roll < dropRate;
        boolean corrupt = !drop && roll < dropRate + corruptRate;
        boolean silent = !drop && !corrupt && roll < dropRate + corruptRate + silentCorruptRate;
        for (Loop l : loops) {
            if (drop) {
                dropped.incrementAndGet();
                continue;
            }
            byte[] body = l.segments().get((int) (k % l.segments().size()));
            if (corrupt) {
                corrupted.incrementAndGet();
                body = garbage(body.length / 4, rnd);
            } else if (silent) {
                silentCorrupted.incrementAndGet();
                body = garbage(body.length, rnd);
            }
            put(l.rendition(), k + ".m4s", body, k, corrupt);
        }
    }

    private static byte[] garbage(int n, SplittableRandom rnd) {
        byte[] b = new byte[Math.max(16, n)];
        for (int i = 0; i < b.length; i++) {
            b[i] = (byte) rnd.nextInt(256);
        }
        return b;
    }

    private java.util.concurrent.CompletableFuture<Void> put(String rendition, String file, byte[] body, long k,
                                                               boolean defect) {
        HttpRequest req = HttpRequest.newBuilder(URI.create(origin + "/publish/" + pipeline + "/" + event + "/"
                        + rendition + "/" + file))
                .timeout(Duration.ofSeconds(10))
                .header("X-Sequence", Long.toString(k))
                .header("X-PTS", Long.toString(Math.max(0, k) * durationMs))
                .header("X-Defect", defect ? "1" : "0")
                .header("Content-Type", "application/octet-stream")
                .PUT(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
        long t0 = System.nanoTime();
        return http.sendAsync(req, HttpResponse.BodyHandlers.discarding()).handle((resp, err) -> {
            long micros = (System.nanoTime() - t0) / 1000;
            if (err != null || resp.statusCode() != 201) {
                if (errors.incrementAndGet() == 1) {
                    System.err.println("first publish error: " + (err != null ? err : "HTTP " + resp.statusCode()));
                }
                return null;
            }
            puts.incrementAndGet();
            bytes.addAndGet(body.length);
            clientMicros.recordValue(Math.min(micros, 60_000_000L));
            if (micros > 500_000) {
                over500ms.incrementAndGet();
            }
            resp.headers().firstValue("X-Write-Micros").ifPresent(v ->
                    serverMicros.recordValue(Math.min(Long.parseLong(v), 60_000_000L)));
            return null;
        });
    }

    // ---------------------------------------------------------------- live

    void live() throws Exception {
        long runMs = args.lng("duration-s", 0) * 1000;
        Path tmp = Files.createTempDirectory("so-live-" + pipeline);
        long now = System.currentTimeMillis();
        long k0 = Math.max(0, Math.floorDiv(now - epochMs, durationMs) + 1);
        long startAt = epochMs + k0 * durationMs;
        Thread.sleep(Math.max(0, startAt - System.currentTimeMillis()));
        List<String> cmd = Ffmpeg.command(tmp, 0, true, k0 * durationMs / 1000.0);
        Process ffmpeg = new ProcessBuilder(cmd).redirectErrorStream(true)
                .redirectOutput(tmp.resolve("ffmpeg.log").toFile()).start();
        Runtime.getRuntime().addShutdownHook(new Thread(ffmpeg::destroy));
        System.err.printf("live: pipeline %s started ffmpeg at segment %d, output %s%n", pipeline, k0, tmp);

        int renditions = Ffmpeg.LADDER.size();
        boolean[] initSent = new boolean[renditions];
        long[] next = new long[renditions];
        long[] lastSize = new long[renditions];
        long[] stableSince = new long[renditions];
        java.util.Arrays.fill(next, 1);
        long started = System.currentTimeMillis();
        while (ffmpeg.isAlive() && (runMs == 0 || System.currentTimeMillis() - started < runMs)) {
            for (int i = 0; i < renditions; i++) {
                String id = Ffmpeg.LADDER.get(i).id();
                Path init = tmp.resolve("init-" + i + ".mp4");
                if (!initSent[i] && Files.exists(init) && Files.size(init) > 0) {
                    put(id, "init.mp4", Files.readAllBytes(init), -1, false);
                    initSent[i] = true;
                }
                Path seg = tmp.resolve(String.format("seg-%d-%05d.m4s", i, next[i]));
                Path after = tmp.resolve(String.format("seg-%d-%05d.m4s", i, next[i] + 1));
                if (!Files.exists(seg)) {
                    continue;
                }
                long size = Files.size(seg);
                long t = System.currentTimeMillis();
                if (size != lastSize[i]) {
                    lastSize[i] = size;
                    stableSince[i] = t;
                }
                boolean complete = Files.exists(after) || (size > 0 && t - stableSince[i] >= 150);
                if (complete) {
                    long k = k0 + next[i] - 1;
                    byte[] body = Files.readAllBytes(seg);
                    SplittableRandom rnd = new SplittableRandom(seed * 1_000_003L + k);
                    double roll = rnd.nextDouble();
                    if (roll < dropRate) {
                        dropped.incrementAndGet();
                    } else if (roll < dropRate + corruptRate) {
                        corrupted.incrementAndGet();
                        put(id, k + ".m4s", garbage(body.length / 4, rnd), k, true);
                    } else {
                        put(id, k + ".m4s", body, k, false);
                    }
                    next[i]++;
                    lastSize[i] = 0;
                }
            }
            Thread.sleep(20);
        }
        ffmpeg.destroy();
        report();
    }

    // ---------------------------------------------------------------- report

    private void report() throws IOException {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("pipeline", pipeline);
        out.put("puts", puts.get());
        out.put("errors", errors.get());
        out.put("dropped", dropped.get());
        out.put("corrupted_flagged", corrupted.get());
        out.put("corrupted_silent", silentCorrupted.get());
        out.put("late", late.get());
        out.put("bytes", bytes.get());
        out.put("client_over_500ms", over500ms.get());
        out.put("client_write_us", summarize(clientMicros));
        out.put("server_write_us", summarize(serverMicros));
        String json = JSON.writeValueAsString(out);
        String path = args.str("stats-out", "");
        if (!path.isEmpty()) {
            Files.writeString(Path.of(path), json);
        }
        System.out.println(json);
    }

    static Map<String, Object> summarize(Histogram h) {
        Map<String, Object> s = new LinkedHashMap<>();
        s.put("count", h.getTotalCount());
        if (h.getTotalCount() > 0) {
            s.put("p50", h.getValueAtPercentile(50));
            s.put("p90", h.getValueAtPercentile(90));
            s.put("p99", h.getValueAtPercentile(99));
            s.put("max", h.getMaxValue());
        }
        return s;
    }
}
