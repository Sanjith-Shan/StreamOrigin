package io.streamorigin.sim;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A simulated fleet of edge caches in front of the origin.
 *
 * <p><b>Live caches.</b> Each of N caches knows the stream template and asks for segment
 * {@code k} of every rendition as soon as the template says its media is complete
 * ({@code epoch + (k+1)*d}), plus up to {@code --jitter-ms} of random skew. That is the storm:
 * all N ask for the newest segment within the jitter window. A cache honours
 * {@code Cache-Control: max-age} on a 404 or 503 before asking again; without one it retries
 * after {@code --poll-ms}. A segment not delivered within {@code --give-up-ms} is missing.
 *
 * <p><b>DVR readers.</b> Open-loop requests at {@code --dvr-rate} per second for random older
 * segments. No retry: a refusal is counted as shed.
 *
 * <p><b>Junk.</b> Open-loop requests at {@code --junk-rate} per second for segments that cannot
 * exist: far future, unknown rendition, unknown event, and older than the DVR window.
 */
public final class EdgeFleet {

    private static final ObjectMapper JSON = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    private final Args args;
    private final String origin;
    private final String event;
    private final HttpClient http;
    private final ExecutorService vthreads = Executors.newVirtualThreadPerTaskExecutor();

    private long epochMs;
    private long durMs;
    private int dvrWindow;
    private List<String> renditions;
    private String firstPipeline;

    private final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();
    private final Map<String, ConcurrentHistogram> hists = new ConcurrentHashMap<>();
    /** Distinct (cache, rendition, k) delivered, for the gap check. */
    private final AtomicLong expectedDeliveries = new AtomicLong();
    private final java.util.Set<String> missingSegments = ConcurrentHashMap.newKeySet();
    private final java.util.Set<Long> requestedSegments = ConcurrentHashMap.newKeySet();

    EdgeFleet(Args args) {
        this.args = args;
        this.origin = args.str("origin", "http://localhost:27081");
        this.event = args.str("event", "demo");
        this.http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(3))
                .executor(Executors.newVirtualThreadPerTaskExecutor()) // never shut down: in-flight requests need it
                .build();
    }

    public static void main(String[] argv) throws Exception {
        new EdgeFleet(new Args(argv)).run();
    }

    private void count(String name) {
        counters.computeIfAbsent(name, n -> new AtomicLong()).incrementAndGet();
    }

    private void countBy(String name, long delta) {
        counters.computeIfAbsent(name, n -> new AtomicLong()).addAndGet(delta);
    }

    private void record(String name, long micros) {
        hists.computeIfAbsent(name, n -> new ConcurrentHistogram(120_000_000L, 3))
                .recordValue(Math.max(0, Math.min(micros, 120_000_000L)));
    }

    void run() throws Exception {
        JsonNode info = JSON.readTree(http.send(HttpRequest.newBuilder(URI.create(origin + "/control/events/" + event)).build(),
                HttpResponse.BodyHandlers.ofString()).body());
        epochMs = info.path("epochMs").asLong();
        durMs = info.path("segmentDurationMs").asLong();
        dvrWindow = info.path("dvrWindowSegments").asInt();
        renditions = new ArrayList<>();
        info.path("renditions").forEach(r -> renditions.add(r.path("id").asText()));
        String only = args.str("renditions", "all");
        if (!only.equals("all")) {
            renditions = List.of(only.split(","));
        }
        firstPipeline = info.path("pipelines").path(0).path("id").asText("A");

        int caches = (int) args.lng("caches", 10);
        long durationMs = args.lng("duration-s", 30) * 1000;
        long warmupMs = args.lng("warmup-s", 0) * 1000;
        long jitterMs = args.lng("jitter-ms", 100);
        long pollMs = args.lng("poll-ms", 250);
        long giveUpMs = args.lng("give-up-ms", 3 * durMs);
        double dvrRate = args.dbl("dvr-rate", 0);
        double junkRate = args.dbl("junk-rate", 0);
        boolean downloadBody = !args.flag("headers-only");

        long start = System.currentTimeMillis();
        long measureFrom = start + warmupMs;
        long end = measureFrom + durationMs;

        List<Thread> drivers = new ArrayList<>();
        if (caches > 0) {
            drivers.add(Thread.ofVirtual().start(() -> liveDriver(caches, measureFrom, end, jitterMs, pollMs, giveUpMs, downloadBody)));
        }
        if (dvrRate > 0) {
            drivers.add(Thread.ofVirtual().start(() -> openLoop("dvr", dvrRate, measureFrom, end, this::dvrRequest)));
        }
        if (junkRate > 0) {
            drivers.add(Thread.ofVirtual().start(() -> openLoop("junk", junkRate, measureFrom, end, this::junkRequest)));
        }
        for (Thread t : drivers) {
            t.join();
        }
        // Let in-flight fetches finish or give up.
        vthreads.shutdown();
        vthreads.awaitTermination(giveUpMs + 5000, TimeUnit.MILLISECONDS);
        report(caches, durationMs, jitterMs, pollMs, dvrRate, junkRate);
    }

    // ------------------------------------------------------------------ live caches

    private void liveDriver(int caches, long from, long end, long jitterMs, long pollMs, long giveUpMs, boolean body) {
        long k = Math.floorDiv(from - epochMs, durMs); // first segment completing after `from`
        while (true) {
            long availableAt = epochMs + (k + 1) * durMs;
            if (availableAt >= end) {
                break;
            }
            sleepUntil(availableAt);
            final long seg = k;
            requestedSegments.add(k);
            for (int c = 0; c < caches; c++) {
                for (String r : renditions) {
                    long skew = jitterMs > 0 ? ThreadLocalRandom.current().nextLong(jitterMs + 1) : 0;
                    expectedDeliveries.incrementAndGet();
                    vthreads.submit(() -> fetchLive(r, seg, availableAt + skew, availableAt + giveUpMs, pollMs, body));
                }
            }
            k++;
        }
    }

    private void fetchLive(String rendition, long k, long firstAttemptAt, long giveUpAt, long pollMs, boolean body) {
        sleepUntil(firstAttemptAt);
        long firstTry = System.currentTimeMillis();
        int attempts = 0;
        while (System.currentTimeMillis() < giveUpAt) {
            attempts++;
            count("live.requests");
            long t0 = System.nanoTime();
            try {
                HttpResponse<InputStream> resp = http.send(
                        HttpRequest.newBuilder(URI.create(origin + "/live/" + event + "/" + rendition + "/" + k + ".m4s"))
                                .timeout(Duration.ofMillis(Math.max(1000, giveUpAt - System.currentTimeMillis() + 1000)))
                                .GET().build(),
                        HttpResponse.BodyHandlers.ofInputStream());
                long headersAt = System.currentTimeMillis();
                int status = resp.statusCode();
                count("live.status." + status);
                if (status == 200) {
                    Body data = consume(resp.body());
                    long doneNanos = System.nanoTime();
                    record("live.request_us", (doneNanos - t0) / 1000);
                    long publishedAt = resp.headers().firstValue("X-Published-At").map(Long::parseLong).orElse(-1L);
                    if (publishedAt > 0) {
                        record("live.publish_to_first_byte_us", Math.max(0, headersAt - publishedAt) * 1000);
                    }
                    record("live.available_to_delivered_us", (System.currentTimeMillis() - firstTry) * 1000);
                    String pipeline = resp.headers().firstValue("X-Pipeline").orElse("?");
                    count("live.delivered");
                    count("live.delivered.pipeline." + pipeline);
                    if (!pipeline.equals(firstPipeline)) {
                        count("live.delivered.failover");
                    }
                    resp.headers().firstValue("X-Served-From").ifPresent(s -> count("live.served_from." + s.toLowerCase()));
                    countBy("live.bytes", data.length());
                    if (body && !data.looksLikeSegment()) {
                        count("live.delivered_corrupt");
                    }
                    if (resp.headers().firstValue("X-Stream-Events").isPresent()) {
                        count("live.with_stream_events");
                    }
                    record("live.attempts", attempts);
                    return;
                }
                drain(resp.body());
                long wait = maxAge(resp).map(s -> Math.max(s * 1000, pollMs)).orElse(pollMs);
                if (status == 404 && maxAge(resp).isPresent()) {
                    count("live.404_with_max_age");
                }
                long next = Math.min(headersAt + wait, giveUpAt);
                if (next >= giveUpAt) {
                    break;
                }
                sleepUntil(next);
            } catch (Exception e) {
                count("live.errors");
                if (counters.get("live.errors").get() == 1) {
                    System.err.println("first live error: " + e);
                    e.printStackTrace();
                }
                sleepUntil(Math.min(System.currentTimeMillis() + pollMs, giveUpAt));
            }
        }
        count("live.missing");
        missingSegments.add(rendition + "/" + k);
    }

    // ------------------------------------------------------------------ open loop

    private void openLoop(String name, double rate, long from, long end, java.util.function.Consumer<String> task) {
        sleepUntil(from);
        long intervalNanos = (long) (1e9 / rate);
        long next = System.nanoTime();
        while (System.currentTimeMillis() < end) {
            next += intervalNanos;
            long wait = next - System.nanoTime();
            if (wait > 0) {
                try {
                    TimeUnit.NANOSECONDS.sleep(wait);
                } catch (InterruptedException e) {
                    return;
                }
            }
            vthreads.submit(() -> task.accept(name));
        }
    }

    private void dvrRequest(String name) {
        long edge = Math.floorDiv(System.currentTimeMillis() - epochMs, durMs) - 2;
        long lo = Math.max(0, edge - dvrWindow + 10);
        long hi = Math.max(lo + 1, edge - 8);
        long k = ThreadLocalRandom.current().nextLong(lo, hi);
        String r = renditions.get(ThreadLocalRandom.current().nextInt(renditions.size()));
        oneShot(name, "/live/" + event + "/" + r + "/" + k + ".m4s");
    }

    private void junkRequest(String name) {
        long edge = Math.floorDiv(System.currentTimeMillis() - epochMs, durMs);
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        String r = renditions.get(rnd.nextInt(renditions.size()));
        String path = switch (rnd.nextInt(4)) {
            case 0 -> "/live/" + event + "/" + r + "/" + (edge + 50 + rnd.nextInt(100_000)) + ".m4s";
            case 1 -> "/live/" + event + "/4k/" + Math.max(0, edge - rnd.nextInt(20)) + ".m4s";
            case 2 -> "/live/no-such-event-" + rnd.nextInt(1000) + "/" + r + "/" + edge + ".m4s";
            default -> "/live/" + event + "/" + r + "/" + Math.max(0, edge - dvrWindow - 50 - rnd.nextInt(1000)) + ".m4s";
        };
        oneShot(name, path);
    }

    private void oneShot(String name, String path) {
        count(name + ".requests");
        long t0 = System.nanoTime();
        try {
            HttpResponse<InputStream> resp = http.send(HttpRequest.newBuilder(URI.create(origin + path))
                    .timeout(Duration.ofSeconds(10)).GET().build(), HttpResponse.BodyHandlers.ofInputStream());
            long n = consume(resp.body()).length();
            int status = resp.statusCode();
            count(name + ".status." + status);
            countBy(name + ".bytes", n);
            record(name + ".request_us." + status, (System.nanoTime() - t0) / 1000);
            record(name + ".request_us", (System.nanoTime() - t0) / 1000);
        } catch (Exception e) {
            count(name + ".errors");
        }
    }

    // ------------------------------------------------------------------ helpers

    /** A response body reduced to what the fleet checks: its length and its first box header. */
    record Body(long length, byte[] head) {
        boolean looksLikeSegment() {
            if (length < 1024 || head.length < 8) {
                return false;
            }
            String box = new String(head, 4, 4, StandardCharsets.ISO_8859_1);
            return box.equals("styp") || box.equals("moof") || box.equals("sidx");
        }
    }

    /** Streams the body without holding it in memory, keeping only the first 8 bytes. */
    static Body consume(InputStream in) {
        try (in) {
            byte[] head = in.readNBytes(8);
            long rest = in.transferTo(java.io.OutputStream.nullOutputStream());
            return new Body(head.length + rest, head);
        } catch (Exception e) {
            return new Body(0, new byte[0]);
        }
    }

    private static Optional<Long> maxAge(HttpResponse<?> resp) {
        return resp.headers().firstValue("Cache-Control").flatMap(cc -> {
            for (String part : cc.split(",")) {
                String p = part.trim();
                if (p.startsWith("max-age=")) {
                    return Optional.of(Long.parseLong(p.substring(8)));
                }
            }
            return Optional.empty();
        });
    }

    private static byte[] drain(InputStream in) {
        try (in) {
            return in.readAllBytes();
        } catch (Exception e) {
            return new byte[0];
        }
    }

    private static void sleepUntil(long wallMs) {
        long wait = wallMs - System.currentTimeMillis();
        if (wait > 0) {
            try {
                Thread.sleep(wait);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void report(int caches, long durationMs, long jitterMs, long pollMs, double dvrRate, double junkRate)
            throws Exception {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("label", args.str("label", ""));
        out.put("caches", caches);
        out.put("renditions", renditions);
        out.put("duration_s", durationMs / 1000);
        out.put("jitter_ms", jitterMs);
        out.put("poll_ms", pollMs);
        out.put("dvr_rate", dvrRate);
        out.put("junk_rate", junkRate);
        out.put("expected_live_deliveries", expectedDeliveries.get());
        out.put("first_segment", requestedSegments.stream().mapToLong(Long::longValue).min().orElse(-1));
        out.put("last_segment", requestedSegments.stream().mapToLong(Long::longValue).max().orElse(-1));
        out.put("missing_segments", new java.util.TreeSet<>(missingSegments).stream().limit(50).toList());
        Map<String, Long> c = new java.util.TreeMap<>();
        counters.forEach((k, v) -> c.put(k, v.get()));
        out.put("counters", c);
        Map<String, Object> h = new java.util.TreeMap<>();
        hists.forEach((k, v) -> h.put(k, summarize(v)));
        out.put("latency", h);
        String json = JSON.writeValueAsString(out);
        String path = args.str("out", "");
        if (!path.isEmpty()) {
            Files.writeString(Path.of(path), json);
        }
        System.out.println(json);
    }

    static Map<String, Object> summarize(Histogram hist) {
        Map<String, Object> s = new LinkedHashMap<>();
        s.put("count", hist.getTotalCount());
        if (hist.getTotalCount() > 0) {
            s.put("p50", hist.getValueAtPercentile(50));
            s.put("p90", hist.getValueAtPercentile(90));
            s.put("p99", hist.getValueAtPercentile(99));
            s.put("p999", hist.getValueAtPercentile(99.9));
            s.put("max", hist.getMaxValue());
            s.put("mean", Math.round(hist.getMean()));
        }
        return s;
    }
}
