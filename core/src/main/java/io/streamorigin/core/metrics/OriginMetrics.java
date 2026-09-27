package io.streamorigin.core.metrics;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.streamorigin.core.store.StoreListener;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

/**
 * Counters and latency histograms for one process. HdrHistogram holds the exact figures that
 * experiments write to the ledger; the same events are mirrored to Micrometer for Prometheus.
 * All latencies are recorded in microseconds.
 */
public final class OriginMetrics implements StoreListener {

    private static final long MAX_MICROS = TimeUnit.SECONDS.toMicros(120);

    private final MeterRegistry registry;
    private final Map<String, LongAdder> counters = new ConcurrentHashMap<>();
    private final Map<String, Recorder> recorders = new ConcurrentHashMap<>();
    private final Map<String, Histogram> accumulated = new ConcurrentHashMap<>();
    private final Map<String, Timer> timers = new ConcurrentHashMap<>();
    private final Map<String, Supplier<Number>> gauges = new ConcurrentHashMap<>();
    private volatile long resetAtMs = System.currentTimeMillis();
    private volatile long cpuAtReset = processCpuNanos();

    public OriginMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public void increment(String name) {
        add(name, 1);
    }

    public void add(String name, long delta) {
        counters.computeIfAbsent(name, n -> {
            LongAdder adder = new LongAdder();
            if (registry != null) {
                FunctionCounter.builder("origin." + n, adder, LongAdder::doubleValue).register(registry);
            }
            return adder;
        }).add(delta);
    }

    public long count(String name) {
        LongAdder a = counters.get(name);
        return a == null ? 0 : a.sum();
    }

    public void recordMicros(String name, long micros) {
        recorders.computeIfAbsent(name, n -> new Recorder(MAX_MICROS, 3))
                .recordValue(Math.max(0, Math.min(micros, MAX_MICROS)));
        if (registry != null) {
            timers.computeIfAbsent(name, n -> Timer.builder("origin." + n)
                    .publishPercentileHistogram()
                    .minimumExpectedValue(Duration.ofMillis(1))
                    .maximumExpectedValue(Duration.ofSeconds(10))
                    .register(registry)).record(micros, TimeUnit.MICROSECONDS);
        }
    }

    public void recordNanosSince(String name, long startNanos) {
        recordMicros(name, (System.nanoTime() - startNanos) / 1000);
    }

    public void gauge(String name, Supplier<Number> value) {
        gauges.put(name, value);
        if (registry != null) {
            Gauge.builder("origin." + name, () -> value.get().doubleValue()).register(registry);
        }
    }

    @Override
    public void metaRead(int keys) {
        increment("store.meta_reads");
    }

    @Override
    public void dataRead(int chunks, int bytes) {
        increment("store.data_reads");
        add("store.chunks_read", chunks);
        add("store.bytes_read", bytes);
    }

    @Override
    public void write(int chunks, int bytes) {
        increment("store.writes");
        add("store.chunks_written", chunks);
    }

    /** Clears counters and histograms so a measurement window starts from zero. */
    public synchronized void reset() {
        counters.values().forEach(LongAdder::reset);
        recorders.values().forEach(Recorder::reset);
        accumulated.values().forEach(Histogram::reset);
        resetAtMs = System.currentTimeMillis();
        cpuAtReset = processCpuNanos();
    }

    public synchronized Map<String, Object> snapshot() {
        Map<String, Object> out = new LinkedHashMap<>();
        long now = System.currentTimeMillis();
        out.put("window_ms", now - resetAtMs);
        out.put("cpu_ms", (processCpuNanos() - cpuAtReset) / 1_000_000);
        Map<String, Long> c = new TreeMap<>();
        counters.forEach((k, v) -> c.put(k, v.sum()));
        out.put("counters", c);
        Map<String, Object> g = new TreeMap<>();
        gauges.forEach((k, v) -> g.put(k, v.get()));
        out.put("gauges", g);
        Map<String, Object> h = new TreeMap<>();
        recorders.forEach((k, rec) -> {
            Histogram acc = accumulated.computeIfAbsent(k, n -> new Histogram(MAX_MICROS, 3));
            acc.add(rec.getIntervalHistogram());
            h.put(k, summarize(acc));
        });
        out.put("latency_us", h);
        return out;
    }

    public static Map<String, Object> summarize(Histogram hist) {
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

    private static long processCpuNanos() {
        if (ManagementFactory.getOperatingSystemMXBean() instanceof com.sun.management.OperatingSystemMXBean os) {
            return os.getProcessCpuTime();
        }
        return 0;
    }
}
