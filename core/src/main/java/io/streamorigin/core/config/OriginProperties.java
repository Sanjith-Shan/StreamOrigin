package io.streamorigin.core.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Every switch the experiments flip. {@code naive: true} overrides all features to off and puts
 * both paths on one store connection, which is the baseline every experiment compares against.
 */
@ConfigurationProperties(prefix = "origin")
public class OriginProperties {

    public enum Role { PUBLISH, EDGE, COMBINED }

    private Role role = Role.COMBINED;
    private String eventsFile = "config/events.yaml";
    private long controlPlaneRefreshMs = 2000;
    private int lookAheadSegments = 2;
    private boolean naive = false;
    private Features features = new Features();
    private Hold hold = new Hold();
    private Store store = new Store();
    private CacheProps cache = new CacheProps();
    private Admission admission = new Admission();
    private List<String> edges = new ArrayList<>(List.of("http://localhost:27081"));
    /**
     * Combined role only: serve the publish path on this port with its own Netty event loops,
     * so publishing keeps its own threads even when both paths share a process. -1 keeps publish
     * on the main port and event loops (the naive layout).
     */
    private int publishPort = -1;
    private int publishLoopThreads = 2;

    public static class Features {
        private boolean controlPlane = true;
        private boolean firstValid = true;
        private boolean holdOpen = true;
        private boolean negativeCache = true;
        private boolean priority = true;
        private boolean writeThrough = true;
        private boolean coalescing = true;
        private boolean notifications = true;

        public boolean isControlPlane() { return controlPlane; }
        public void setControlPlane(boolean v) { controlPlane = v; }
        public boolean isFirstValid() { return firstValid; }
        public void setFirstValid(boolean v) { firstValid = v; }
        public boolean isHoldOpen() { return holdOpen; }
        public void setHoldOpen(boolean v) { holdOpen = v; }
        public boolean isNegativeCache() { return negativeCache; }
        public void setNegativeCache(boolean v) { negativeCache = v; }
        public boolean isPriority() { return priority; }
        public void setPriority(boolean v) { priority = v; }
        public boolean isWriteThrough() { return writeThrough; }
        public void setWriteThrough(boolean v) { writeThrough = v; }
        public boolean isCoalescing() { return coalescing; }
        public void setCoalescing(boolean v) { coalescing = v; }
        public boolean isNotifications() { return notifications; }
        public void setNotifications(boolean v) { notifications = v; }
    }

    public static class Hold {
        /** Hold a request if its segment is due within this many milliseconds. */
        private long aheadMs = 2000;
        /** How long past the expected publish time to keep holding before answering 404. */
        private long graceMs = 1500;
        private long sweepMs = 200;

        public long getAheadMs() { return aheadMs; }
        public void setAheadMs(long v) { aheadMs = v; }
        public long getGraceMs() { return graceMs; }
        public void setGraceMs(long v) { graceMs = v; }
        public long getSweepMs() { return sweepMs; }
        public void setSweepMs(long v) { sweepMs = v; }
    }

    public static class Store {
        /** redis, rocksdb or memory. */
        private String backend = "redis";
        private String writeUri = "redis://localhost:27379";
        private String readUri = "redis://localhost:27380";
        /** One store and one connection for both paths. */
        private boolean shared = false;
        private int chunkBytes = 1 << 20;
        private long ttlSeconds = 600;
        private String rocksdbPath = "data/rocksdb";
        private long readDelayMs = 0;
        private long writeDelayMs = 0;

        public String getBackend() { return backend; }
        public void setBackend(String v) { backend = v; }
        public String getWriteUri() { return writeUri; }
        public void setWriteUri(String v) { writeUri = v; }
        public String getReadUri() { return readUri; }
        public void setReadUri(String v) { readUri = v; }
        public boolean isShared() { return shared; }
        public void setShared(boolean v) { shared = v; }
        public int getChunkBytes() { return chunkBytes; }
        public void setChunkBytes(int v) { chunkBytes = v; }
        public long getTtlSeconds() { return ttlSeconds; }
        public void setTtlSeconds(long v) { ttlSeconds = v; }
        public String getRocksdbPath() { return rocksdbPath; }
        public void setRocksdbPath(String v) { rocksdbPath = v; }
        public long getReadDelayMs() { return readDelayMs; }
        public void setReadDelayMs(long v) { readDelayMs = v; }
        public long getWriteDelayMs() { return writeDelayMs; }
        public void setWriteDelayMs(long v) { writeDelayMs = v; }
    }

    public static class CacheProps {
        private long maxMb = 512;
        /** How many seconds of every rendition to keep. */
        private long seconds = 30;

        public long getMaxMb() { return maxMb; }
        public void setMaxMb(long v) { maxMb = v; }
        public long getSeconds() { return seconds; }
        public void setSeconds(long v) { seconds = v; }
    }

    public static class Admission {
        /** gradient (adaptive) or fixed. */
        private String mode = "gradient";
        private int fixedLimit = 64;
        private int initialLimit = 64;
        private int minLimit = 8;
        private int maxLimit = 1024;
        private double tolerance = 1.5;
        private long windowMs = 100;
        private double dvrShare = 0.5;
        /** Live-edge requests are admitted up to max(adaptive limit, this). */
        private int liveFloor = 512;
        private int liveEdgeSegments = 3;
        private double liveRate = 100_000;
        private double dvrRate = 100_000;
        private int shedMaxAgeSeconds = 5;

        public String getMode() { return mode; }
        public void setMode(String v) { mode = v; }
        public int getFixedLimit() { return fixedLimit; }
        public void setFixedLimit(int v) { fixedLimit = v; }
        public int getInitialLimit() { return initialLimit; }
        public void setInitialLimit(int v) { initialLimit = v; }
        public int getMinLimit() { return minLimit; }
        public void setMinLimit(int v) { minLimit = v; }
        public int getMaxLimit() { return maxLimit; }
        public void setMaxLimit(int v) { maxLimit = v; }
        public double getTolerance() { return tolerance; }
        public void setTolerance(double v) { tolerance = v; }
        public long getWindowMs() { return windowMs; }
        public void setWindowMs(long v) { windowMs = v; }
        public double getDvrShare() { return dvrShare; }
        public void setDvrShare(double v) { dvrShare = v; }
        public int getLiveFloor() { return liveFloor; }
        public void setLiveFloor(int v) { liveFloor = v; }
        public int getLiveEdgeSegments() { return liveEdgeSegments; }
        public void setLiveEdgeSegments(int v) { liveEdgeSegments = v; }
        public double getLiveRate() { return liveRate; }
        public void setLiveRate(double v) { liveRate = v; }
        public double getDvrRate() { return dvrRate; }
        public void setDvrRate(double v) { dvrRate = v; }
        public int getShedMaxAgeSeconds() { return shedMaxAgeSeconds; }
        public void setShedMaxAgeSeconds(int v) { shedMaxAgeSeconds = v; }
    }

    /** Applies {@code naive} on top of whatever features were configured. */
    public Features effectiveFeatures() {
        if (!naive) {
            return features;
        }
        Features off = new Features();
        off.setControlPlane(false);
        off.setFirstValid(false);
        off.setHoldOpen(false);
        off.setNegativeCache(false);
        off.setPriority(false);
        off.setWriteThrough(false);
        off.setCoalescing(false);
        off.setNotifications(features.isNotifications());
        return off;
    }

    public boolean sharedStore() {
        return naive || store.shared || role == Role.COMBINED && store.shared;
    }

    public Role getRole() { return role; }
    public void setRole(Role v) { role = v; }
    public String getEventsFile() { return eventsFile; }
    public void setEventsFile(String v) { eventsFile = v; }
    public long getControlPlaneRefreshMs() { return controlPlaneRefreshMs; }
    public void setControlPlaneRefreshMs(long v) { controlPlaneRefreshMs = v; }
    public int getLookAheadSegments() { return lookAheadSegments; }
    public void setLookAheadSegments(int v) { lookAheadSegments = v; }
    public boolean isNaive() { return naive; }
    public void setNaive(boolean v) { naive = v; }
    public Features getFeatures() { return features; }
    public void setFeatures(Features v) { features = v; }
    public Hold getHold() { return hold; }
    public void setHold(Hold v) { hold = v; }
    public Store getStore() { return store; }
    public void setStore(Store v) { store = v; }
    public CacheProps getCache() { return cache; }
    public void setCache(CacheProps v) { cache = v; }
    public Admission getAdmission() { return admission; }
    public void setAdmission(Admission v) { admission = v; }
    public int getPublishPort() { return publishPort; }
    public void setPublishPort(int v) { publishPort = v; }
    public int getPublishLoopThreads() { return publishLoopThreads; }
    public void setPublishLoopThreads(int v) { publishLoopThreads = v; }
    public List<String> getEdges() { return edges; }
    public void setEdges(List<String> v) { edges = v; }
}
