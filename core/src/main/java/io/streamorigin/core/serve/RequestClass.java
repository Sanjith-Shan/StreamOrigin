package io.streamorigin.core.serve;

/** Live viewers are served before replay viewers when the origin is short of capacity. */
public enum RequestClass {
    LIVE_EDGE, DVR;

    public String metricName() {
        return this == LIVE_EDGE ? "live" : "dvr";
    }
}
