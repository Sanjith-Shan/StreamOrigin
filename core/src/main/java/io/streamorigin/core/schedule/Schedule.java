package io.streamorigin.core.schedule;

import io.streamorigin.core.model.EventDef;

/**
 * Template arithmetic. Segment {@code k} covers media time {@code [k*d, (k+1)*d)} after the
 * epoch, so it cannot exist before {@code epoch + (k+1)*d}; a pipeline with encode delay
 * {@code e} is expected to publish it at {@code epoch + (k+1)*d + e}. Nothing here reads a
 * manifest or storage: the origin predicts when a segment should exist from the clock alone.
 */
public final class Schedule {

    private final long epochMs;
    private final long durationMs;
    private final long encodeDelayMs;

    public Schedule(long epochMs, long durationMs, long encodeDelayMs) {
        if (durationMs <= 0) {
            throw new IllegalArgumentException("segment duration must be positive");
        }
        this.epochMs = epochMs;
        this.durationMs = durationMs;
        this.encodeDelayMs = encodeDelayMs;
    }

    public static Schedule of(EventDef event) {
        return new Schedule(event.epochMs(), event.segmentDurationMs(), event.nominalEncodeDelayMs());
    }

    public long epochMs() {
        return epochMs;
    }

    public long durationMs() {
        return durationMs;
    }

    /** Wall-clock time at which segment {@code k}'s media is complete at the encoder. */
    public long segmentEndMs(long k) {
        return epochMs + (k + 1) * durationMs;
    }

    /** Wall-clock time at which segment {@code k} is expected to be published. */
    public long expectedPublishMs(long k) {
        return segmentEndMs(k) + encodeDelayMs;
    }

    /** Highest segment expected to be published by {@code nowMs}, or -1 if none is yet. */
    public long liveEdge(long nowMs) {
        return Math.max(-1, Math.floorDiv(nowMs - epochMs - encodeDelayMs, durationMs) - 1);
    }

    /** Highest segment whose media is complete by {@code nowMs}, ignoring encode delay. */
    public long availableEdge(long nowMs) {
        return Math.max(-1, Math.floorDiv(nowMs - epochMs, durationMs) - 1);
    }

    /** Milliseconds until segment {@code k} is expected; zero or negative if already due. */
    public long millisUntilExpected(long k, long nowMs) {
        return expectedPublishMs(k) - nowMs;
    }
}
