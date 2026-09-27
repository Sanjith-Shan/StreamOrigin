package io.streamorigin.core.control;

/** What the control plane decided about a segment request before any storage was touched. */
public enum Verdict {
    /** In the template's plausible range; go look for it. */
    PLAUSIBLE,
    UNKNOWN_EVENT,
    UNKNOWN_RENDITION,
    /** Beyond the live edge by more than the configured look-ahead. */
    TOO_FAR_AHEAD,
    /** Older than the DVR window, or before segment 0. */
    OUTSIDE_WINDOW;

    public boolean rejected() {
        return this != PLAUSIBLE;
    }
}
