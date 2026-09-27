package io.streamorigin.core.model;

/** One encoding ladder rung. Bandwidth is in bits per second. */
public record Rendition(String id, int bandwidth, int width, int height) {
}
