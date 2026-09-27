package io.streamorigin.core.model;

/**
 * A stream event (ad break, warning) that takes effect from a segment onwards. Delivered as a
 * cumulative header on every segment at or after {@code fromSegment}.
 */
public record Notification(String id, long fromSegment, String type, String data) {
}
