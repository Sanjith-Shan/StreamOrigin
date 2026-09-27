package io.streamorigin.core.serve;

import io.streamorigin.core.metrics.OriginMetrics;
import io.streamorigin.core.model.Segment;

/**
 * Where publish notifications land on the serve side. With bytes, the copy is written through to
 * the cache and held requests are answered from it directly; without bytes, the held requests
 * trigger one store read.
 */
public final class EdgeSink {

    private final SegmentResolver resolver;
    private final LiveEdgeWaiters waiters;
    private final OriginMetrics metrics;

    public EdgeSink(SegmentResolver resolver, LiveEdgeWaiters waiters, OriginMetrics metrics) {
        this.resolver = resolver;
        this.waiters = waiters;
        this.metrics = metrics;
    }

    public void accept(Segment segment, boolean withBytes) {
        metrics.increment("notify.received");
        if (withBytes) {
            if (resolver.offer(segment) && waiters != null) {
                waiters.publish(segment);
            }
        } else if (waiters != null) {
            waiters.wake(segment.key());
        }
    }
}
