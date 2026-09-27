package io.streamorigin.core.publish;

import io.streamorigin.core.model.Segment;
import reactor.core.publisher.Mono;

/** Tells the serve side a copy was published, optionally carrying the bytes (write-through). */
public interface EdgeNotifier {

    Mono<Void> notify(Segment segment, boolean withBytes);
}
