package io.streamorigin.core.control;

import io.streamorigin.core.model.EventDef;
import io.streamorigin.core.model.Notification;
import io.streamorigin.core.schedule.Schedule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Hierarchical metadata (event, rendition, segment schedule) held in memory and refreshed on a
 * timer. Answers "could this segment exist?" without touching storage.
 */
public final class ControlPlane implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ControlPlane.class);

    public record EventState(EventDef def, Schedule schedule) {
    }

    private final Path file;
    private final int lookAheadSegments;
    private volatile Map<String, EventState> events = Map.of();
    private volatile long loadedMtime = -1;
    private ScheduledExecutorService refresher;

    public ControlPlane(Path file, int lookAheadSegments) {
        this.file = file;
        this.lookAheadSegments = lookAheadSegments;
    }

    /** For tests and single-process use: fixed events, no file. */
    public static ControlPlane of(Collection<EventDef> defs, int lookAheadSegments) {
        ControlPlane cp = new ControlPlane(null, lookAheadSegments);
        cp.install(defs);
        return cp;
    }

    public ControlPlane start(long refreshMillis) {
        reload();
        refresher = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "control-plane-refresh");
            t.setDaemon(true);
            return t;
        });
        refresher.scheduleWithFixedDelay(this::reload, refreshMillis, refreshMillis, TimeUnit.MILLISECONDS);
        return this;
    }

    void reload() {
        if (file == null) {
            return;
        }
        try {
            long mtime = Files.getLastModifiedTime(file).toMillis();
            if (mtime == loadedMtime) {
                return;
            }
            Map<String, EventState> previous = events;
            List<EventDef> defs = new ArrayList<>();
            for (EventDef def : EventConfigLoader.load(file)) {
                // Notifications added at runtime survive a file refresh.
                EventState old = previous.get(def.id());
                if (old != null && def.notifications().isEmpty()) {
                    def = def.withNotifications(old.def().notifications());
                }
                defs.add(def);
            }
            install(defs);
            loadedMtime = mtime;
            log.info("control plane loaded {} event(s) from {}", defs.size(), file);
        } catch (Exception e) {
            log.warn("control plane refresh failed, keeping previous state: {}", e.toString());
        }
    }

    private void install(Collection<EventDef> defs) {
        Map<String, EventState> next = new LinkedHashMap<>();
        for (EventDef def : defs) {
            next.put(def.id(), new EventState(def, Schedule.of(def)));
        }
        events = Map.copyOf(next);
    }

    public Optional<EventState> event(String id) {
        return Optional.ofNullable(events.get(id));
    }

    public Collection<EventState> events() {
        return events.values();
    }

    public int lookAheadSegments() {
        return lookAheadSegments;
    }

    /** Checks a media segment request against the template. Pure, in-memory. */
    public Verdict check(String eventId, String renditionId, long k, long nowMs) {
        EventState state = events.get(eventId);
        if (state == null) {
            return Verdict.UNKNOWN_EVENT;
        }
        if (state.def().rendition(renditionId).isEmpty()) {
            return Verdict.UNKNOWN_RENDITION;
        }
        if (k < 0) {
            return k == -1 ? Verdict.PLAUSIBLE : Verdict.OUTSIDE_WINDOW;
        }
        long edge = state.schedule().liveEdge(nowMs);
        if (k > edge + lookAheadSegments) {
            return Verdict.TOO_FAR_AHEAD;
        }
        if (k < edge - state.def().dvrWindowSegments()) {
            return Verdict.OUTSIDE_WINDOW;
        }
        return Verdict.PLAUSIBLE;
    }

    public synchronized void addNotification(String eventId, Notification n) {
        EventState state = events.get(eventId);
        if (state == null) {
            throw new IllegalArgumentException("unknown event " + eventId);
        }
        List<Notification> updated = new ArrayList<>(state.def().notifications());
        updated.removeIf(x -> x.id().equals(n.id()));
        updated.add(n);
        Map<String, EventState> next = new LinkedHashMap<>(events);
        EventDef def = state.def().withNotifications(updated);
        next.put(eventId, new EventState(def, state.schedule()));
        events = Map.copyOf(next);
    }

    @Override
    public void close() {
        if (refresher != null) {
            refresher.shutdownNow();
        }
    }
}
