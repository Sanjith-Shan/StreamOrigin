package io.streamorigin.core.control;

import io.streamorigin.core.Fixtures;
import io.streamorigin.core.model.EventDef;
import io.streamorigin.core.model.Notification;
import io.streamorigin.core.schedule.Schedule;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.LongRange;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

final class ControlPlaneTest {

    private static final long EPOCH = 1_767_225_600_000L;
    private static final long DUR = 2_000;
    private static final int DVR = 10;
    private static final int LOOK_AHEAD = 2;

    private final ControlPlane cp = ControlPlane.of(List.of(Fixtures.event("demo", EPOCH, DUR, DVR)), LOOK_AHEAD);
    private final Schedule schedule = cp.event("demo").orElseThrow().schedule();
    private final long now = EPOCH + 100 * DUR + 700;
    private final long edge = schedule.liveEdge(now);

    @Test
    void unknownEvent() {
        assertThat(cp.check("nope", "480p", 5, now)).isEqualTo(Verdict.UNKNOWN_EVENT);
    }

    @Test
    void unknownRendition() {
        assertThat(cp.check("demo", "4k", 5, now)).isEqualTo(Verdict.UNKNOWN_RENDITION);
    }

    @Test
    void tooFarAhead() {
        assertThat(edge).isEqualTo(99);
        assertThat(cp.check("demo", "480p", edge + LOOK_AHEAD + 1, now)).isEqualTo(Verdict.TOO_FAR_AHEAD);
        assertThat(cp.check("demo", "480p", edge + LOOK_AHEAD, now)).isEqualTo(Verdict.PLAUSIBLE);
    }

    @Test
    void outsideWindow() {
        assertThat(cp.check("demo", "480p", edge - DVR - 1, now)).isEqualTo(Verdict.OUTSIDE_WINDOW);
        assertThat(cp.check("demo", "480p", edge - DVR, now)).isEqualTo(Verdict.PLAUSIBLE);
        assertThat(cp.check("demo", "480p", -2, now)).isEqualTo(Verdict.OUTSIDE_WINDOW);
    }

    @Test
    void plausibleIncludingInit() {
        assertThat(cp.check("demo", "480p", edge, now)).isEqualTo(Verdict.PLAUSIBLE);
        assertThat(cp.check("demo", "480p", -1, now)).isEqualTo(Verdict.PLAUSIBLE);
        // Init is plausible even before the first segment exists.
        assertThat(cp.check("demo", "480p", -1, EPOCH - 10_000)).isEqualTo(Verdict.PLAUSIBLE);
        assertThat(Verdict.PLAUSIBLE.rejected()).isFalse();
        assertThat(Verdict.TOO_FAR_AHEAD.rejected()).isTrue();
    }

    @Property
    void anyIndexInsideWindowIsPlausible(@ForAll @LongRange(min = 0, max = 10_000_000) long offset,
                                         @ForAll @IntRange(min = 0, max = 500) int dvr,
                                         @ForAll @IntRange(min = 0, max = 10) int lookAhead,
                                         @ForAll @LongRange(min = 0, max = 1_000) long pick) {
        ControlPlane plane = ControlPlane.of(List.of(Fixtures.event("e", EPOCH, DUR, dvr)), lookAhead);
        long t = EPOCH + offset;
        long e = plane.event("e").orElseThrow().schedule().liveEdge(t);
        long lo = Math.max(0, e - dvr);
        long hi = e + lookAhead;
        if (hi < lo) {
            return;
        }
        long k = lo + pick % (hi - lo + 1);
        assertThat(plane.check("e", "480p", k, t)).isEqualTo(Verdict.PLAUSIBLE);
    }

    @Test
    void addNotificationReplacesById() {
        cp.addNotification("demo", new Notification("n1", 5, "ad", "x"));
        cp.addNotification("demo", new Notification("n1", 7, "ad", "y"));
        assertThat(cp.event("demo").orElseThrow().def().notifications())
                .containsExactly(new Notification("n1", 7, "ad", "y"));
    }

    @Test
    void reloadPicksUpChangedFileAndKeepsStateOnBrokenFile(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("events.yaml");
        Files.writeString(file, yaml("one", 2000));
        Files.setLastModifiedTime(file, FileTime.fromMillis(1_000_000));
        try (ControlPlane plane = new ControlPlane(file, 2).start(50)) {
            assertThat(plane.event("one")).isPresent();
            assertThat(plane.event("one").get().def().segmentDurationMs()).isEqualTo(2000);

            Files.writeString(file, yaml("two", 4000));
            Files.setLastModifiedTime(file, FileTime.fromMillis(2_000_000));
            awaitTrue(() -> plane.event("two").isPresent());
            assertThat(plane.event("one")).isEmpty();
            assertThat(plane.event("two").get().schedule().durationMs()).isEqualTo(4000);

            Files.writeString(file, "events: [ {{{ not yaml");
            Files.setLastModifiedTime(file, FileTime.fromMillis(3_000_000));
            Thread.sleep(300);
            assertThat(plane.event("two")).isPresent();

            // A file that parses but lacks pipelines is rejected just the same.
            Files.writeString(file, "events:\n  - id: three\n    epoch: 0\n    renditions: [{id: a}]\n");
            Files.setLastModifiedTime(file, FileTime.fromMillis(4_000_000));
            Thread.sleep(300);
            assertThat(plane.event("two")).isPresent();
            assertThat(plane.event("three")).isEmpty();
        }
    }

    @Test
    void runtimeNotificationsSurviveReload(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("events.yaml");
        Files.writeString(file, yaml("one", 2000));
        Files.setLastModifiedTime(file, FileTime.fromMillis(1_000_000));
        try (ControlPlane plane = new ControlPlane(file, 2).start(50)) {
            plane.addNotification("one", new Notification("ad1", 3, "ad", ""));
            Files.writeString(file, yaml("one", 6000));
            Files.setLastModifiedTime(file, FileTime.fromMillis(2_000_000));
            awaitTrue(() -> plane.event("one").get().def().segmentDurationMs() == 6000);
            assertThat(plane.event("one").get().def().notifications()).extracting(Notification::id).containsExactly("ad1");
        }
    }

    @Test
    void loaderParsesIsoAndEpochMillis() throws Exception {
        String yaml = """
                events:
                  - id: iso
                    epoch: 2026-01-01T00:00:00Z
                    renditions: [{ id: 480p, bandwidth: 1000000, width: 854, height: 480 }]
                    pipelines: [{ id: A, encodeDelayMs: 600 }]
                  - id: millis
                    epoch: 1767225600000
                    segmentDurationMs: 4000
                    dvrWindowSegments: 30
                    renditions: [{ id: 480p }]
                    pipelines: [{ id: A }, { id: B, encodeDelayMs: 900 }]
                    notifications: [{ id: n1, fromSegment: 12, type: ad, data: slot }]
                  - id: quoted
                    epoch: "1767225600000"
                    renditions: [{ id: 480p }]
                    pipelines: [{ id: A }]
                """;
        List<EventDef> defs = EventConfigLoader.parse(yaml);
        assertThat(defs).extracting(EventDef::id).containsExactly("iso", "millis", "quoted");
        long expected = Instant.parse("2026-01-01T00:00:00Z").toEpochMilli();
        assertThat(defs).allSatisfy(d -> assertThat(d.epochMs()).isEqualTo(expected));
        EventDef iso = defs.get(0);
        assertThat(iso.segmentDurationMs()).isEqualTo(2000);
        assertThat(iso.dvrWindowSegments()).isEqualTo(150);
        assertThat(iso.rendition("480p").orElseThrow().bandwidth()).isEqualTo(1_000_000);
        EventDef millis = defs.get(1);
        assertThat(millis.segmentDurationMs()).isEqualTo(4000);
        assertThat(millis.dvrWindowSegments()).isEqualTo(30);
        assertThat(millis.pipelineOrder()).containsExactly("A", "B");
        assertThat(millis.nominalEncodeDelayMs()).isZero();
        assertThat(millis.notifications()).containsExactly(new Notification("n1", 12, "ad", "slot"));
    }

    private static String yaml(String id, long durationMs) {
        return """
                events:
                  - id: %s
                    epoch: 2026-01-01T00:00:00Z
                    segmentDurationMs: %d
                    renditions: [{ id: 480p, bandwidth: 1000000, width: 854, height: 480 }]
                    pipelines: [{ id: A, encodeDelayMs: 600 }]
                """.formatted(id, durationMs);
    }

    private static void awaitTrue(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within 5 s");
            }
            Thread.sleep(20);
        }
    }
}
