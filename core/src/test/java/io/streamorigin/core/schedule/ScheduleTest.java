package io.streamorigin.core.schedule;

import io.streamorigin.core.model.EventDef;
import io.streamorigin.core.model.PipelineDef;
import io.streamorigin.core.model.Rendition;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.LongRange;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class ScheduleTest {

    private static final long EPOCH_MIN = 1_600_000_000_000L;
    private static final long EPOCH_MAX = 1_900_000_000_000L;

    @Property
    void liveEdgeBracketsExpectedPublishTimes(@ForAll @LongRange(min = EPOCH_MIN, max = EPOCH_MAX) long epoch,
                                              @ForAll @LongRange(min = 500, max = 10_000) long duration,
                                              @ForAll @LongRange(min = 0, max = 5_000) long delay,
                                              @ForAll @LongRange(min = -100_000, max = 10_000_000) long offset) {
        Schedule s = new Schedule(epoch, duration, delay);
        long t = epoch + offset;
        long k = s.liveEdge(t);
        assertThat(k).isGreaterThanOrEqualTo(-1);
        if (k >= 0) {
            assertThat(s.expectedPublishMs(k)).isLessThanOrEqualTo(t);
            assertThat(t).isLessThan(s.expectedPublishMs(k + 1));
        } else {
            assertThat(t).isLessThan(s.expectedPublishMs(0));
        }
    }

    @Property
    void liveEdgeIsMonotone(@ForAll @LongRange(min = EPOCH_MIN, max = EPOCH_MAX) long epoch,
                            @ForAll @LongRange(min = 500, max = 10_000) long duration,
                            @ForAll @LongRange(min = 0, max = 5_000) long delay,
                            @ForAll @LongRange(min = -100_000, max = 10_000_000) long offset,
                            @ForAll @LongRange(min = 0, max = 100_000) long step) {
        Schedule s = new Schedule(epoch, duration, delay);
        long t = epoch + offset;
        assertThat(s.liveEdge(t + step)).isGreaterThanOrEqualTo(s.liveEdge(t));
    }

    @Property
    void liveEdgeIsMinusOneBeforeFirstPublish(@ForAll @LongRange(min = EPOCH_MIN, max = EPOCH_MAX) long epoch,
                                              @ForAll @LongRange(min = 500, max = 10_000) long duration,
                                              @ForAll @LongRange(min = 0, max = 5_000) long delay,
                                              @ForAll @LongRange(min = 1, max = 1_000_000) long before) {
        Schedule s = new Schedule(epoch, duration, delay);
        long firstPublish = s.expectedPublishMs(0);
        assertThat(s.liveEdge(firstPublish - before)).isEqualTo(-1);
        assertThat(s.liveEdge(firstPublish)).isZero();
    }

    @Property
    void millisUntilExpectedAgreesWithLiveEdge(@ForAll @LongRange(min = EPOCH_MIN, max = EPOCH_MAX) long epoch,
                                               @ForAll @LongRange(min = 500, max = 10_000) long duration,
                                               @ForAll @LongRange(min = 0, max = 5_000) long delay,
                                               @ForAll @LongRange(min = 0, max = 10_000_000) long offset,
                                               @ForAll @LongRange(min = 0, max = 10_000) long k) {
        Schedule s = new Schedule(epoch, duration, delay);
        long t = epoch + offset;
        long until = s.millisUntilExpected(k, t);
        assertThat(until).isEqualTo(s.expectedPublishMs(k) - t);
        // Due (zero or negative) exactly when k is at or behind the live edge.
        assertThat(until <= 0).isEqualTo(k <= s.liveEdge(t));
    }

    @Test
    void exampleTimeline() {
        Schedule s = new Schedule(10_000, 2_000, 600);
        assertThat(s.segmentEndMs(0)).isEqualTo(12_000);
        assertThat(s.expectedPublishMs(0)).isEqualTo(12_600);
        assertThat(s.liveEdge(12_599)).isEqualTo(-1);
        assertThat(s.liveEdge(12_600)).isZero();
        assertThat(s.liveEdge(14_599)).isZero();
        assertThat(s.liveEdge(14_600)).isEqualTo(1);
        assertThat(s.liveEdge(0)).isEqualTo(-1);
        assertThat(s.availableEdge(12_000)).isZero();
        assertThat(s.availableEdge(11_999)).isEqualTo(-1);
        assertThat(s.millisUntilExpected(1, 13_000)).isEqualTo(1_600);
    }

    @Test
    void ofUsesSmallestPipelineDelay() {
        EventDef def = new EventDef("e", 1_000, 4_000, 10, List.of(new Rendition("480p", 1, 1, 1)),
                List.of(new PipelineDef("A", 900), new PipelineDef("B", 300)), List.of());
        Schedule s = Schedule.of(def);
        assertThat(s.epochMs()).isEqualTo(1_000);
        assertThat(s.durationMs()).isEqualTo(4_000);
        assertThat(s.expectedPublishMs(0)).isEqualTo(5_300);
    }

    @Test
    void rejectsNonPositiveDuration() {
        assertThatThrownBy(() -> new Schedule(0, 0, 0)).isInstanceOf(IllegalArgumentException.class);
    }
}
