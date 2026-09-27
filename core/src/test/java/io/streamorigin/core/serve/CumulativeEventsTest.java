package io.streamorigin.core.serve;

import io.streamorigin.core.model.Notification;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

final class CumulativeEventsTest {

    private static List<String> ids(String header) {
        if (header.isEmpty()) {
            return List.of();
        }
        return Arrays.stream(header.split(", ")).map(e -> e.substring(3, e.indexOf(';'))).toList();
    }

    @Test
    void onlyNotificationsInEffectAtK() {
        List<Notification> all = List.of(
                new Notification("a", 10, "ad", ""),
                new Notification("b", 20, "warn", "x"),
                new Notification("c", 30, "ad", ""));
        assertThat(ServeHandler.cumulativeEvents(all, 9)).isEmpty();
        assertThat(ids(ServeHandler.cumulativeEvents(all, 10))).containsExactly("a");
        assertThat(ids(ServeHandler.cumulativeEvents(all, 29))).containsExactly("a", "b");
        assertThat(ids(ServeHandler.cumulativeEvents(all, 1000))).containsExactly("a", "b", "c");
    }

    @Test
    void atMostEightNewestLast() {
        List<Notification> all = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            all.add(new Notification("n" + i, i, "ad", ""));
        }
        all.add(new Notification("future", 500, "ad", ""));
        String header = ServeHandler.cumulativeEvents(all, 100);
        assertThat(ids(header)).hasSize(ServeHandler.MAX_NOTIFICATIONS_IN_HEADER)
                .containsExactly("n4", "n5", "n6", "n7", "n8", "n9", "n10", "n11");
    }

    @Test
    void formatsFieldsAndSanitisesData() {
        String header = ServeHandler.cumulativeEvents(List.of(new Notification("x", 3, "ad", "a,b;c")), 3);
        assertThat(header).isEqualTo("id=x;type=ad;from=3;data=a b c");
    }
}
