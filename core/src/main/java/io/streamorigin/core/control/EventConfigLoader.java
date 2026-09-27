package io.streamorigin.core.control;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.streamorigin.core.model.EventDef;
import io.streamorigin.core.model.Notification;
import io.streamorigin.core.model.PipelineDef;
import io.streamorigin.core.model.Rendition;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads event definitions from YAML. The epoch is either an ISO instant or epoch milliseconds;
 * every process serving an event must read the same value, so run scripts write it once.
 */
public final class EventConfigLoader {

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

    private EventConfigLoader() {
    }

    public static List<EventDef> load(Path file) throws IOException {
        return parse(Files.readString(file));
    }

    public static List<EventDef> parse(String yaml) throws IOException {
        JsonNode root = YAML.readTree(yaml);
        List<EventDef> events = new ArrayList<>();
        for (JsonNode e : root.path("events")) {
            List<Rendition> renditions = new ArrayList<>();
            for (JsonNode r : e.path("renditions")) {
                renditions.add(new Rendition(r.path("id").asText(), r.path("bandwidth").asInt(),
                        r.path("width").asInt(), r.path("height").asInt()));
            }
            List<PipelineDef> pipelines = new ArrayList<>();
            for (JsonNode p : e.path("pipelines")) {
                pipelines.add(new PipelineDef(p.path("id").asText(), p.path("encodeDelayMs").asLong(0)));
            }
            List<Notification> notifications = new ArrayList<>();
            for (JsonNode n : e.path("notifications")) {
                notifications.add(new Notification(n.path("id").asText(), n.path("fromSegment").asLong(),
                        n.path("type").asText(), n.path("data").asText("")));
            }
            if (renditions.isEmpty() || pipelines.isEmpty()) {
                throw new IOException("event " + e.path("id").asText() + " needs renditions and pipelines");
            }
            events.add(new EventDef(e.path("id").asText(), parseEpoch(e.path("epoch")),
                    e.path("segmentDurationMs").asLong(2000), e.path("dvrWindowSegments").asInt(150),
                    renditions, pipelines, notifications));
        }
        return events;
    }

    private static long parseEpoch(JsonNode node) {
        if (node.isNumber()) {
            return node.asLong();
        }
        String text = node.asText();
        if (text.chars().allMatch(Character::isDigit) && !text.isEmpty()) {
            return Long.parseLong(text);
        }
        return Instant.parse(text).toEpochMilli();
    }
}
