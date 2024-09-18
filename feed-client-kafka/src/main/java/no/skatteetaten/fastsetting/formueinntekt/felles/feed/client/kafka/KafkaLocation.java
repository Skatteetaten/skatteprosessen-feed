package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.kafka;

import java.util.Map;
import java.util.stream.Collectors;

public class KafkaLocation {

    private final Map<Integer, Long> offsets;

    public KafkaLocation(Map<Integer, Long> offsets) {
        this.offsets = offsets;
    }

    public Map<Integer, Long> getOffsets() {
        return offsets;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        KafkaLocation that = (KafkaLocation) object;
        return offsets.equals(that.offsets);
    }

    @Override
    public int hashCode() {
        return offsets.hashCode();
    }

    @Override
    public String toString() {
        return toString("offset");
    }

    String toString(String infix) {
        return offsets.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .map(entry -> entry.getKey() + ":" + entry.getValue())
            .collect(Collectors.joining("/", "kafka." + infix + "@", ""));
    }
}
