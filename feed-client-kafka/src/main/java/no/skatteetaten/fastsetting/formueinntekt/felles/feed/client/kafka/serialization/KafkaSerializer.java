package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.kafka.serialization;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.kafka.KafkaLocation;

public class KafkaSerializer implements Function<KafkaLocation, Map<String, String>> {

    @Override
    public Map<String, String> apply(KafkaLocation location) {
        return location.getOffsets().entrySet().stream().collect(Collectors.toMap(
            entry -> entry.getKey().toString(),
            entry -> entry.getValue().toString()
        ));
    }
}
