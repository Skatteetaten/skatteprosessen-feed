package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.kafka.serialization;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.kafka.KafkaLocation;

public class KafkaDeserializer implements Function<Map<String, String>, KafkaLocation> {

    @Override
    public KafkaLocation apply(Map<String, String> serialization) {
        return new KafkaLocation(serialization.entrySet().stream().collect(Collectors.toMap(
            entry -> Integer.valueOf(entry.getKey()),
            entry -> Long.valueOf(entry.getValue())
        )));
    }
}
