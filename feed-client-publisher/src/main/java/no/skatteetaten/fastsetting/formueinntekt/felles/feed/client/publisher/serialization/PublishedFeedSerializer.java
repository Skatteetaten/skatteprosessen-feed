package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.publisher.serialization;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.publisher.PublishedFeedLocation;

public class PublishedFeedSerializer implements Function<PublishedFeedLocation, Map<String, String>> {

    @Override
    public Map<String, String> apply(PublishedFeedLocation location) {
        Map<String, String> serialization = new HashMap<>();
        serialization.put("SEQUENCE", String.valueOf(location.getSequence()));
        serialization.put("DIRECTION", location.getDirection().name());
        return serialization;
    }
}
