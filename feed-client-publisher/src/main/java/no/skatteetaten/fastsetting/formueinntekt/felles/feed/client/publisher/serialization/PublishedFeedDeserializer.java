package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.publisher.serialization;

import java.util.Map;
import java.util.function.Function;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.publisher.PublishedFeedLocation;

public class PublishedFeedDeserializer implements Function<Map<String, String>, PublishedFeedLocation> {

    @Override
    public PublishedFeedLocation apply(Map<String, String> serialization) {
        return new PublishedFeedLocation(
            Long.parseLong(serialization.get("SEQUENCE")),
            PublishedFeedLocation.Direction.valueOf(serialization.get("DIRECTION"))
        );
    }
}
