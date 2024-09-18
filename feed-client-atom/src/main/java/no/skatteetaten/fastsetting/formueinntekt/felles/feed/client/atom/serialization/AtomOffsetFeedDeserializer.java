package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom.serialization;

import java.util.Map;
import java.util.function.Function;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom.offset.AtomOffsetFeedLocation;

public class AtomOffsetFeedDeserializer implements Function<Map<String, String>, AtomOffsetFeedLocation> {

    @Override
    public AtomOffsetFeedLocation apply(Map<String, String> serialization) {
        return new AtomOffsetFeedLocation(
            Long.parseLong(serialization.get("OFFSET")),
            AtomOffsetFeedLocation.Direction.valueOf(serialization.get("DIRECTION"))
        );
    }
}
