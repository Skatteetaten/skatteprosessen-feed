package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom.serialization;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom.offset.AtomOffsetFeedLocation;

public class AtomOffsetFeedSerializer implements Function<AtomOffsetFeedLocation, Map<String, String>> {

    @Override
    public Map<String, String> apply(AtomOffsetFeedLocation location) {
        Map<String, String> serialization = new HashMap<>();
        serialization.put("OFFSET", String.valueOf(location.getOffset()));
        serialization.put("DIRECTION", location.getDirection().name());
        return serialization;
    }
}
