package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom.serialization;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom.hopper.AtomHopperFeedLocation;

public class AtomHopperFeedSerializer implements Function<AtomHopperFeedLocation, Map<String, String>> {

    @Override
    public Map<String, String> apply(AtomHopperFeedLocation location) {
        Map<String, String> serialization = new HashMap<>();
        serialization.put("MARKER", String.valueOf(location.getMarker()));
        serialization.put("DIRECTION", location.getDirection().name());
        return serialization;
    }
}
