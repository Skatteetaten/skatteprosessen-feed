package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom.serialization;

import java.util.Map;
import java.util.function.Function;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom.hopper.AtomHopperFeedLocation;

public class AtomHopperFeedDeserializer implements Function<Map<String, String>, AtomHopperFeedLocation> {

    @Override
    public AtomHopperFeedLocation apply(Map<String, String> serialization) {
        return new AtomHopperFeedLocation(
            serialization.get("MARKER"),
            AtomHopperFeedLocation.Direction.valueOf(serialization.get("DIRECTION"))
        );
    }
}
