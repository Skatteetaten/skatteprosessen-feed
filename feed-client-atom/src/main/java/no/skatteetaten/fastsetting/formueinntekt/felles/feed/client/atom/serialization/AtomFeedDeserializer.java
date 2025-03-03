package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom.serialization;

import java.util.Map;
import java.util.function.Function;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom.AtomFeedLocation;

public class AtomFeedDeserializer implements Function<Map<String, String>, AtomFeedLocation> {

    @Override
    public AtomFeedLocation apply(Map<String, String> serialization) {
        return new AtomFeedLocation(serialization.get("PAGE"), serialization.get("ENTRY"), serialization.get("ETAG"));
    }
}
