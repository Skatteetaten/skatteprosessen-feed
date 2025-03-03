package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom.serialization;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom.AtomFeedLocation;

public class AtomFeedSerializer implements Function<AtomFeedLocation, Map<String, String>> {

    @Override
    public Map<String, String> apply(AtomFeedLocation location) {
        Map<String, String> serialization = new HashMap<>();
        serialization.put("PAGE", location.getPage());
        location.getLastEntry().ifPresent(entry -> serialization.put("ENTRY", entry));
        location.getEtag().ifPresent(etag -> serialization.put("ETAG", etag));
        return serialization;
    }
}
