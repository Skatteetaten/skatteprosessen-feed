package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.skifteattest.serialization;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

public class SkifteattestFeedSerializer implements Function<Long, Map<String, String>> {

    @Override
    public Map<String, String> apply(Long location) {
        return Collections.singletonMap("SEQUENCE", location.toString());
    }
}
