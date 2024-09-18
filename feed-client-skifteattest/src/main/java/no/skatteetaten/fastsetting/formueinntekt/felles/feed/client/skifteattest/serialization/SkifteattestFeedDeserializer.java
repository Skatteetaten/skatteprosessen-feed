package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.skifteattest.serialization;

import java.util.Map;
import java.util.function.Function;

public class SkifteattestFeedDeserializer implements Function<Map<String, String>, Long> {

    @Override
    public Long apply(Map<String, String> serialization) {
        return Long.valueOf(serialization.get("SEQUENCE"));
    }
}
