package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.sql.serialization;

import java.util.Map;
import java.util.function.Function;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.sql.SqlFeedLocation;

public class SqlFeedDeserializer implements Function<Map<String, String>, SqlFeedLocation> {

    @Override
    public SqlFeedLocation apply(Map<String, String> serialization) {
        return new SqlFeedLocation(
            serialization.get("VALUE"),
            SqlFeedLocation.Direction.valueOf(serialization.get("DIRECTION"))
        );
    }
}
