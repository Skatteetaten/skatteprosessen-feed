package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.sql.serialization;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.sql.SqlFeedLocation;

public class SqlFeedSerializer implements Function<SqlFeedLocation, Map<String, String>> {

    @Override
    public Map<String, String> apply(SqlFeedLocation location) {
        Map<String, String> serialization = new HashMap<>();
        serialization.put("VALUE", String.valueOf(location.getValue()));
        serialization.put("DIRECTION", location.getDirection().name());
        return serialization;
    }
}

