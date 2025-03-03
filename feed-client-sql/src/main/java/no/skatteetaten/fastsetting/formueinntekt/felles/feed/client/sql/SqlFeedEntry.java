package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.sql;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedEntry;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SqlFeedEntry implements FeedEntry<SqlFeedLocation> {

    private final SqlFeedLocation location;

    private final Map<String, ?> values;

    SqlFeedEntry(SqlFeedLocation location, Map<String, ?> values) {
        this.location = location;
        this.values = values;
    }

    static SqlFeedEntry of(
        List<String> columns,
        SqlFeedEndpoint.SqlValueReader reader,
        SqlFeedLocation.Direction direction,
        ResultSet rs
    ) throws SQLException {
        Object location = reader.apply(rs, 1);
        Map<String, Object> values;
        if (columns.size() == 1) {
            values = Collections.singletonMap(columns.get(0), location);
        } else {
            values = new HashMap<>();
            for (int index = 2; index <= columns.size(); index++) {
                values.put(columns.get(index - 1), reader.apply(rs, index));
            }
        }
        return new SqlFeedEntry(new SqlFeedLocation(
            location.toString(),
            direction
        ), values);
    }

    public Map<String, ?> getValues() {
        return values;
    }

    @Override
    public SqlFeedLocation getLocation() {
        return location;
    }

    @Override
    public String toString() {
        return "entry@" + location;
    }
}
