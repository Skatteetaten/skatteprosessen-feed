package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.sql.serialization;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.sql.SqlFeedLocation;

public class SqlFeedSerializationTest {

    @Test
    public void kan_serialize_sql_location() {
        Map<String, String> serialization = new SqlFeedSerializer().apply(new SqlFeedLocation(
            "foo", SqlFeedLocation.Direction.FORWARD
        ));
        assertThat(serialization).containsEntry("VALUE", "foo");
        assertThat(serialization).containsEntry("DIRECTION", "FORWARD");
    }

    @Test
    public void kan_deserialize_sql_location() {
        Map<String, String> serialization = new HashMap<>();
        serialization.put("VALUE", "foo");
        serialization.put("DIRECTION", "FORWARD");
        assertThat(new SqlFeedDeserializer().apply(serialization)).isEqualTo(new SqlFeedLocation(
            "foo", SqlFeedLocation.Direction.FORWARD
        )).extracting(SqlFeedLocation::getDirection).isEqualTo(SqlFeedLocation.Direction.FORWARD);
    }
}
