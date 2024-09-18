package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.publisher.serialization;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.publisher.PublishedFeedLocation;
import org.junit.Test;

public class PublishedFeedSerializationTest {

    @Test
    public void kan_serialize_published_location() {
        Map<String, String> serialization = new PublishedFeedSerializer().apply(
            new PublishedFeedLocation(42, PublishedFeedLocation.Direction.FORWARD)
        );
        assertThat(serialization).containsEntry("SEQUENCE", "42");
        assertThat(serialization).containsEntry("DIRECTION", "FORWARD");
    }

    @Test
    public void kan_deserialize_published_location() {
        Map<String, String> serialization = new HashMap<>();
        serialization.put("SEQUENCE", "42");
        serialization.put("DIRECTION", "FORWARD");
        assertThat(new PublishedFeedDeserializer().apply(serialization))
            .isEqualTo(new PublishedFeedLocation(42, PublishedFeedLocation.Direction.FORWARD))
            .extracting(PublishedFeedLocation::getDirection).isEqualTo(PublishedFeedLocation.Direction.FORWARD);
    }
}
