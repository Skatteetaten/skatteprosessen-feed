package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.filesystem.serialization;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.filesystem.FileSystemLocation;
import org.junit.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class FileSystemSerializationTest {

    @Test
    public void kan_serialize_kafka_location() {
        Map<String, String> serialization = new FileSystemSerializer().apply(
            new FileSystemLocation("location", FileSystemLocation.Direction.FORWARD)
        );
        assertThat(serialization).isEqualTo(Map.of("FILE", "location", "DIRECTION", "FORWARD"));
    }

    @Test
    public void kan_deserialize_kafka_location() {
        Map<String, String> serialization = Map.of("FILE", "location", "DIRECTION", "FORWARD");
        assertThat(new FileSystemDeserializer().apply(serialization))
            .isEqualTo(new FileSystemLocation("location", FileSystemLocation.Direction.FORWARD));
    }
}
