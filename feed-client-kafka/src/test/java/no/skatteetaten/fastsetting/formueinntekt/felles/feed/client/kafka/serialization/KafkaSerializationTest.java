package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.kafka.serialization;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.Map;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.kafka.KafkaLocation;
import org.junit.Test;

public class KafkaSerializationTest {

    @Test
    public void kan_serialize_kafka_location() {
        Map<String, String> serialization = new KafkaSerializer().apply(
            new KafkaLocation(Collections.singletonMap(42, 84L))
        );
        assertThat(serialization).containsEntry("42", "84");
    }

    @Test
    public void kan_deserialize_kafka_location() {
        Map<String, String> serialization = Collections.singletonMap("42", "84");
        assertThat(new KafkaDeserializer().apply(serialization))
            .isEqualTo(new KafkaLocation(Collections.singletonMap(42, 84L)));
    }
}
