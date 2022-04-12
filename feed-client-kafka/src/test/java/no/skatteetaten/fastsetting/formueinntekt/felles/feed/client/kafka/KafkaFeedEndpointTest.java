package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Category(KafkaContainer.class)
public class KafkaFeedEndpointTest {

    @Rule
    public final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3")).withEmbeddedZookeeper();

    private KafkaProducer<String, String> producer;
    private KafkaFeedEndpoint<String, String> endpoint;

    @Before
    public void setUp() {
        endpoint = KafkaFeedEndpoint.of(
            kafka.getBootstrapServers(), "foo", Duration.ofSeconds(5),
            new StringDeserializer(), new StringDeserializer()
        );
        producer = new KafkaProducer<>(
            Collections.singletonMap(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()),
            new StringSerializer(), new StringSerializer()
        );
    }

    @After
    public void tearDown() {
        producer.close();
        endpoint.close();
    }

    @Test
    public void can_read_first_page() throws Exception {
        RecordMetadata first = producer.send(new ProducerRecord<>("foo", "bar", "val1")).get();
        RecordMetadata second = producer.send(new ProducerRecord<>("foo", "qux", "val2")).get();
        assertThat(endpoint.getFirstPage()).hasValueSatisfying(page -> {
            assertThat(page.getEntries()).hasSize(2);
            assertThat(page.getEntries().get(0).getKey()).isEqualTo("bar");
            assertThat(page.getEntries().get(0).getValue()).isEqualTo("val1");
            assertThat(page.getEntries().get(0).getLocation().getOffsets()).hasSize(1);
            assertThat(page.getEntries().get(1).getKey()).isEqualTo("qux");
            assertThat(page.getEntries().get(1).getValue()).isEqualTo("val2");
            assertThat(page.getEntries().get(1).getLocation().getOffsets()).hasSize(1);
            assertThat(page.getLocation().getOffsets())
                .isEqualTo(Collections.singletonMap(first.partition(), first.offset() - 1));
            assertThat(page.getNextLocation().map(KafkaLocation::getOffsets))
                .contains(Collections.singletonMap(second.partition(), second.offset()));
        });
        assertThat(endpoint.getFirstPage()).hasValueSatisfying(page -> assertThat(page.getEntries()).hasSize(2));
    }

    @Test
    public void can_read_specific_page() throws Exception {
        RecordMetadata record = producer.send(new ProducerRecord<>("foo", "bar", "val")).get();
        assertThat(endpoint.getPage(new KafkaLocation(Collections.singletonMap(
            record.partition(), record.offset() - 1
        )))).hasValueSatisfying(page -> {
            assertThat(page.getEntries()).hasSize(1);
            assertThat(page.getEntries().get(0).getKey()).isEqualTo("bar");
            assertThat(page.getEntries().get(0).getValue()).isEqualTo("val");
            assertThat(page.getEntries().get(0).getLocation().getOffsets()).hasSize(1);
            assertThat(page.getLocation().getOffsets())
                .isEqualTo(Collections.singletonMap(record.partition(), record.offset() - 1));
            assertThat(page.getNextLocation().map(KafkaLocation::getOffsets))
                .contains(Collections.singletonMap(record.partition(), record.offset()));
        });
    }

    @Test
    public void can_read_empty_page() throws Exception {
        RecordMetadata record = producer.send(new ProducerRecord<>("foo", "bar", "val")).get();
        assertThat(endpoint.getPage(new KafkaLocation(Collections.singletonMap(
            record.partition(), record.offset() + 1
        )))).isEmpty();
    }

    @Test
    public void can_read_last_location() throws Exception {
        RecordMetadata record = producer.send(new ProducerRecord<>("foo", "bar", "val")).get();
        assertThat(endpoint.getLastLocation()).hasValueSatisfying(location -> {
            assertThat(location.getOffsets()).isEqualTo(Collections.singletonMap(record.partition(), record.offset() + 1));
            assertThat(endpoint.getPage(location)).isEmpty();
        });
    }

    @Test
    public void can_read_consecutive_pages() throws Exception {
        RecordMetadata first = producer.send(new ProducerRecord<>("foo", "bar", "val1")).get();
        RecordMetadata second = producer.send(new ProducerRecord<>("foo", "qux", "val2")).get();
        Optional<KafkaFeedPage<String, String>> firstPage = endpoint.getFirstPage();
        assertThat(firstPage).hasValueSatisfying(page -> {
            assertThat(page.getEntries()).hasSize(2);
            assertThat(page.getEntries().get(0).getKey()).isEqualTo("bar");
            assertThat(page.getEntries().get(0).getValue()).isEqualTo("val1");
            assertThat(page.getEntries().get(0).getLocation().getOffsets()).hasSize(1);
            assertThat(page.getEntries().get(1).getKey()).isEqualTo("qux");
            assertThat(page.getEntries().get(1).getValue()).isEqualTo("val2");
            assertThat(page.getEntries().get(1).getLocation().getOffsets()).hasSize(1);
            assertThat(page.getLocation().getOffsets())
                .isEqualTo(Collections.singletonMap(first.partition(), first.offset() - 1));
            assertThat(page.getNextLocation().map(KafkaLocation::getOffsets))
                .contains(Collections.singletonMap(second.partition(), second.offset()));
        });
        assertThat(endpoint.getPage(firstPage.orElseThrow().getLocation())).hasValueSatisfying(page -> {
            assertThat(page.getLocation().getOffsets()).isEqualTo(Collections.singletonMap(0, -1L));
            assertThat(page.getNextLocation()).contains(new KafkaLocation(Collections.singletonMap(0, 1L)));
            assertThat(page.getEntries()).hasSize(2);
            assertThat(page.getEntries().get(0).getKey()).isEqualTo("bar");
            assertThat(page.getEntries().get(0).getValue()).isEqualTo("val1");
            assertThat(page.getEntries().get(0).getLocation().getOffsets()).hasSize(1);
            assertThat(page.getEntries().get(1).getKey()).isEqualTo("qux");
            assertThat(page.getEntries().get(1).getValue()).isEqualTo("val2");
            assertThat(page.getEntries().get(1).getLocation().getOffsets()).hasSize(1);
            assertThat(page.getLocation().getOffsets())
                .isEqualTo(Collections.singletonMap(first.partition(), first.offset() - 1));
            assertThat(page.getNextLocation().map(KafkaLocation::getOffsets))
                .contains(Collections.singletonMap(second.partition(), second.offset()));
        });
        RecordMetadata third = producer.send(new ProducerRecord<>("foo", "baz", "val3")).get();
        assertThat(endpoint.getPage(firstPage.orElseThrow().getNextLocation().orElseThrow())).hasValueSatisfying(page -> {
            assertThat(page.getLocation().getOffsets()).isEqualTo(Collections.singletonMap(0, 1L));
            assertThat(page.getNextLocation()).contains(new KafkaLocation(Collections.singletonMap(0, 2L)));
            assertThat(page.getEntries()).hasSize(1);
            assertThat(page.getEntries().get(0).getKey()).isEqualTo("baz");
            assertThat(page.getEntries().get(0).getValue()).isEqualTo("val3");
            assertThat(page.getEntries().get(0).getLocation().getOffsets()).hasSize(1);
            assertThat(page.getLocation().getOffsets())
                .isEqualTo(Collections.singletonMap(third.partition(), third.offset() - 1));
            assertThat(page.getNextLocation().map(KafkaLocation::getOffsets))
                .contains(Collections.singletonMap(third.partition(), third.offset()));
        });
    }
}
