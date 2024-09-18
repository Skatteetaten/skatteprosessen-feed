package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.kafka;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;

public class KafkaFeedPageTest {

    @Test
    public void find_entries_until() {
        List<? extends KafkaFeedEntry<?, ?>> entries = page(42, 84).getEntriesUntil(new KafkaLocation(Collections.singletonMap(0, 42L)));
        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).getLocation()).isEqualTo(new KafkaLocation(Collections.singletonMap(0, 41L)));
    }

    @Test
    public void find_entries_after() {
        List<? extends KafkaFeedEntry<?, ?>> entries = page(42, 84).getEntriesAfter(new KafkaLocation(Collections.singletonMap(0, 42L)));
        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).getLocation()).isEqualTo(new KafkaLocation(Collections.singletonMap(0, 83L)));
    }

    @Test
    public void find_entries_between() {
        List<? extends KafkaFeedEntry<?, ?>> entries = page(42, 84).getEntriesBetween(
            new KafkaLocation(Collections.singletonMap(0, 42L)),
            new KafkaLocation(Collections.singletonMap(0, 84L))
        );
        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).getLocation()).isEqualTo(new KafkaLocation(Collections.singletonMap(0, 83L)));
    }

    @Test
    public void find_entries_until_different_partition() {
        List<? extends KafkaFeedEntry<?, ?>> entries = page(42, 84).getEntriesUntil(new KafkaLocation(Collections.singletonMap(1, 42L)));
        assertThat(entries).isEmpty();
    }

    @Test
    public void find_entries_after_different_partition() {
        List<? extends KafkaFeedEntry<?, ?>> entries = page(42, 84).getEntriesAfter(new KafkaLocation(Collections.singletonMap(1, 42L)));
        assertThat(entries).hasSize(2);
        assertThat(entries.get(0).getLocation()).isEqualTo(new KafkaLocation(Collections.singletonMap(0, 41L)));
        assertThat(entries.get(1).getLocation()).isEqualTo(new KafkaLocation(Collections.singletonMap(0, 83L)));
    }

    @Test
    public void find_entries_between_different_location() {
        List<? extends KafkaFeedEntry<?, ?>> entries = page(42, 84).getEntriesBetween(
            new KafkaLocation(Collections.singletonMap(1, 42L)),
            new KafkaLocation(Collections.singletonMap(1, 84L))
        );
        assertThat(entries).isEmpty();
    }

    private static KafkaFeedPage<?, ?> page(long... offsets) {
        return new KafkaFeedPage<>(new ConsumerRecords<>(Collections.singletonMap(
            new TopicPartition("topic", 0),
            LongStream.of(offsets)
                .mapToObj(offset -> new ConsumerRecord<Void, Void>("topic", 0, offset, null, null))
                .collect(Collectors.toList())
        )), Collections.emptyMap());
    }
}
