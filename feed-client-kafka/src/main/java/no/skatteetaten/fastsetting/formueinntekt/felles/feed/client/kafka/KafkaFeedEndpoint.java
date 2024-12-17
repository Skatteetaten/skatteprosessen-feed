package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedEndpoint;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;

public class KafkaFeedEndpoint<KEY, VALUE> implements FeedEndpoint<KafkaLocation,
    KafkaFeedEntry<KEY, VALUE>,
    KafkaFeedPage<KEY, VALUE>>, AutoCloseable {

    private final Consumer<KEY, VALUE> consumer;

    private final String topic;

    private final Duration timeout;

    private final Set<Integer> partitions = ConcurrentHashMap.newKeySet();

    private boolean unsubscribed = true;

    public KafkaFeedEndpoint(Consumer<KEY, VALUE> consumer, String topic, Duration timeout) {
        this.consumer = consumer;
        this.topic = topic;
        this.timeout = timeout;
    }

    public static <KEY, VALUE> KafkaFeedEndpoint<KEY, VALUE> of(
        String bootstrap, String topic, Duration timeout,
        Deserializer<KEY> keyDeserializer, Deserializer<VALUE> valueDeserializer
    ) {
        return of(bootstrap, topic, timeout, keyDeserializer, valueDeserializer, Collections.emptyMap());
    }

    public static <KEY, VALUE> KafkaFeedEndpoint<KEY, VALUE> of(
        String bootstrap, String topic, Duration timeout,
        Deserializer<KEY> keyDeserializer, Deserializer<VALUE> valueDeserializer,
        Map<String, Object> configuration
    ) {
        Map<String, Object> properties = new HashMap<>(configuration);
        if (!properties.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, "feed-" + UUID.randomUUID());
        }
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return new KafkaFeedEndpoint<>(
            new KafkaConsumer<>(properties, keyDeserializer, valueDeserializer),
            topic, timeout
        );
    }

    private void initialize() {
        if (unsubscribed) {
            consumer.subscribe(Collections.singleton(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> revoked) {
                    synchronized (KafkaFeedEndpoint.this) {
                        revoked.stream()
                            .filter(partition -> partition.topic().equals(topic))
                            .forEach(partition -> partitions.remove(partition.partition()));
                    }
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> assigned) {
                    synchronized (KafkaFeedEndpoint.this) {
                        assigned.stream()
                            .filter(partition -> partition.topic().equals(topic))
                            .forEach(partition -> partitions.add(partition.partition()));
                    }
                }
            });
            consumer.poll(timeout); // Activates the subscription.
            unsubscribed = false;
        }
    }

    @Override
    public synchronized Optional<KafkaFeedPage<KEY, VALUE>> getFirstPage() {
        initialize();
        List<TopicPartition> partitions = this.partitions.stream()
            .map(partition -> new TopicPartition(topic, partition))
            .collect(Collectors.toList());
        if (partitions.isEmpty()) {
            return Optional.empty();
        } else {
            consumer.seekToBeginning(partitions);
            return Optional.of(new KafkaFeedPage<>(consumer.poll(timeout), Collections.emptyMap()));
        }
    }

    @Override
    public synchronized Optional<KafkaFeedPage<KEY, VALUE>> getLastPage() {
        throw new UnsupportedOperationException("Cannot read last page for Kafka log");
    }

    @Override
    public synchronized Optional<KafkaLocation> getLastLocation() {
        initialize();
        List<TopicPartition> partitions = this.partitions.stream()
            .map(partition -> new TopicPartition(topic, partition))
            .collect(Collectors.toList());
        if (partitions.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(new KafkaLocation(consumer.endOffsets(partitions).entrySet().stream().collect(Collectors.toMap(
                entry -> entry.getKey().partition(),
                Map.Entry::getValue
            ))));
        }
    }

    @Override
    public synchronized Optional<KafkaFeedPage<KEY, VALUE>> getPage(KafkaLocation location) {
        if (location.getOffsets().isEmpty()) {
            throw new IllegalArgumentException("Supplied location does not define any offsets");
        }
        initialize();
        List<TopicPartition> beginning = new ArrayList<>();
        partitions.stream().map(partition -> new TopicPartition(topic, partition)).forEach(partition -> {
            if (location.getOffsets().containsKey(partition.partition())) {
                consumer.seek(partition, location.getOffsets().get(partition.partition()) + 1);
            } else {
                beginning.add(partition);
            }
        });
        if (!beginning.isEmpty()) {
            consumer.seekToBeginning(beginning);
        }
        return Optional.of(new KafkaFeedPage<>(consumer.poll(timeout), location.getOffsets())).filter(KafkaFeedPage::isActive);
    }

    @Override
    public synchronized void close() {
        consumer.close();
    }
}
