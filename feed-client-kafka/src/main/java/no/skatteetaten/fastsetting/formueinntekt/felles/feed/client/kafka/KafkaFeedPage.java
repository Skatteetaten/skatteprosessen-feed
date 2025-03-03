package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.kafka;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedPage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class KafkaFeedPage<KEY, VALUE> implements FeedPage<KafkaLocation, KafkaFeedEntry<KEY, VALUE>> {

    private final ConsumerRecords<KEY, VALUE> records;
    private final Map<Integer, Long> offsets;

    KafkaFeedPage(ConsumerRecords<KEY, VALUE> records, Map<Integer, Long> offsets) {
        this.records = records;
        this.offsets = offsets;
    }

    @Override
    public KafkaLocation getLocation() {
        Map<Integer, Long> offsets = new HashMap<>();
        records.forEach(record -> offsets.merge(record.partition(), record.offset() - 1, Math::min));
        return new KafkaLocation(offsets);
    }

    @Override
    public Optional<KafkaLocation> getPreviousLocation() {
        throw new UnsupportedOperationException("Cannot resolve a previous location for Kafka");
    }

    @Override
    public Optional<KafkaLocation> getNextLocation() {
        Map<Integer, Long> offsets = new HashMap<>(this.offsets);
        records.forEach(record -> offsets.merge(record.partition(), record.offset(), Math::max));
        return Optional.of(new KafkaLocation(offsets));
    }

    @Override
    public List<KafkaFeedEntry<KEY, VALUE>> getEntries() {
        return StreamSupport.stream(records.spliterator(), false)
            .sorted(Comparator
                .<ConsumerRecord<?, ?>>comparingInt(ConsumerRecord::partition)
                .thenComparingLong(ConsumerRecord::offset))
            .map(KafkaFeedEntry::new)
            .collect(Collectors.toList());
    }

    @Override
    public boolean hasLocation(KafkaLocation location) {
        return StreamSupport.stream(records.spliterator(), false).anyMatch(record -> {
            Long offset = location.getOffsets().get(record.partition());
            return offset != null && offset == record.offset();
        });
    }

    @Override
    public List<KafkaFeedEntry<KEY, VALUE>> getEntriesAfter(KafkaLocation location) {
        return StreamSupport.stream(records.spliterator(), false).filter(record -> {
            Long offset = location.getOffsets().get(record.partition());
            return offset == null || offset < record.offset();
        }).map(KafkaFeedEntry::new).collect(Collectors.toList());
    }

    @Override
    public List<KafkaFeedEntry<KEY, VALUE>> getEntriesUntil(KafkaLocation location) {
        return StreamSupport.stream(records.spliterator(), false).filter(record -> {
            Long offset = location.getOffsets().get(record.partition());
            return offset != null && offset >= record.offset();
        }).map(KafkaFeedEntry::new).collect(Collectors.toList());
    }

    @Override
    public List<KafkaFeedEntry<KEY, VALUE>> getEntriesBetween(KafkaLocation lower, KafkaLocation upper) {
        return StreamSupport.stream(records.spliterator(), false).filter(record -> {
            Long offset = lower.getOffsets().get(record.partition());
            return offset == null || offset < record.offset();
        }).filter(record -> {
            Long offset = upper.getOffsets().get(record.partition());
            return offset != null && offset >= record.offset();
        }).map(KafkaFeedEntry::new).collect(Collectors.toList());
    }

    boolean isActive() {
        return !records.isEmpty();
    }

    @Override
    public Optional<String> toDisplayString() {
        StringBuilder sb = new StringBuilder();
        StreamSupport.stream(records.spliterator(), false).forEach(record -> sb.append(record.partition())
            .append('/')
            .append(record.offset())
            .append('\n')
            .append(record.key() == null ? "" : record.key().toString())
            .append('\n')
            .append(record.value() == null ? "" : record.value().toString())
            .append('\n')
            .append('\n'));
        return Optional.of(sb.toString());
    }

    @Override
    public String toString() {
        return getLocation().toString("page");
    }
}
