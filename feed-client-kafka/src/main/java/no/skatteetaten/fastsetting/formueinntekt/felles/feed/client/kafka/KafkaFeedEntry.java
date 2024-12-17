package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.kafka;

import java.util.Collections;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedEntry;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaFeedEntry<KEY, VALUE> implements FeedEntry<KafkaLocation> {

    private final ConsumerRecord<KEY, VALUE> record;

    KafkaFeedEntry(ConsumerRecord<KEY, VALUE> record) {
        this.record = record;
    }

    public KEY getKey() {
        return record.key();
    }

    public VALUE getValue() {
        return record.value();
    }

    public ConsumerRecord<KEY, VALUE> getRaw() {
        return record;
    }

    @Override
    public KafkaLocation getLocation() {
        return new KafkaLocation(Collections.singletonMap(record.partition(), record.offset() - 1));
    }

    @Override
    public String toString() {
        return "entry@" + record.partition() + "/" + record.offset();
    }
}
