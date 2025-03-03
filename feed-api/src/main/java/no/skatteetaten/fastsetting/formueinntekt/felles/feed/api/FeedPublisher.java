package no.skatteetaten.fastsetting.formueinntekt.felles.feed.api;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

public interface FeedPublisher<PAYLOAD, TRANSACTION> {

    long INCEPTION = 0;

    default boolean initialize(TRANSACTION transaction) {
        return false;
    }

    @SuppressWarnings("unchecked")
    default void publish(TRANSACTION transaction, PAYLOAD... payloads) {
        publish(transaction, Arrays.asList(payloads));
    }

    void publish(TRANSACTION transaction, Collection<? extends PAYLOAD> payloads);

    default Optional<PAYLOAD> payload(TRANSACTION transaction, long sequence) {
        if (sequence <= INCEPTION) {
            return Optional.empty();
        }
        return page(transaction, sequence - 1, 1, false).stream()
            .findFirst()
            .filter(entry -> entry.getOffset() == sequence)
            .map(Entry::getPayload);
    }

    List<Entry<PAYLOAD>> page(TRANSACTION transaction, long sequence, int size, boolean backwards);

    long limit(TRANSACTION transaction);

    void reset(TRANSACTION transaction);

    class Entry<PAYLOAD> {

        private final long sequence;

        private final PAYLOAD payload;

        public Entry(long sequence, PAYLOAD payload) {
            this.sequence = sequence;
            this.payload = payload;
        }

        public long getOffset() {
            return sequence;
        }

        public PAYLOAD getPayload() {
            return payload;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (object == null || getClass() != object.getClass()) {
                return false;
            }
            Entry<?> published = (Entry<?>) object;
            if (sequence != published.sequence) {
                return false;
            }
            if (payload != null ? !payload.equals(published.payload) : published.payload != null) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int result = (int) (sequence ^ (sequence >>> 32));
            result = 31 * result + (payload != null ? payload.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "feed.publication@" + sequence + ":" + payload;
        }
    }
}
