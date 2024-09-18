package no.skatteetaten.fastsetting.formueinntekt.felles.feed.memory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedPublisher;

public class InMemoryFeedPublisher<PAYLOAD> implements FeedPublisher<PAYLOAD, Void> {

    private final List<PAYLOAD> payloads;

    public InMemoryFeedPublisher() {
        payloads = new ArrayList<>();
    }

    @Override
    public synchronized void publish(Void ignored, Collection<? extends PAYLOAD> payloads) {
        this.payloads.addAll(payloads);
    }

    @Override
    public List<Entry<PAYLOAD>> page(Void unused, long offset, int size, boolean backwards) {
        if (size < 0) {
            throw new IllegalArgumentException("Size cannot be negative: " + size);
        } else if (size == 0) {
            return Collections.emptyList();
        }
        if (backwards) {
            if (offset <= INCEPTION) {
                return Collections.emptyList();
            } else if (offset > payloads.size()) {
                offset = payloads.size();
            }
            int minimum = Math.max(0, (int) offset - size), maximum = (int) offset;
            return IntStream.range(0, maximum - minimum)
                .map(value -> maximum - value - 1)
                .mapToObj(index -> new Entry<>(index + 1, payloads.get((index))))
                .collect(Collectors.toList());
        } else {
            if (offset < 0) {
                offset = INCEPTION;
            } else if (offset >= payloads.size()) {
                return Collections.emptyList();
            }
            return IntStream.range((int) offset, Math.min(payloads.size(), (int) offset + size))
                .mapToObj(index -> new Entry<>(index + 1, payloads.get((index))))
                .collect(Collectors.toList());
        }
    }

    @Override
    public long limit(Void unused) {
        return payloads.size();
    }

    @Override
    public void reset(Void unused) {
        payloads.clear();
    }
}
