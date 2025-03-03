package no.skatteetaten.fastsetting.formueinntekt.felles.feed.memory;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedEndpoint;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedPage;

public class InMemoryFeedEndpoint<PAYLOAD> implements FeedEndpoint<Integer,
    InMemoryFeedEntry<PAYLOAD>,
    FeedPage<Integer, InMemoryFeedEntry<PAYLOAD>>> {

    private final int pageSize;
    private final List<PAYLOAD> entries = new CopyOnWriteArrayList<>();

    public InMemoryFeedEndpoint(int pageSize) {
        if (pageSize < 1) {
            throw new IllegalArgumentException("Page size must be positive: " + pageSize);
        }
        this.pageSize = pageSize;
    }

    @SafeVarargs
    public final InMemoryFeedEndpoint<PAYLOAD> add(PAYLOAD... entries) {
        return add(Arrays.asList(entries));
    }

    public InMemoryFeedEndpoint<PAYLOAD> add(List<PAYLOAD> entries) {
        this.entries.addAll(entries);
        return this;
    }

    @Override
    public Optional<FeedPage<Integer, InMemoryFeedEntry<PAYLOAD>>> getFirstPage() {
        return entries.isEmpty() ? Optional.empty() : Optional.of(new InMemoryFeedPage(0));
    }

    @Override
    public Optional<FeedPage<Integer, InMemoryFeedEntry<PAYLOAD>>> getLastPage() {
        return entries.isEmpty() ? Optional.empty() : Optional.of(new InMemoryFeedPage(entries.size() - 1));
    }

    @Override
    public Optional<FeedPage<Integer, InMemoryFeedEntry<PAYLOAD>>> getPage(Integer location) {
        return location < 0 || location >= entries.size()
            ? Optional.empty()
            : Optional.of(new InMemoryFeedPage(location));
    }

    private class InMemoryFeedPage implements FeedPage<Integer, InMemoryFeedEntry<PAYLOAD>> {

        private final int from, to, maximum;

        private InMemoryFeedPage(int location) {
            from = location - location % pageSize;
            to = Math.min(from + pageSize, entries.size());
            maximum = entries.size();
        }

        @Override
        public Integer getLocation() {
            return to - 1;
        }

        @Override
        public Optional<Integer> getPreviousLocation() {
            return from == 0 ? Optional.empty() : Optional.of(from - 1);
        }

        @Override
        public Optional<Integer> getNextLocation() {
            return maximum > to ? Optional.of(from + pageSize) : Optional.empty();
        }

        @Override
        public List<InMemoryFeedEntry<PAYLOAD>> getEntries() {
            return IntStream.range(from, to)
                .mapToObj(index -> new InMemoryFeedEntry<>(index, entries.get(index)))
                .collect(Collectors.toList());
        }

        @Override
        public String toString() {
            return "page[" + from + "/" + to + "]";
        }
    }
}
