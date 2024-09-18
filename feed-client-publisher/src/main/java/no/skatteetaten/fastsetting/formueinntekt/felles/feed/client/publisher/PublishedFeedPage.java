package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.publisher;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedPage;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedPublisher;

public class PublishedFeedPage<PAYLOAD> implements FeedPage<PublishedFeedLocation, PublishedFeedEntry<PAYLOAD>> {

    private final List<FeedPublisher.Entry<? extends PAYLOAD>> entries;

    private final boolean reversed;

    PublishedFeedPage(List<FeedPublisher.Entry<? extends PAYLOAD>> entries, boolean reversed) {
        this.entries = entries;
        this.reversed = reversed;
    }

    @Override
    public PublishedFeedLocation getLocation() {
        return new PublishedFeedLocation((reversed
            ? entries.get(0)
            : entries.get(entries.size() - 1)).getOffset(), PublishedFeedLocation.Direction.BACKWARD);
    }

    @Override
    public Optional<PublishedFeedLocation> getPreviousLocation() {
        return Optional.of(new PublishedFeedLocation((reversed
                ? entries.get(entries.size() - 1)
                : entries.get(0)).getOffset() - 1, PublishedFeedLocation.Direction.BACKWARD))
            .filter(location -> location.getSequence() > 0);
    }

    @Override
    public Optional<PublishedFeedLocation> getNextLocation() {
        return Optional.of(new PublishedFeedLocation((reversed
            ? entries.get(0)
            : entries.get(entries.size() - 1)).getOffset(), PublishedFeedLocation.Direction.FORWARD));
    }

    @Override
    public List<PublishedFeedEntry<PAYLOAD>> getEntries() {
        List<PublishedFeedEntry<PAYLOAD>> entries = this.entries.stream()
            .map(entry -> new PublishedFeedEntry<PAYLOAD>(entry.getOffset(), entry.getPayload()))
            .collect(Collectors.toList());
        if (reversed) {
            Collections.reverse(entries);
        }
        return entries;
    }

    @Override
    public boolean hasLocation(PublishedFeedLocation location) {
        long lower, upper;
        if (reversed) {
            lower = entries.get(entries.size() - 1).getOffset();
            upper = entries.get(0).getOffset();
        } else {
            lower = entries.get(0).getOffset();
            upper = entries.get(entries.size() - 1).getOffset();
        }
        return lower <= location.getSequence() && upper >= location.getSequence();
    }

    @Override
    public List<PublishedFeedEntry<PAYLOAD>> getEntriesAfter(PublishedFeedLocation location) {
        return getEntries().stream()
            .filter(entry -> entry.getLocation().getSequence() > location.getSequence())
            .collect(Collectors.toList());
    }

    @Override
    public List<PublishedFeedEntry<PAYLOAD>> getEntriesUntil(PublishedFeedLocation location) {
        return getEntries().stream()
            .filter(entry -> entry.getLocation().getSequence() <= location.getSequence())
            .collect(Collectors.toList());
    }

    @Override
    public List<PublishedFeedEntry<PAYLOAD>> getEntriesBetween(PublishedFeedLocation lower, PublishedFeedLocation upper) {
        return getEntries().stream()
            .filter(entry -> entry.getLocation().getSequence() > lower.getSequence())
            .filter(entry -> entry.getLocation().getSequence() <= upper.getSequence())
            .collect(Collectors.toList());
    }

    @Override
    public Optional<String> toDisplayString() {
        StringBuilder sb = new StringBuilder();
        entries.forEach(entry -> sb.append(entry.getOffset())
            .append('\n')
            .append(entry.getPayload())
            .append('\n')
            .append('\n'));
        return Optional.of(sb.toString());
    }

    @Override
    public String toString() {
        return "page@" + getLocation();
    }
}
