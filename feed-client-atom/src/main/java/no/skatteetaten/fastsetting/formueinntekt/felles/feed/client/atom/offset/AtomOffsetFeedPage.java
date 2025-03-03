package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom.offset;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndFeed;
import com.rometools.rome.io.FeedException;
import com.rometools.rome.io.SyndFeedOutput;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedPage;

public class AtomOffsetFeedPage<PAYLOAD> implements FeedPage<
    AtomOffsetFeedLocation,
    AtomOffsetFeedEntry<PAYLOAD>> {

    private final SyndFeed feed;

    private final boolean reversed;

    private final ToLongFunction<String> offsetResolver;

    private final BiFunction<SyndFeed, SyndEntry, PAYLOAD> payloadResolver;

    AtomOffsetFeedPage(
        SyndFeed feed,
        boolean reversed,
        ToLongFunction<String> offsetResolver,
        BiFunction<SyndFeed, SyndEntry, PAYLOAD> payloadResolver
    ) {
        this.feed = feed;
        this.reversed = reversed;
        this.offsetResolver = offsetResolver;
        this.payloadResolver = payloadResolver;
    }

    @Override
    public AtomOffsetFeedLocation getLocation() {
        return new AtomOffsetFeedLocation(offsetResolver.applyAsLong(reversed
            ? feed.getEntries().get(0).getUri()
            : feed.getEntries().get(feed.getEntries().size() - 1).getUri()), AtomOffsetFeedLocation.Direction.BACKWARD);
    }

    @Override
    public Optional<AtomOffsetFeedLocation> getPreviousLocation() {
        return Optional.of(new AtomOffsetFeedLocation(
            offsetResolver.applyAsLong(reversed
                ? feed.getEntries().get(feed.getEntries().size() - 1).getUri()
                : feed.getEntries().get(0).getUri()) - 1,
            AtomOffsetFeedLocation.Direction.BACKWARD
        )).filter(location -> location.getOffset() > 0);
    }

    @Override
    public Optional<AtomOffsetFeedLocation> getNextLocation() {
        return Optional.of(new AtomOffsetFeedLocation(
            offsetResolver.applyAsLong(reversed
                ? feed.getEntries().get(0).getUri()
                : feed.getEntries().get(feed.getEntries().size() - 1).getUri()),
            AtomOffsetFeedLocation.Direction.FORWARD
        ));
    }

    @Override
    public List<AtomOffsetFeedEntry<PAYLOAD>> getEntries() {
        List<AtomOffsetFeedEntry<PAYLOAD>> entries = feed.getEntries().stream()
            .map(entry -> new AtomOffsetFeedEntry<>(offsetResolver, payloadResolver, feed, entry))
            .collect(Collectors.toList());
        if (reversed) {
            Collections.reverse(entries);
        }
        return entries;
    }

    @Override
    public boolean hasLocation(AtomOffsetFeedLocation location) {
        long lower, upper;
        if (reversed) {
            upper = offsetResolver.applyAsLong(feed.getEntries().get(0).getUri());
            lower = offsetResolver.applyAsLong(feed.getEntries().get(feed.getEntries().size() - 1).getUri());
        } else {
            lower = offsetResolver.applyAsLong(feed.getEntries().get(0).getUri());
            upper = offsetResolver.applyAsLong(feed.getEntries().get(feed.getEntries().size() - 1).getUri());
        }
        return lower <= location.getOffset() && upper >= location.getOffset();
    }

    @Override
    public List<AtomOffsetFeedEntry<PAYLOAD>> getEntriesAfter(AtomOffsetFeedLocation location) {
        return getEntries().stream()
            .filter(entry -> entry.getLocation().getOffset() > location.getOffset())
            .collect(Collectors.toList());
    }

    @Override
    public List<AtomOffsetFeedEntry<PAYLOAD>> getEntriesUntil(AtomOffsetFeedLocation location) {
        return getEntries().stream()
            .filter(entry -> entry.getLocation().getOffset() <= location.getOffset())
            .collect(Collectors.toList());
    }

    @Override
    public List<AtomOffsetFeedEntry<PAYLOAD>> getEntriesBetween(
        AtomOffsetFeedLocation lower, AtomOffsetFeedLocation upper
    ) {
        return getEntries().stream()
            .filter(entry -> entry.getLocation().getOffset() > lower.getOffset())
            .filter(entry -> entry.getLocation().getOffset() <= upper.getOffset())
            .collect(Collectors.toList());
    }

    @Override
    public Optional<String> toDisplayString() {
        try {
            return Optional.of(new SyndFeedOutput().outputString(feed, true));
        } catch (FeedException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public String toString() {
        return "page@" + getLocation();
    }
}
