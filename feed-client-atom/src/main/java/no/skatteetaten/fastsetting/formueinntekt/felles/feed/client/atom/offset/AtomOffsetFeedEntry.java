package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom.offset;

import java.util.function.BiFunction;
import java.util.function.ToLongFunction;

import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndFeed;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedEntry;

public class AtomOffsetFeedEntry<PAYLOAD> implements FeedEntry<AtomOffsetFeedLocation> {

    private final ToLongFunction<String> offsetResolver;
    private final BiFunction<SyndFeed, SyndEntry, PAYLOAD> payloadResolver;

    private final SyndFeed feed;
    private final SyndEntry entry;

    AtomOffsetFeedEntry(
        ToLongFunction<String> offsetResolver, BiFunction<SyndFeed, SyndEntry, PAYLOAD> payloadResolver,
        SyndFeed feed, SyndEntry entry
    ) {
        this.offsetResolver = offsetResolver;
        this.feed = feed;
        this.entry = entry;
        this.payloadResolver = payloadResolver;
    }

    @Override
    public AtomOffsetFeedLocation getLocation() {
        return new AtomOffsetFeedLocation(offsetResolver.applyAsLong(entry.getUri()), AtomOffsetFeedLocation.Direction.BACKWARD);
    }

    public PAYLOAD getPayload() {
        return payloadResolver.apply(feed, entry);
    }

    @Override
    public String toString() {
        return "entry@" + getLocation();
    }
}
