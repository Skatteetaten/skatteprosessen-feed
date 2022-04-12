package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom;

import java.util.function.BiFunction;

import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndFeed;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedEntry;

public class AtomFeedEntry<PAYLOAD> implements FeedEntry<AtomFeedLocation> {

    private final BiFunction<SyndFeed, SyndEntry, PAYLOAD> payloadResolver;

    private final SyndFeed feed;
    private final SyndEntry entry;

    AtomFeedEntry(BiFunction<SyndFeed, SyndEntry, PAYLOAD> payloadResolver, SyndFeed feed, SyndEntry entry) {
        this.feed = feed;
        this.entry = entry;
        this.payloadResolver = payloadResolver;
    }

    @Override
    public AtomFeedLocation getLocation() {
        return new AtomFeedLocation(feed.getUri(), entry.getUri());
    }

    public PAYLOAD getPayload() {
        return payloadResolver.apply(feed, entry);
    }

    @Override
    public String toString() {
        return "entry@" + getLocation();
    }
}
