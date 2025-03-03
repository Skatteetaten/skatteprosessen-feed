package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom.hopper;

import java.util.function.BiFunction;

import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndFeed;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedEntry;

public class AtomHopperFeedEntry<PAYLOAD> implements FeedEntry<AtomHopperFeedLocation> {

    private final BiFunction<SyndFeed, SyndEntry, PAYLOAD> payloadResolver;

    private final SyndFeed feed;
    private final SyndEntry entry;

    private final AtomHopperFeedLocation.Direction direction;

    AtomHopperFeedEntry(
        BiFunction<SyndFeed, SyndEntry, PAYLOAD> payloadResolver,
        SyndFeed feed, SyndEntry entry,
        AtomHopperFeedLocation.Direction direction
    ) {
        this.feed = feed;
        this.entry = entry;
        this.payloadResolver = payloadResolver;
        this.direction = direction;
    }

    @Override
    public AtomHopperFeedLocation getLocation() {
        return new AtomHopperFeedLocation(entry.getUri(), direction);
    }

    public PAYLOAD getPayload() {
        return payloadResolver.apply(feed, entry);
    }

    @Override
    public String toString() {
        return "entry@" + getLocation();
    }
}
