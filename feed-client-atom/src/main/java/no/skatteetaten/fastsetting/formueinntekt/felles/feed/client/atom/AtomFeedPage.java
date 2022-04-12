package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndFeed;
import com.rometools.rome.feed.synd.SyndLink;
import com.rometools.rome.io.FeedException;
import com.rometools.rome.io.SyndFeedOutput;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedPage;

public class AtomFeedPage<PAYLOAD> implements FeedPage<AtomFeedLocation, AtomFeedEntry<PAYLOAD>> {

    private final SyndFeed feed;

    private final String etag;

    private final BiFunction<SyndFeed, SyndEntry, PAYLOAD> payloadResolver;

    private final Function<String, String> linkResolver;

    private final String previous, next;

    AtomFeedPage(
        SyndFeed feed, String etag,
        BiFunction<SyndFeed, SyndEntry, PAYLOAD> payloadResolver, Function<String, String> linkResolver,
        String previous, String next
    ) {
        this.feed = feed;
        this.etag = etag;
        this.payloadResolver = payloadResolver;
        this.linkResolver = linkResolver;
        this.previous = previous;
        this.next = next;
    }

    @Override
    public AtomFeedLocation getLocation() {
        return new AtomFeedLocation(feed.getUri(), feed.getEntries().stream()
            .findFirst()
            .map(SyndEntry::getUri)
            .orElse(null), etag);
    }

    @Override
    public Optional<AtomFeedLocation> getPageLocation() {
        return Optional.of(new AtomFeedLocation(feed.getUri()));
    }

    @Override
    public Optional<AtomFeedLocation> getPreviousLocation() {
        return feed.getLinks().stream()
            .filter(link -> link.getRel().equals(previous))
            .findAny()
            .map(SyndLink::getHref)
            .map(linkResolver)
            .map(AtomFeedLocation::new);
    }

    @Override
    public Optional<AtomFeedLocation> getNextLocation() {
        return feed.getLinks().stream()
            .filter(link -> link.getRel().equals(next))
            .findAny()
            .map(SyndLink::getHref)
            .map(linkResolver)
            .map(AtomFeedLocation::new);
    }

    @Override
    public List<AtomFeedEntry<PAYLOAD>> getEntries() {
        List<AtomFeedEntry<PAYLOAD>> entries = feed.getEntries().stream()
            .map(entry -> new AtomFeedEntry<>(payloadResolver, feed, entry))
            .collect(Collectors.toList());
        Collections.reverse(entries);
        return entries;
    }

    @Override
    public boolean hasLocation(AtomFeedLocation location) {
        return location.getLastEntry().isPresent() && location.getPage().equals(feed.getUri());
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
