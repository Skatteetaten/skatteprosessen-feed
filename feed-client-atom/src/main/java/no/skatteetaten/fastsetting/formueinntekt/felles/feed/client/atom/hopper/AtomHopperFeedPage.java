package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom.hopper;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndFeed;
import com.rometools.rome.io.FeedException;
import com.rometools.rome.io.SyndFeedOutput;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedPage;

public class AtomHopperFeedPage<PAYLOAD> implements FeedPage<
    AtomHopperFeedLocation,
    AtomHopperFeedEntry<PAYLOAD>> {

    final SyndFeed feed;

    private final AtomHopperFeedLocation.Direction direction;

    private final BiFunction<SyndFeed, SyndEntry, PAYLOAD> payloadResolver;

    AtomHopperFeedPage(
        SyndFeed feed, AtomHopperFeedLocation.Direction direction,
        BiFunction<SyndFeed, SyndEntry, PAYLOAD> payloadResolver
    ) {
        this.feed = feed;
        this.direction = direction;
        this.payloadResolver = payloadResolver;
    }

    static Optional<String> toMarker(String url) {
        String[] parts = url.split("\\?");
        if (parts.length > 1) {
            for (String parameter : parts[1].split("&")) {
                String[] pair = parameter.split("=");
                if (pair[0].equals("marker")) {
                    return Optional.of(pair[1]).filter(marker -> !marker.isEmpty());
                }
            }
        }
        throw new IllegalArgumentException("Could not extract marker from: " + url);
    }

    @Override
    public AtomHopperFeedLocation getLocation() {
        return new AtomHopperFeedLocation(feed.getEntries().get(0).getUri(), direction);
    }

    @Override
    public Optional<AtomHopperFeedLocation> getPreviousLocation() {
        return feed.getLinks().stream()
            .filter(link -> link.getRel().equals("next"))
            .findAny()
            .flatMap(link -> AtomHopperFeedPage.toMarker(link.getHref()))
            .map(location -> new AtomHopperFeedLocation(location, AtomHopperFeedLocation.Direction.BACKWARD));
    }

    @Override
    public Optional<AtomHopperFeedLocation> getNextLocation() {
        return Optional.of(new AtomHopperFeedLocation(
            feed.getEntries().get(0).getUri(),
            AtomHopperFeedLocation.Direction.FORWARD
        ));
    }

    @Override
    public List<AtomHopperFeedEntry<PAYLOAD>> getEntries() {
        List<AtomHopperFeedEntry<PAYLOAD>> entries = feed.getEntries().stream()
            .map(entry -> new AtomHopperFeedEntry<>(payloadResolver, feed, entry, direction))
            .collect(Collectors.toList());
        Collections.reverse(entries);
        return entries;
    }

    @Override
    public boolean hasLocation(AtomHopperFeedLocation location) {
        return feed.getEntries().stream().anyMatch(entry -> entry.getUri().equals(location.getMarker()));
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
