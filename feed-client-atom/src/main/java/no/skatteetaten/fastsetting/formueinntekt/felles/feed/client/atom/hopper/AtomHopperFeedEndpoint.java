package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom.hopper;

import java.io.BufferedInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.function.BiFunction;

import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndFeed;
import com.rometools.rome.io.FeedException;
import com.rometools.rome.io.SyndFeedInput;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedEndpoint;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedHttpClient;
import org.xml.sax.InputSource;

public class AtomHopperFeedEndpoint<PAYLOAD> implements FeedEndpoint<AtomHopperFeedLocation,
    AtomHopperFeedEntry<PAYLOAD>,
    AtomHopperFeedPage<PAYLOAD>> {

    private final URL endpoint;
    private final FeedHttpClient httpClient;

    private final boolean notFoundOnEmpty;
    private final int size;

    private final BiFunction<SyndFeed, SyndEntry, PAYLOAD> payloadResolver;

    private final SyndFeedInput feedInput = new SyndFeedInput();

    public AtomHopperFeedEndpoint(
        URL endpoint,
        FeedHttpClient httpClient,
        boolean notFoundOnEmpty,
        int size,
        BiFunction<SyndFeed, SyndEntry, PAYLOAD> payloadResolver
    ) {
        this.endpoint = endpoint;
        this.httpClient = httpClient;
        this.notFoundOnEmpty = notFoundOnEmpty;
        this.size = size;
        this.payloadResolver = payloadResolver;
        feedInput.setXmlHealerOn(false);
    }

    @Override
    public Optional<AtomHopperFeedPage<PAYLOAD>> getFirstPage() {
        return getLastPage()
            .map(page -> page.feed)
            .flatMap(feed -> feed.getLinks().stream().filter(link -> link.getRel().equals("last")).findAny())
            .flatMap(link -> AtomHopperFeedPage.toMarker(link.getHref()))
            .flatMap(marker -> getPage(marker, AtomHopperFeedLocation.Direction.BACKWARD, false));
    }

    @Override
    public Optional<AtomHopperFeedPage<PAYLOAD>> getLastPage() {
        return getPage(null, AtomHopperFeedLocation.Direction.BACKWARD, notFoundOnEmpty);
    }

    @Override
    public Optional<AtomHopperFeedPage<PAYLOAD>> getPage(AtomHopperFeedLocation location) {
        return getPage(location.getMarker(), location.getDirection(), false);
    }

    private Optional<AtomHopperFeedPage<PAYLOAD>> getPage(
        String marker,
        AtomHopperFeedLocation.Direction direction,
        boolean notFoundOnEmpty
    ) {
        FeedHttpClient.Request request = new FeedHttpClient.Request(endpoint, "");
        if (marker != null) {
            request = request.withQuery("marker", marker);
        }
        switch (direction) {
        case FORWARD:
            request = request.withQuery("direction", "forward");
            break;
        case BACKWARD:
            request = request.withQuery("direction", "backward");
            break;
        default:
            throw new IllegalStateException("Unknown direction: " + direction);
        }
        try {
            return httpClient.get(request.withQuery("limit", Integer.toString(size)), response -> {
                switch (response.getStatus()) {
                case 200:
                    try (Reader reader = new InputStreamReader(
                        new BufferedInputStream(response.getContent()),
                        response.getCharset().orElse(StandardCharsets.UTF_8)
                    )) {
                        SyndFeed feed = feedInput.build(new InputSource(reader));
                        return feed.getEntries().isEmpty()
                            ? Optional.empty()
                            : Optional.of(new AtomHopperFeedPage<>(feed, direction, payloadResolver));
                    } catch (FeedException e) {
                        throw new IllegalStateException("Could not parse feed page for " + endpoint, e);
                    }
                case 404:
                    if (notFoundOnEmpty) {
                        return Optional.empty();
                    }
                default:
                    throw response.toException();
                }
            });
        } catch (Exception e) {
            throw new IllegalStateException("Could not read from " + endpoint, e);
        }
    }
}