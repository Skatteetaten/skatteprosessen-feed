package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom;

import java.io.BufferedInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedEndpoint;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedHttpClient;
import org.xml.sax.InputSource;

import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndFeed;
import com.rometools.rome.io.FeedException;
import com.rometools.rome.io.SyndFeedInput;

public class AtomFeedEndpoint<PAYLOAD> implements FeedEndpoint<AtomFeedLocation,
    AtomFeedEntry<PAYLOAD>,
    AtomFeedPage<PAYLOAD>> {

    private final URL endpoint;
    private final FeedHttpClient httpClient;

    private final boolean notFoundOnEmpty;

    private final BiFunction<SyndFeed, SyndEntry, PAYLOAD> payloadResolver;
    private final UrlResolver urlResolver;
    private final Function<String, String> linkResolver;
    private final String previous, next;

    private final SyndFeedInput feedInput = new SyndFeedInput();

    public AtomFeedEndpoint(
        URL endpoint,
        FeedHttpClient httpClient,
        boolean notFoundOnEmpty,
        BiFunction<SyndFeed, SyndEntry, PAYLOAD> payloadResolver
    ) {
        this(endpoint, httpClient, notFoundOnEmpty, payloadResolver, (url, page) -> {
            if (page == null || page.isEmpty()) {
                return new FeedHttpClient.Request(url, "");
            } else {
                return new FeedHttpClient.Request(url, "{page}").withSubstitution("page", page);
            }
        }, Function.identity(), "previous-archive", "next-archive");
    }

    public AtomFeedEndpoint(
        URL endpoint,
        FeedHttpClient httpClient,
        boolean notFoundOnEmpty,
        BiFunction<SyndFeed, SyndEntry, PAYLOAD> payloadResolver,
        UrlResolver urlResolver,
        Function<String, String> linkResolver,
        String previous, String next
    ) {
        this.endpoint = endpoint;
        this.httpClient = httpClient;
        this.notFoundOnEmpty = notFoundOnEmpty;
        this.payloadResolver = payloadResolver;
        this.urlResolver = urlResolver;
        this.linkResolver = linkResolver;
        this.previous = previous;
        this.next = next;
        feedInput.setXmlHealerOn(false);
    }

    @Override
    public Optional<AtomFeedPage<PAYLOAD>> getFirstPage() {
        return getPage(urlResolver.getFirstLocation(endpoint), null, notFoundOnEmpty);
    }

    @Override
    public Optional<AtomFeedPage<PAYLOAD>> getFirstPage(AtomFeedLocation boundary) {
        return getPage(urlResolver.getFirstLocation(endpoint), boundary.getEtag().orElse(null), notFoundOnEmpty);
    }

    @Override
    public Optional<AtomFeedPage<PAYLOAD>> getLastPage() {
        return getPage(urlResolver.getLastLocation(endpoint), null, notFoundOnEmpty);
    }

    @Override
    public Optional<AtomFeedPage<PAYLOAD>> getLastPage(AtomFeedLocation boundary) {
        return getPage(urlResolver.getLastLocation(endpoint), boundary.getEtag().orElse(null), notFoundOnEmpty);
    }

    @Override
    public Optional<AtomFeedPage<PAYLOAD>> getPage(AtomFeedLocation location) {
        return getPage(
            urlResolver.getLocation(endpoint, location.getPage()),
            location.getEtag().orElse(null),
            false
        );
    }

    @Override
    public Optional<AtomFeedPage<PAYLOAD>> getPage(AtomFeedLocation location, AtomFeedLocation boundary) {
        return location.equals(boundary) ? Optional.empty() : getPage(
            urlResolver.getLocation(endpoint, location.getPage()),
            boundary == null ? null : boundary.getEtag().orElse(null),
            false
        );
    }

    private Optional<AtomFeedPage<PAYLOAD>> getPage(FeedHttpClient.Request request, String etag, boolean notFoundOnEmpty) {
        try {
            Map<String, String> headers = new HashMap<>();
            if (etag != null) {
                headers.put("If-None-Match", etag);
            }
            return httpClient.get(request, headers, response -> {
                switch (response.getStatus()) {
                case 200:
                    try (Reader reader = new InputStreamReader(
                        new BufferedInputStream(response.getContent()),
                        response.getCharset().orElse(StandardCharsets.UTF_8)
                    )) {
                        return Optional.of(new AtomFeedPage<>(
                            feedInput.build(new InputSource(reader)),
                            response.getHeader("ETag").orElse(null),
                            payloadResolver, linkResolver,
                            previous, next
                        ));
                    } catch (FeedException e) {
                        throw new IllegalStateException("Could not parse feed page for " + request, e);
                    }
                case 304:
                    return Optional.empty();
                case 404:
                    if (notFoundOnEmpty) {
                        return Optional.empty();
                    }
                default:
                    throw response.toException();
                }
            });
        } catch (Exception e) {
            throw new IllegalStateException("Could not read from " + request, e);
        }
    }

    @FunctionalInterface
    public interface UrlResolver {

        default FeedHttpClient.Request getFirstLocation(URL url) {
            return getLocation(url, "first");
        }

        default FeedHttpClient.Request getLastLocation(URL url) {
            return getLocation(url, "");
        }

        FeedHttpClient.Request getLocation(URL url, String page);
    }
}
