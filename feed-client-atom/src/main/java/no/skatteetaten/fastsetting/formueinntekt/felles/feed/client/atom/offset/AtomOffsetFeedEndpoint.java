package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom.offset;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.ToLongFunction;

import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndFeed;
import com.rometools.rome.io.FeedException;
import com.rometools.rome.io.SyndFeedInput;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedEndpoint;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedHttpClient;
import org.xml.sax.InputSource;

public abstract class AtomOffsetFeedEndpoint<PAYLOAD> implements FeedEndpoint<AtomOffsetFeedLocation,
    AtomOffsetFeedEntry<PAYLOAD>,
    AtomOffsetFeedPage<PAYLOAD>> {

    final URL endpoint;
    final FeedHttpClient httpClient;

    final boolean notFoundOnEmpty;
    final int size;

    private final ToLongFunction<String> offsetResolver;
    private final BiFunction<SyndFeed, SyndEntry, PAYLOAD> payloadResolver;
    final UrlResolver urlResolver;

    private final boolean alwaysReversed, inclusiveForward;

    private final SyndFeedInput feedInput = new SyndFeedInput();

    AtomOffsetFeedEndpoint(
        URL endpoint,
        FeedHttpClient httpClient,
        boolean notFoundOnEmpty,
        int size,
        ToLongFunction<String> offsetResolver,
        BiFunction<SyndFeed, SyndEntry, PAYLOAD> payloadResolver,
        UrlResolver urlResolver,
        boolean alwaysReversed,
        boolean inclusiveForward
    ) {
        this.endpoint = endpoint;
        this.httpClient = httpClient;
        this.notFoundOnEmpty = notFoundOnEmpty;
        this.size = size;
        this.offsetResolver = offsetResolver;
        this.payloadResolver = payloadResolver;
        this.urlResolver = urlResolver;
        this.alwaysReversed = alwaysReversed;
        this.inclusiveForward = inclusiveForward;
        feedInput.setXmlHealerOn(false);
    }

    @Override
    public Optional<AtomOffsetFeedPage<PAYLOAD>> getPage(AtomOffsetFeedLocation location) {
        return getPage(urlResolver.getLocation(
            endpoint, location.getDirection(), String.valueOf(inclusiveForward ? location.getOffset() : location.getRequestOffset()), size
        ), location.getDirection(), false);
    }

    Optional<AtomOffsetFeedPage<PAYLOAD>> getPage(
        FeedHttpClient.Request request, AtomOffsetFeedLocation.Direction direction, boolean notFoundOnEmpty
    ) {
        try  {
            return httpClient.get(request, FeedHttpClient.APPLICATION_ATOM_XML, response -> {
                switch (response.getStatus()) {
                case 200:
                    try (Reader reader = new InputStreamReader(
                        new BufferedInputStream(response.getContent()),
                        response.getCharset().orElse(StandardCharsets.UTF_8)
                    )) {
                        SyndFeed feed = feedInput.build(new InputSource(reader));
                        return feed.getEntries().isEmpty() ? Optional.empty() : Optional.of(new AtomOffsetFeedPage<>(feed,
                            alwaysReversed || direction == AtomOffsetFeedLocation.Direction.BACKWARD,
                            offsetResolver,
                            payloadResolver));
                    } catch (FeedException e) {
                        throw new IllegalStateException("Could not parse feed page for " + request, e);
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
            throw new IllegalStateException("Could not read from " + request, e);
        }
    }

    @FunctionalInterface
    public interface UrlResolver {

        FeedHttpClient.Request getLocation(URL endpoint, AtomOffsetFeedLocation.Direction direction, String offset, int count);
    }

    public static class WithLinkedUpperOffset<PAYLOAD> extends AtomOffsetFeedEndpoint<PAYLOAD> {

        private final String limit;

        public WithLinkedUpperOffset(
            URL endpoint,
            FeedHttpClient httpClient,
            boolean notFoundOnEmpty,
            int size,
            BiFunction<SyndFeed, SyndEntry, PAYLOAD> payloadResolver
        ) {
            this(endpoint, httpClient, notFoundOnEmpty, size, Long::parseLong, payloadResolver, (url, direction, offset, count) -> {
                FeedHttpClient.Request request = new FeedHttpClient.Request(url, "/{direction}/{offset}");
                switch (direction) {
                case FORWARD:
                    request.withSubstitution("direction", "kronologisk");
                    break;
                case BACKWARD:
                    request.withSubstitution("direction", "atom");
                    break;
                default:
                    throw new IllegalStateException("Unknown direction: " + direction);
                }
                return request.withSubstitution("offset", offset).withQuery("size", size);
            }, "kronologisk/limit", false, false);
        }

        public WithLinkedUpperOffset(
            URL endpoint,
            FeedHttpClient httpClient,
            boolean notFoundOnEmpty,
            int size,
            ToLongFunction<String> offsetResolver,
            BiFunction<SyndFeed, SyndEntry, PAYLOAD> payloadResolver,
            UrlResolver urlResolver,
            String limit,
            boolean alwaysReversed,
            boolean inclusiveForward
        ) {
            super(endpoint, httpClient, notFoundOnEmpty, size, offsetResolver, payloadResolver, urlResolver, alwaysReversed, inclusiveForward);
            this.limit = limit;
        }

        @Override
        public Optional<AtomOffsetFeedPage<PAYLOAD>> getFirstPage() {
            return getPage(
                urlResolver.getLocation(endpoint, AtomOffsetFeedLocation.Direction.FORWARD, "0", size),
                AtomOffsetFeedLocation.Direction.FORWARD,
                notFoundOnEmpty
            );
        }

        @Override
        public Optional<AtomOffsetFeedLocation> getLastLocation() {
            try {
                return httpClient.get(new FeedHttpClient.Request(endpoint, limit), response -> {
                    switch (response.getStatus()) {
                    case 200:
                        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                            response.getContent(),
                            response.getCharset().orElse(StandardCharsets.UTF_8)
                        ))) {
                            return Optional.of(new AtomOffsetFeedLocation(
                                Long.parseLong(reader.readLine()),
                                AtomOffsetFeedLocation.Direction.FORWARD
                            ));
                        }
                    case 404:
                    default:
                        throw response.toException();
                    }
                });
            } catch (Exception e) {
                throw new IllegalStateException("Could not read feed limit from " + endpoint, e);
            }
        }

        @Override
        public Optional<AtomOffsetFeedPage<PAYLOAD>> getLastPage() {
            return getLastLocation().flatMap(location -> getPage(
                urlResolver.getLocation(
                    endpoint,
                    AtomOffsetFeedLocation.Direction.BACKWARD,
                    String.valueOf(location.getOffset()),
                    size
                ),
                AtomOffsetFeedLocation.Direction.BACKWARD,
                notFoundOnEmpty
            ));
        }
    }

    public static class WithNamedUpperOffset<PAYLOAD> extends AtomOffsetFeedEndpoint<PAYLOAD> {

        private final String first, last;

        public WithNamedUpperOffset(
            URL endpoint,
            FeedHttpClient httpClient,
            boolean notFoundOnEmpty,
            int size,
            BiFunction<SyndFeed, SyndEntry, PAYLOAD> payloadResolver
        ) {
            this(endpoint, httpClient, notFoundOnEmpty, size, Long::parseLong, payloadResolver, (url, direction, offset, count) -> {
                FeedHttpClient.Request request;
                if (!"siste".equals(offset)) {
                    request = new FeedHttpClient.Request(endpoint, "/{offset}/{type}/{count}").withSubstitution("offset", offset);
                    switch (direction) {
                    case FORWARD:
                        request.withSubstitution("type", "nyere");
                        break;
                    case BACKWARD:
                        request.withSubstitution("type", "eldre");
                        break;
                    default:
                        throw new IllegalStateException("Unknown direction: " + direction);
                    }
                } else if (direction != AtomOffsetFeedLocation.Direction.BACKWARD) {
                    throw new IllegalArgumentException("Cannot read last location forward for " + url);
                } else {
                    request = new FeedHttpClient.Request(endpoint, "/siste/{count}");
                }
                return request.withSubstitution("count", count);
            }, "0", "siste", false, false);
        }

        public WithNamedUpperOffset(
            URL endpoint,
            FeedHttpClient httpClient,
            boolean notFoundOnEmpty,
            int size,
            ToLongFunction<String> offsetResolver,
            BiFunction<SyndFeed, SyndEntry, PAYLOAD> payloadResolver,
            UrlResolver urlResolver,
            String first, String last,
            boolean alwaysReversed,
            boolean inclusiveForward
        ) {
            super(endpoint, httpClient, notFoundOnEmpty, size, offsetResolver, payloadResolver, urlResolver, alwaysReversed, inclusiveForward);
            this.first = first;
            this.last = last;
        }

        @Override
        public Optional<AtomOffsetFeedPage<PAYLOAD>> getFirstPage() {
            return getPage(
                urlResolver.getLocation(endpoint, AtomOffsetFeedLocation.Direction.FORWARD, first, size),
                AtomOffsetFeedLocation.Direction.FORWARD,
                notFoundOnEmpty
            );
        }

        @Override
        public Optional<AtomOffsetFeedPage<PAYLOAD>> getLastPage() {
            return getPage(
                urlResolver.getLocation(endpoint, AtomOffsetFeedLocation.Direction.BACKWARD, last, size),
                AtomOffsetFeedLocation.Direction.BACKWARD,
                notFoundOnEmpty
            );
        }
    }
}
