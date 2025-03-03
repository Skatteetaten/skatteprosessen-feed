package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.publisher;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.LongSupplier;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedEndpoint;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedHttpClient;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedPublisher;

public class PublishedFeedEndpoint<PAYLOAD> implements FeedEndpoint<PublishedFeedLocation, PublishedFeedEntry<PAYLOAD>, PublishedFeedPage<PAYLOAD>> {

    private final Function<PublishedFeedLocation, List<FeedPublisher.Entry<? extends PAYLOAD>>> resolver;
    private final LongSupplier limit;

    public PublishedFeedEndpoint(Function<PublishedFeedLocation, List<FeedPublisher.Entry<? extends PAYLOAD>>> resolver) {
        this.resolver = resolver;
        limit = () -> getLastPage().map(page -> page.getLocation().getSequence()).orElse(FeedPublisher.INCEPTION);
    }

    public PublishedFeedEndpoint(Function<PublishedFeedLocation, List<FeedPublisher.Entry<? extends PAYLOAD>>> resolver, LongSupplier limit) {
        this.resolver = resolver;
        this.limit = limit;
    }

    public static <PAYLOAD> PublishedFeedEndpoint<PAYLOAD> ofDefaultPath(URL endpoint,
                                                                         FeedHttpClient httpClient,
                                                                         int size,
                                                                         Resolver<PAYLOAD> resolver) {
        return ofDefaultPath(path -> new FeedHttpClient.Request(endpoint, path), httpClient, size, resolver);
    }

    public static <PAYLOAD> PublishedFeedEndpoint<PAYLOAD> ofDefaultPath(Function<String, FeedHttpClient.Request> factory,
                                                                         FeedHttpClient httpClient,
                                                                         int size,
                                                                         Resolver<PAYLOAD> resolver) {
        return new PublishedFeedEndpoint<>(location -> {
            try {
                return httpClient.get(
                    factory.apply("{sequence}")
                        .withSubstitution("sequence", location.getSequence())
                        .withQuery("size", size)
                        .withQuery("backward", location.getDirection() == PublishedFeedLocation.Direction.BACKWARD),
                    response -> {
                        switch (response.getStatus()) {
                        case 200:
                            return resolver.apply(response.getContent());
                        case 204:
                            return Collections.emptyList();
                        default:
                            throw response.toException();
                        }
                    }
                );
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to read " + location, e);
            }
        }, () -> {
            try {
                return httpClient.get(factory.apply("limit"), response -> {
                    if (response.getStatus() == 200) {
                        return Long.parseLong(new String(response.getContent().readAllBytes(), StandardCharsets.UTF_8));
                    } else {
                        throw response.toException();
                    }
                });
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to read limit", e);
            }
        });
    }

    @Override
    public Optional<PublishedFeedPage<PAYLOAD>> getFirstPage() {
        return getPage(new PublishedFeedLocation(FeedPublisher.INCEPTION, PublishedFeedLocation.Direction.FORWARD));
    }

    @Override
    public Optional<PublishedFeedPage<PAYLOAD>> getLastPage() {
        return getPage(new PublishedFeedLocation(Long.MAX_VALUE, PublishedFeedLocation.Direction.BACKWARD));
    }

    @Override
    public Optional<PublishedFeedLocation> getLastLocation() {
        long sequence = limit.getAsLong();
        return sequence > 0 ? Optional.of(new PublishedFeedLocation(sequence, PublishedFeedLocation.Direction.FORWARD)) : Optional.empty();
    }

    @Override
    public Optional<PublishedFeedPage<PAYLOAD>> getPage(PublishedFeedLocation location) {
        List<FeedPublisher.Entry<? extends PAYLOAD>> entries = resolver.apply(location);
        return entries.isEmpty()
            ? Optional.empty()
            : Optional.of(new PublishedFeedPage<>(entries, location.getDirection() == PublishedFeedLocation.Direction.BACKWARD));
    }

    @FunctionalInterface
    public interface Resolver<PAYLOAD> {

        List<FeedPublisher.Entry<? extends PAYLOAD>> apply(InputStream inputStream) throws IOException;
    }
}
