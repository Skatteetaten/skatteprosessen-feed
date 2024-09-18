package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.skifteattest;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedEndpoint;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedHttpClient;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.skifteattest.dto.SkifteattestHendelse;

public class SkifteattestFeedEndpoint<T>
    implements FeedEndpoint<Long, SkifteattestFeedEntry<T>, SkifteattestFeedPage<T>> {

    private final URL endpoint;
    private final FeedHttpClient httpClient;
    private final int size;
    private final ObjectReader reader;

    public SkifteattestFeedEndpoint(URL endpoint, FeedHttpClient httpClient, int size, Class<T> type) {
        this.endpoint = endpoint;
        this.httpClient = httpClient;
        this.size = size;
        ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        reader = objectMapper.readerFor(objectMapper.getTypeFactory().constructCollectionType(
            ArrayList.class,
            objectMapper.getTypeFactory().constructParametricType(SkifteattestHendelse.class, type)
        ));
    }

    @Override
    public Optional<SkifteattestFeedPage<T>> getFirstPage() {
        return getPage(0L);
    }

    @Override
    public Optional<Long> getLastLocation() {
        try {
            return httpClient.get(new FeedHttpClient.Request(endpoint, "api/feed/v1/limit"),
                FeedHttpClient.APPLICATION_JSON, response -> {
                    switch (response.getStatus()) {
                    case 200:
                        try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.getContent()))) {
                            return Optional.of(reader.readLine()).map(Long::valueOf).filter(limit -> limit > 0);
                        }
                    case 204:
                        return Optional.empty();
                    default:
                        throw response.toException();
                    }
                });
        } catch (Exception e) {
            throw new IllegalStateException("Could not get maximum offset from " + endpoint, e);
        }
    }

    @Override
    public Optional<SkifteattestFeedPage<T>> getLastPage() {
        return getLastLocation().flatMap(location -> getPage(location - 1));
    }

    @Override
    public Optional<SkifteattestFeedPage<T>> getPage(Long location) {
        try {
            return httpClient.get(
                new FeedHttpClient.Request(endpoint, "api/feed/v1/{location}").withSubstitution("location", location)
                    .withQuery("size", size), FeedHttpClient.APPLICATION_JSON, response -> {
                    switch (response.getStatus()) {
                    case 200:
                        return Optional.of(reader.<List<SkifteattestHendelse<T>>>readValue(response.getContent()))
                            .map(SkifteattestFeedPage::new);
                    case 204:
                        return Optional.empty();
                    default:
                        throw response.toException();
                    }
                });
        } catch (Exception e) {
            throw new IllegalStateException("Could not read from " + endpoint, e);
        }
    }
}
