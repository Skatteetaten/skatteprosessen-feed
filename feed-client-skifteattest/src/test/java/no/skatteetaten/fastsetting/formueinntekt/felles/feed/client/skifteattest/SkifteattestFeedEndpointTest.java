package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.skifteattest;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.github.tomakehurst.wiremock.junit.WireMockRule;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.ApacheFeedHttpClient;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.skifteattest.dto.Hendelse;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.skifteattest.dto.SkifteattestHendelse;

public class SkifteattestFeedEndpointTest {
    private final ObjectWriter writer =
        new ObjectMapper().writerFor(new TypeReference<List<SkifteattestHendelse<String>>>() { });
    @Rule
    public WireMockRule wireMock = new WireMockRule(wireMockConfig().dynamicPort());
    private CloseableHttpClient httpClient;
    private SkifteattestFeedEndpoint<String> endpoint;

    public static List<SkifteattestHendelse<String>> hendelser(long from, long to) {
        return LongStream.iterate(from, value -> from > to ? value - 1 : value + 1)
            .limit(Math.abs(to - from) + 1)
            .mapToObj(sequence -> new SkifteattestHendelse<>(sequence, new Hendelse(), "entry-" + sequence))
            .collect(Collectors.toList());
    }

    @Before
    public void setUp() throws Exception {
        httpClient = HttpClients.createDefault();
        endpoint = new SkifteattestFeedEndpoint<>(
            new URL("http://localhost:" + wireMock.port()),
            new ApacheFeedHttpClient(httpClient),
            2, String.class
        );
    }

    @After
    public void tearDown() throws Exception {
        httpClient.close();
    }

    @Test
    public void can_read_first_page() throws Exception {
        stubFor(get(urlPathEqualTo("/api/feed/v1/0"))
            .withQueryParam("size", equalTo("2"))
            .willReturn(aResponse()
                .withStatus(200)
                .withBody(writer.writeValueAsString(hendelser(1, 2)))));

        assertThat(endpoint.getFirstPage()).hasValueSatisfying(page -> {
            assertThat(page.getLocation()).isEqualTo(0);
            assertThat(page.getNextLocation()).contains(2L);
            assertThatThrownBy(page::getPreviousLocation).isInstanceOf(UnsupportedOperationException.class);
            assertThat(page.getEntries())
                .extracting(SkifteattestFeedEntry::getPayload)
                .extracting(SkifteattestHendelse::getSekvensnummer)
                .containsExactly(1L, 2L);
            assertThat(page.hasLocation(2L)).isTrue();
            assertThat(page.hasLocation(42L)).isFalse();
        });
    }

    @Test
    public void can_read_last_page() throws Exception {
        stubFor(get(urlPathEqualTo("/api/feed/v1/limit"))
            .willReturn(aResponse()
                .withStatus(200)
                .withBody("1")));

        stubFor(get(urlPathEqualTo("/api/feed/v1/0"))
            .withQueryParam("size", equalTo("2"))
            .willReturn(aResponse()
                .withStatus(200)
                .withBody(writer.writeValueAsString(hendelser(1, 1)))));

        assertThat(endpoint.getLastPage()).hasValueSatisfying(page -> {
            assertThat(page.getLocation()).isEqualTo(0);
            assertThat(page.getNextLocation()).contains(1L);
            assertThatThrownBy(page::getPreviousLocation).isInstanceOf(UnsupportedOperationException.class);
            assertThat(page.getEntries())
                .extracting(SkifteattestFeedEntry::getPayload)
                .extracting(SkifteattestHendelse::getSekvensnummer)
                .containsExactly(1L);
            assertThat(page.hasLocation(1L)).isTrue();
            assertThat(page.hasLocation(42L)).isFalse();
        });
    }

    @Test
    public void can_read_page() throws Exception {
        stubFor(get(urlPathEqualTo("/api/feed/v1/1"))
            .withQueryParam("size", equalTo("2"))
            .willReturn(aResponse()
                .withStatus(200)
                .withBody(writer.writeValueAsString(hendelser(2, 3)))));

        assertThat(endpoint.getPage(1L)).hasValueSatisfying(page -> {
            assertThat(page.getLocation()).isEqualTo(1);
            assertThat(page.getNextLocation()).contains(3L);
            assertThatThrownBy(page::getPreviousLocation).isInstanceOf(UnsupportedOperationException.class);
            assertThat(page.getEntries())
                .extracting(SkifteattestFeedEntry::getPayload)
                .extracting(SkifteattestHendelse::getSekvensnummer)
                .containsExactly(2L, 3L);
            assertThat(page.hasLocation(2L)).isTrue();
            assertThat(page.hasLocation(42L)).isFalse();
        });
    }

    @Test
    public void can_read_empty_page() {
        stubFor(get(urlPathEqualTo("/api/feed/v1/1"))
            .withQueryParam("size", equalTo("2"))
            .willReturn(aResponse()
                .withStatus(204)));

        assertThat(endpoint.getPage(1L)).isEmpty();
    }

    @Test
    public void can_read_last_location() {
        stubFor(get(urlPathEqualTo("/api/feed/v1/limit"))
            .willReturn(aResponse()
                .withStatus(200)
                .withBody("3")));

        assertThat(endpoint.getLastLocation()).contains(3L);
    }

    @Test
    public void can_read_empty_limit() {
        stubFor(get(urlPathEqualTo("/api/feed/v1/limit"))
            .willReturn(aResponse()
                .withStatus(204)));

        assertThat(endpoint.getLastLocation()).isEmpty();
    }
}