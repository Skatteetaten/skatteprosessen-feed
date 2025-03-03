package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.publisher;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.ApacheFeedHttpClient;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedPublisher;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class PublishedFeedEndpointTest {

    @Rule
    public WireMockRule wireMock = new WireMockRule(wireMockConfig().dynamicPort());

    private CloseableHttpClient httpClient;
    private PublishedFeedEndpoint<String> endpoint;

    @Before
    public void setUp() throws Exception {
        httpClient = HttpClients.createDefault();
        endpoint = PublishedFeedEndpoint.ofDefaultPath(
            new URL("http://localhost:" + wireMock.port()),
            new ApacheFeedHttpClient(httpClient),
            2,
            inputStream -> {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
                    return reader.lines().map(line -> new FeedPublisher.Entry<>(
                        Long.parseLong(line.substring(0, line.indexOf(':'))),
                        line.substring(line.indexOf(':') + 1)
                    )).collect(Collectors.toList());
                }
            }
        );
    }

    @After
    public void tearDown() throws Exception {
        httpClient.close();
    }

    @Test
    public void can_read_first_page() {
        stubFor(get(urlPathEqualTo("/0"))
            .withQueryParam("backward", equalTo("false"))
            .withQueryParam("size", equalTo("2"))
            .willReturn(aResponse()
                .withStatus(200)
                .withBody("1:foo\n2:bar")));

        assertThat(endpoint.getFirstPage()).hasValueSatisfying(page -> {
            assertThat(page.getLocation()).isEqualTo(new PublishedFeedLocation(2, PublishedFeedLocation.Direction.BACKWARD));
            assertThat(page.getNextLocation()).contains(new PublishedFeedLocation(2, PublishedFeedLocation.Direction.FORWARD));
            assertThat(page.getPreviousLocation()).isNotPresent();
            assertThat(page.getEntries())
                .extracting(PublishedFeedEntry::getLocation)
                .extracting(PublishedFeedLocation::getSequence)
                .containsExactly(1L, 2L);
            assertThat(page.hasLocation(new PublishedFeedLocation(2, PublishedFeedLocation.Direction.BACKWARD))).isTrue();
            assertThat(page.hasLocation(new PublishedFeedLocation(42, PublishedFeedLocation.Direction.BACKWARD))).isFalse();
        });
    }
    @Test
    public void can_read_last_page() {
        stubFor(get(urlPathEqualTo("/" + Long.MAX_VALUE))
            .withQueryParam("backward", equalTo("true"))
            .withQueryParam("size", equalTo("2"))
            .willReturn(aResponse()
                .withStatus(200)
                .withBody("100:bar\n99:foo")));

        assertThat(endpoint.getLastPage()).hasValueSatisfying(page -> {
            assertThat(page.getLocation()).isEqualTo(new PublishedFeedLocation(100, PublishedFeedLocation.Direction.BACKWARD));
            assertThat(page.getNextLocation()).contains(new PublishedFeedLocation(100, PublishedFeedLocation.Direction.FORWARD));
            assertThat(page.getPreviousLocation())
                .contains(new PublishedFeedLocation(98, PublishedFeedLocation.Direction.BACKWARD));
            assertThat(page.getEntries())
                .extracting(PublishedFeedEntry::getLocation)
                .extracting(PublishedFeedLocation::getSequence)
                .containsExactly(99L, 100L);
            assertThat(page.hasLocation(new PublishedFeedLocation(100L, PublishedFeedLocation.Direction.BACKWARD))).isTrue();
            assertThat(page.hasLocation(new PublishedFeedLocation(42, PublishedFeedLocation.Direction.BACKWARD))).isFalse();
        });
    }

    @Test
    public void can_read_page_forward() {
        stubFor(get(urlPathEqualTo("/49"))
            .withQueryParam("backward", equalTo("false"))
            .withQueryParam("size", equalTo("2"))
            .willReturn(aResponse()
                .withStatus(200)
                .withBody("50:foo\n51:bar")));

        assertThat(endpoint.getPage(
            new PublishedFeedLocation(49, PublishedFeedLocation.Direction.FORWARD)
        )).hasValueSatisfying(page -> {
            assertThat(page.getLocation()).isEqualTo(new PublishedFeedLocation(51, PublishedFeedLocation.Direction.BACKWARD));
            assertThat(page.getNextLocation()).contains(new PublishedFeedLocation(51, PublishedFeedLocation.Direction.FORWARD));
            assertThat(page.getPreviousLocation()).contains(new PublishedFeedLocation(49, PublishedFeedLocation.Direction.BACKWARD));
            assertThat(page.getEntries())
                .extracting(PublishedFeedEntry::getLocation)
                .extracting(PublishedFeedLocation::getSequence)
                .containsExactly(50L, 51L);
            assertThat(page.hasLocation(new PublishedFeedLocation(50, PublishedFeedLocation.Direction.BACKWARD))).isTrue();
            assertThat(page.hasLocation(new PublishedFeedLocation(42, PublishedFeedLocation.Direction.BACKWARD))).isFalse();
        });
    }

    @Test
    public void can_read_page_backward() {
        stubFor(get(urlPathEqualTo("/50"))
            .withQueryParam("backward", equalTo("true"))
            .withQueryParam("size", equalTo("2"))
            .willReturn(aResponse()
                .withStatus(200)
                .withBody("50:foo\n49:bar")));

        assertThat(endpoint.getPage(
            new PublishedFeedLocation(50, PublishedFeedLocation.Direction.BACKWARD)
        )).hasValueSatisfying(page -> {
            assertThat(page.getLocation()).isEqualTo(new PublishedFeedLocation(50, PublishedFeedLocation.Direction.BACKWARD));
            assertThat(page.getNextLocation()).contains(new PublishedFeedLocation(50, PublishedFeedLocation.Direction.FORWARD));
            assertThat(page.getPreviousLocation()).contains(new PublishedFeedLocation(48, PublishedFeedLocation.Direction.BACKWARD));
            assertThat(page.getEntries())
                .extracting(PublishedFeedEntry::getLocation)
                .extracting(PublishedFeedLocation::getSequence)
                .containsExactly(49L, 50L);
            assertThat(page.hasLocation(new PublishedFeedLocation(50, PublishedFeedLocation.Direction.BACKWARD))).isTrue();
            assertThat(page.hasLocation(new PublishedFeedLocation(42, PublishedFeedLocation.Direction.BACKWARD))).isFalse();
        });
    }

    @Test
    public void can_read_empty_page() {
        stubFor(get(urlPathEqualTo("/50"))
            .withQueryParam("backward", equalTo("false"))
            .withQueryParam("size", equalTo("2"))
            .willReturn(aResponse().withStatus(204)));

        assertThat(endpoint.getPage(new PublishedFeedLocation(50, PublishedFeedLocation.Direction.FORWARD))).isNotPresent();
    }

    @Test
    public void can_read_limit() {
        stubFor(get(urlPathEqualTo("/limit"))
            .willReturn(aResponse()
                .withStatus(200)
                .withBody("50")));

        assertThat(endpoint.getLastLocation()).contains(new PublishedFeedLocation(50, PublishedFeedLocation.Direction.FORWARD));
    }
}
