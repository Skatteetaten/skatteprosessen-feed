package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom.offset;

import static org.assertj.core.api.Assertions.assertThat;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

import java.net.URL;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.ApacheFeedHttpClient;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom.AtomFeedContentTextResolver;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.rometools.rome.feed.synd.SyndContent;
import com.rometools.rome.feed.synd.SyndContentImpl;
import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndEntryImpl;
import com.rometools.rome.feed.synd.SyndFeed;
import com.rometools.rome.feed.synd.SyndFeedImpl;
import com.rometools.rome.io.SyndFeedOutput;

public class AtomOffsetFeedEndpointWithLinkedUpperOffsetTest {

    @Rule
    public WireMockRule wireMock = new WireMockRule(wireMockConfig().dynamicPort());

    private CloseableHttpClient httpClient;
    private AtomOffsetFeedEndpoint<Optional<String>> endpoint;

    private SyndFeedOutput feedOutput = new SyndFeedOutput();

    @Before
    public void setUp() throws Exception {
        httpClient = HttpClients.createDefault();
        endpoint = new AtomOffsetFeedEndpoint.WithLinkedUpperOffset<>(
            new URL("http://localhost:" + wireMock.port()),
            new ApacheFeedHttpClient(httpClient),
            false, 2, new AtomFeedContentTextResolver()
        );
    }

    @After
    public void tearDown() throws Exception {
        httpClient.close();
    }

    @Test
    public void can_read_first_page() throws Exception {
        stubFor(get(urlPathEqualTo("/kronologisk/0"))
            .withQueryParam("size", equalTo("2"))
            .willReturn(aResponse()
                .withStatus(200)
                .withBody(feedOutput.outputString(feed(1, 2)))));

        Assertions.assertThat(endpoint.getFirstPage()).hasValueSatisfying(page -> {
            Assertions.assertThat(page.getLocation().getOffset()).isEqualTo(2);
            Assertions.assertThat(page.getLocation().getDirection()).isEqualTo(AtomOffsetFeedLocation.Direction.BACKWARD);
            Assertions.assertThat(page.getPreviousLocation()).isNotPresent();
            Assertions.assertThat(page.getNextLocation())
                .contains(new AtomOffsetFeedLocation(2, AtomOffsetFeedLocation.Direction.FORWARD));
            Assertions.assertThat(page.getEntries())
                .extracting(AtomOffsetFeedEntry::getLocation)
                .extracting(AtomOffsetFeedLocation::getOffset)
                .containsExactly(1L, 2L);
            Assertions.assertThat(page.hasLocation(new AtomOffsetFeedLocation(1, AtomOffsetFeedLocation.Direction.BACKWARD)))
                .isTrue();
            Assertions.assertThat(page.hasLocation(new AtomOffsetFeedLocation(42, AtomOffsetFeedLocation.Direction.BACKWARD)))
                .isFalse();
        });
    }

    @Test
    public void can_read_last_page() throws Exception {
        stubFor(get(urlPathEqualTo("/kronologisk/limit"))
            .willReturn(aResponse()
                .withStatus(200)
                .withBody("100")));
        stubFor(get(urlPathEqualTo("/atom/100"))
            .withQueryParam("size", equalTo("2"))
            .willReturn(aResponse()
                .withStatus(200)
                .withBody(feedOutput.outputString(feed(100, 99)))));

        Assertions.assertThat(endpoint.getLastPage()).hasValueSatisfying(page -> {
            Assertions.assertThat(page.getLocation().getOffset()).isEqualTo(100);
            Assertions.assertThat(page.getLocation().getDirection()).isEqualTo(AtomOffsetFeedLocation.Direction.BACKWARD);
            Assertions.assertThat(page.getPreviousLocation())
                .contains(new AtomOffsetFeedLocation(98, AtomOffsetFeedLocation.Direction.BACKWARD));
            Assertions.assertThat(page.getNextLocation())
                .contains(new AtomOffsetFeedLocation(100, AtomOffsetFeedLocation.Direction.FORWARD));
            Assertions.assertThat(page.getEntries())
                .extracting(AtomOffsetFeedEntry::getLocation)
                .extracting(AtomOffsetFeedLocation::getOffset)
                .containsExactly(99L, 100L);
            Assertions.assertThat(page.hasLocation(new AtomOffsetFeedLocation(100, AtomOffsetFeedLocation.Direction.BACKWARD)))
                .isTrue();
            Assertions.assertThat(page.hasLocation(new AtomOffsetFeedLocation(42, AtomOffsetFeedLocation.Direction.BACKWARD)))
                .isFalse();
        });
    }

    @Test
    public void can_read_forward() throws Exception {
        stubFor(get(urlPathEqualTo("/kronologisk/50"))
            .withQueryParam("size", equalTo("2"))
            .willReturn(aResponse()
                .withStatus(200)
                .withBody(feedOutput.outputString(feed(50, 51)))));

        assertThat(endpoint.getPage(
            new AtomOffsetFeedLocation(49, AtomOffsetFeedLocation.Direction.FORWARD)
        )).hasValueSatisfying(page -> {
            assertThat(page.getLocation().getOffset()).isEqualTo(51);
            assertThat(page.getLocation().getDirection()).isEqualTo(AtomOffsetFeedLocation.Direction.BACKWARD);
            assertThat(page.getPreviousLocation())
                .contains(new AtomOffsetFeedLocation(49, AtomOffsetFeedLocation.Direction.BACKWARD));
            assertThat(page.getNextLocation())
                .contains(new AtomOffsetFeedLocation(51, AtomOffsetFeedLocation.Direction.FORWARD));
            assertThat(page.getEntries())
                .extracting(AtomOffsetFeedEntry::getLocation)
                .extracting(AtomOffsetFeedLocation::getOffset)
                .containsExactly(50L, 51L);
            assertThat(page.hasLocation(new AtomOffsetFeedLocation(50, AtomOffsetFeedLocation.Direction.BACKWARD)))
                .isTrue();
            assertThat(page.hasLocation(new AtomOffsetFeedLocation(42, AtomOffsetFeedLocation.Direction.BACKWARD)))
                .isFalse();
        });
    }

    @Test
    public void can_read_backward() throws Exception {
        stubFor(get(urlPathEqualTo("/atom/50"))
            .withQueryParam("size", equalTo("2"))
            .willReturn(aResponse()
                .withStatus(200)
                .withBody(feedOutput.outputString(feed(50, 49)))));

        assertThat(endpoint.getPage(
            new AtomOffsetFeedLocation(50, AtomOffsetFeedLocation.Direction.BACKWARD)
        )).hasValueSatisfying(page -> {
            assertThat(page.getLocation().getOffset()).isEqualTo(50);
            assertThat(page.getLocation().getDirection()).isEqualTo(AtomOffsetFeedLocation.Direction.BACKWARD);
            assertThat(page.getPreviousLocation()).contains(
                new AtomOffsetFeedLocation(48, AtomOffsetFeedLocation.Direction.BACKWARD)
            );
            assertThat(page.getNextLocation()).contains(
                new AtomOffsetFeedLocation(50, AtomOffsetFeedLocation.Direction.FORWARD)
            );
            assertThat(page.getEntries())
                .extracting(AtomOffsetFeedEntry::getLocation)
                .extracting(AtomOffsetFeedLocation::getOffset)
                .containsExactly(49L, 50L);
            assertThat(page.hasLocation(new AtomOffsetFeedLocation(50, AtomOffsetFeedLocation.Direction.BACKWARD)))
                .isTrue();
            assertThat(page.hasLocation(new AtomOffsetFeedLocation(42, AtomOffsetFeedLocation.Direction.BACKWARD)))
                .isFalse();
        });
    }

    @Test
    public void can_read_empty_page() throws Exception {
        SyndFeed feed = new SyndFeedImpl();
        feed.setFeedType("atom_1.0");
        feed.setUri("test");

        stubFor(get(urlPathEqualTo("/kronologisk/50"))
            .withQueryParam("size", equalTo("2"))
            .willReturn(aResponse()
                .withStatus(200)
                .withBody(feedOutput.outputString(feed))));

        assertThat(endpoint.getPage(
            new AtomOffsetFeedLocation(49, AtomOffsetFeedLocation.Direction.FORWARD)
        )).isNotPresent();
    }

    private static SyndFeed feed(long from, long to) {
        SyndFeed feed = new SyndFeedImpl();
        feed.setFeedType("atom_1.0");
        feed.setUri("test");
        feed.setEntries(LongStream.iterate(from, offset -> from > to ? offset - 1 : offset + 1).mapToObj(offset -> {
            SyndEntry entry = new SyndEntryImpl();
            entry.setUri(String.valueOf(offset));
            SyndContent content = new SyndContentImpl();
            content.setType("plain/text");
            content.setValue("entry" + offset);
            entry.setContents(Collections.singletonList(content));
            return entry;
        }).limit(Math.abs(to - from) + 1).collect(Collectors.toList()));
        return feed;
    }
}