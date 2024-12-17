package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom.hopper;

import static org.assertj.core.api.Assertions.assertThat;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.absent;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.ApacheFeedHttpClient;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom.AtomFeedContentTextResolver;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
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
import com.rometools.rome.feed.synd.SyndLink;
import com.rometools.rome.feed.synd.SyndLinkImpl;
import com.rometools.rome.io.SyndFeedOutput;

public class AtomHopperFeedEndpointTest {

    @Rule
    public WireMockRule wireMock = new WireMockRule(wireMockConfig().dynamicPort());

    private CloseableHttpClient httpClient;
    private AtomHopperFeedEndpoint<Optional<String>> endpoint;

    private SyndFeedOutput feedOutput = new SyndFeedOutput();

    @Before
    public void setUp() throws Exception {
        httpClient = HttpClients.createDefault();
        endpoint = new AtomHopperFeedEndpoint<>(
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
        stubFor(get(urlPathEqualTo("/"))
            .withQueryParam("marker", absent())
            .willReturn(aResponse()
                .withStatus(200)
                .withBody(feedOutput.outputString(
                    feed("foo", null, null, "foobar", "dummy"))
                )));

        stubFor(get(urlPathEqualTo("/"))
            .withQueryParam("marker", equalTo("foobar"))
            .willReturn(aResponse()
                .withStatus(200)
                .withBody(feedOutput.outputString(
                    feed("foo", null, null, null, "bar", "qux"))
                )));

        assertThat(endpoint.getFirstPage()).hasValueSatisfying(page -> {
            assertThat(page.getLocation().getMarker()).isEqualTo("1");
            assertThat(page.getLocation().getDirection()).isEqualTo(AtomHopperFeedLocation.Direction.BACKWARD);
            assertThat(page.getPreviousLocation()).isNotPresent();
            assertThat(page.getNextLocation()).contains(new AtomHopperFeedLocation(
                "1", AtomHopperFeedLocation.Direction.FORWARD
            ));
            assertThat(page.getEntries()).hasSize(2);
            assertThat(page.getEntries().get(0).getLocation().getMarker()).isEqualTo("0");
            assertThat(page.getEntries().get(0).getLocation().getDirection())
                .isEqualTo(AtomHopperFeedLocation.Direction.BACKWARD);
            assertThat(page.getEntries().get(0).getPayload()).contains("bar");
            assertThat(page.getEntries().get(1).getLocation().getMarker()).isEqualTo("1");
            assertThat(page.getEntries().get(1).getLocation().getDirection())
                .isEqualTo(AtomHopperFeedLocation.Direction.BACKWARD);
            assertThat(page.getEntries().get(1).getPayload()).contains("qux");
        });
    }

    @Test
    public void can_read_last_page() throws Exception {
        stubFor(get(urlPathEqualTo("/"))
            .withQueryParam("marker", absent())
            .willReturn(aResponse()
                .withStatus(200)
                .withBody(feedOutput.outputString(
                    feed("foo", null, null, null, "bar", "qux"))
                )));

        assertThat(endpoint.getLastPage()).hasValueSatisfying(page -> {
            assertThat(page.getLocation().getMarker()).isEqualTo("1");
            assertThat(page.getLocation().getDirection()).isEqualTo(AtomHopperFeedLocation.Direction.BACKWARD);
            assertThat(page.getPreviousLocation()).isNotPresent();
            assertThat(page.getNextLocation()).contains(new AtomHopperFeedLocation(
                "1", AtomHopperFeedLocation.Direction.FORWARD
            ));
            assertThat(page.getEntries()).hasSize(2);
            assertThat(page.getEntries().get(0).getLocation().getMarker()).isEqualTo("0");
            assertThat(page.getEntries().get(0).getLocation().getDirection())
                .isEqualTo(AtomHopperFeedLocation.Direction.BACKWARD);
            assertThat(page.getEntries().get(0).getPayload()).contains("bar");
            assertThat(page.getEntries().get(1).getLocation().getMarker()).isEqualTo("1");
            assertThat(page.getEntries().get(1).getLocation().getDirection())
                .isEqualTo(AtomHopperFeedLocation.Direction.BACKWARD);
            assertThat(page.getEntries().get(1).getPayload()).contains("qux");
        });
    }

    @Test
    public void can_read_page() throws Exception {
        stubFor(get(urlPathEqualTo("/"))
            .withQueryParam("marker", equalTo("foobar"))
            .willReturn(aResponse()
                .withStatus(200)
                .withBody(feedOutput.outputString(
                    feed("foo", "quxbaz", "bazqux", null, "bar", "qux"))
                )));

        assertThat(endpoint.getPage(new AtomHopperFeedLocation(
            "foobar", AtomHopperFeedLocation.Direction.FORWARD
        ))).hasValueSatisfying(page -> {
            assertThat(page.getLocation().getMarker()).isEqualTo("1");
            assertThat(page.getLocation().getDirection()).isEqualTo(AtomHopperFeedLocation.Direction.FORWARD);
            assertThat(page.getPreviousLocation()).contains(new AtomHopperFeedLocation(
                "quxbaz", AtomHopperFeedLocation.Direction.BACKWARD
            ));
            assertThat(page.getNextLocation()).contains(new AtomHopperFeedLocation(
                "1", AtomHopperFeedLocation.Direction.FORWARD
            ));
            assertThat(page.getEntries()).hasSize(2);
            assertThat(page.getEntries().get(0).getLocation().getMarker()).isEqualTo("0");
            assertThat(page.getEntries().get(0).getLocation().getDirection())
                .isEqualTo(AtomHopperFeedLocation.Direction.FORWARD);
            assertThat(page.getEntries().get(0).getPayload()).contains("bar");
            assertThat(page.getEntries().get(1).getLocation().getMarker()).isEqualTo("1");
            assertThat(page.getEntries().get(1).getLocation().getDirection())
                .isEqualTo(AtomHopperFeedLocation.Direction.FORWARD);
            assertThat(page.getEntries().get(1).getPayload()).contains("qux");
        });
    }

    private static SyndFeed feed(String current, String previous, String next, String last, String... documents) {
        SyndFeed feed = new SyndFeedImpl();
        feed.setFeedType("atom_1.0");
        feed.setUri(current);
        List<SyndLink> links = new ArrayList<>();
        if (previous != null) {
            SyndLink link = new SyndLinkImpl();
            link.setHref("http://localhost?marker=" + previous);
            link.setRel("next");
            links.add(link);
        }
        if (next != null) {
            SyndLink link = new SyndLinkImpl();
            link.setHref("http://localhost?marker=" + next);
            link.setRel("previous");
            links.add(link);
        }
        if (last != null) {
            SyndLink link = new SyndLinkImpl();
            link.setHref("http://localhost?marker=" + last);
            link.setRel("last");
            links.add(link);
        }
        feed.setLinks(links);
        feed.setEntries(IntStream.range(0, documents.length).mapToObj(index -> {
            SyndEntry entry = new SyndEntryImpl();
            entry.setUri(String.valueOf(documents.length - index - 1));
            SyndContent content = new SyndContentImpl();
            content.setType("plain/text");
            content.setValue(documents[documents.length - index - 1]);
            entry.setContents(Collections.singletonList(content));
            return entry;
        }).collect(Collectors.toList()));
        return feed;
    }
}