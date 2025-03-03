package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom;

import static org.assertj.core.api.Assertions.assertThat;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
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

public class AtomFeedEndpointTest {

    @Rule
    public WireMockRule wireMock = new WireMockRule(wireMockConfig().dynamicPort());

    private CloseableHttpClient httpClient;
    private AtomFeedEndpoint<Optional<String>> endpoint;

    private SyndFeedOutput feedOutput = new SyndFeedOutput();

    @Before
    public void setUp() throws Exception {
        httpClient = HttpClients.createDefault();
        endpoint = new AtomFeedEndpoint<>(
            new URL("http://localhost:" + wireMock.port()),
            new ApacheFeedHttpClient(httpClient),
            false, new AtomFeedContentTextResolver()
        );
    }

    @After
    public void tearDown() throws Exception {
        httpClient.close();
    }

    @Test
    public void can_read_first_page() throws Exception {
        stubFor(get(urlPathEqualTo("/first"))
            .willReturn(aResponse()
                .withStatus(200)
                .withBody(feedOutput.outputString(
                    feed("foo", null, null, "bar", "qux"))
                )));

        assertThat(endpoint.getFirstPage()).hasValueSatisfying(page -> {
            assertThat(page.getLocation().getPage()).isEqualTo("foo");
            assertThat(page.getLocation().getLastEntry()).contains("0");
            assertThat(page.getPreviousLocation()).isNotPresent();
            assertThat(page.getNextLocation()).isNotPresent();
            assertThat(page.getEntries()).hasSize(2);
            assertThat(page.getEntries().get(0).getLocation().getPage()).isEqualTo("foo");
            assertThat(page.getEntries().get(0).getLocation().getLastEntry()).contains("1");
            assertThat(page.getEntries().get(0).getPayload()).contains("qux");
            assertThat(page.getEntries().get(1).getLocation().getPage()).isEqualTo("foo");
            assertThat(page.getEntries().get(1).getLocation().getLastEntry()).contains("0");
            assertThat(page.getEntries().get(1).getPayload()).contains("bar");
        });
    }

    @Test
    public void can_read_last_page() throws Exception {
        stubFor(get(urlPathEqualTo("/"))
            .willReturn(aResponse()
                .withStatus(200)
                .withBody(feedOutput.outputString(
                    feed("foo", null, null, "bar", "qux"))
                )));

        assertThat(endpoint.getLastPage()).hasValueSatisfying(page -> {
            assertThat(page.getLocation().getPage()).isEqualTo("foo");
            assertThat(page.getLocation().getLastEntry()).contains("0");
            assertThat(page.getPreviousLocation()).isNotPresent();
            assertThat(page.getNextLocation()).isNotPresent();
            assertThat(page.getEntries()).hasSize(2);
            assertThat(page.getEntries().get(0).getLocation().getPage()).isEqualTo("foo");
            assertThat(page.getEntries().get(0).getLocation().getLastEntry()).contains("1");
            assertThat(page.getEntries().get(0).getPayload()).contains("qux");
            assertThat(page.getEntries().get(1).getLocation().getPage()).isEqualTo("foo");
            assertThat(page.getEntries().get(1).getLocation().getLastEntry()).contains("0");
            assertThat(page.getEntries().get(1).getPayload()).contains("bar");
        });
    }

    @Test
    public void can_read_page() throws Exception {
        stubFor(get(urlPathEqualTo("/baz"))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Etag", "foobar")
                .withBody(feedOutput.outputString(
                    feed("foo", "foobar", "quxbaz", "bar", "qux"))
                )));

        assertThat(endpoint.getPage(new AtomFeedLocation("baz"))).hasValueSatisfying(page -> {
            assertThat(page.getLocation().getPage()).isEqualTo("foo");
            assertThat(page.getLocation().getLastEntry()).contains("0");
            assertThat(page.getLocation().getEtag()).contains("foobar--gzip");
            assertThat(page.getPreviousLocation()).hasValueSatisfying(previous -> {
                assertThat(previous.getPage()).isEqualTo("foobar");
                assertThat(previous.getLastEntry()).isNotPresent();
            });
            assertThat(page.getNextLocation()).hasValueSatisfying(next -> {
                assertThat(next.getPage()).isEqualTo("quxbaz");
                assertThat(next.getLastEntry()).isNotPresent();
            });
            assertThat(page.getEntries()).hasSize(2);
            assertThat(page.getEntries().get(0).getLocation().getPage()).isEqualTo("foo");
            assertThat(page.getEntries().get(0).getLocation().getLastEntry()).contains("1");
            assertThat(page.getEntries().get(0).getPayload()).contains("qux");
            assertThat(page.getEntries().get(1).getLocation().getPage()).isEqualTo("foo");
            assertThat(page.getEntries().get(1).getLocation().getLastEntry()).contains("0");
            assertThat(page.getEntries().get(1).getPayload()).contains("bar");
        });
    }

    @Test
    public void can_detect_etag() {
        stubFor(get(urlPathEqualTo("/baz"))
            .withHeader("If-None-Match", equalTo("foobar"))
            .willReturn(aResponse()
                .withStatus(304)));

        assertThat(endpoint.getPage(new AtomFeedLocation("baz", null, "foobar"))).isNotPresent();
    }

    private static SyndFeed feed(String current, String previous, String next, String... documents) {
        SyndFeed feed = new SyndFeedImpl();
        feed.setFeedType("atom_1.0");
        feed.setUri(current);
        List<SyndLink> links = new ArrayList<>();
        if (previous != null) {
            SyndLink link = new SyndLinkImpl();
            link.setHref(previous);
            link.setRel("previous-archive");
            links.add(link);
        }
        if (next != null) {
            SyndLink link = new SyndLinkImpl();
            link.setHref(next);
            link.setRel("next-archive");
            links.add(link);
        }
        feed.setLinks(links);
        feed.setEntries(IntStream.range(0, documents.length).mapToObj(index -> {
            SyndEntry entry = new SyndEntryImpl();
            entry.setUri(String.valueOf(index));
            SyndContent content = new SyndContentImpl();
            content.setType("plain/text");
            content.setValue(documents[index]);
            entry.setContents(Collections.singletonList(content));
            return entry;
        }).collect(Collectors.toList()));
        return feed;
    }
}