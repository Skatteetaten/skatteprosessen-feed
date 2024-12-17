package no.skatteetaten.fastsetting.formueinntekt.felles.feed.api;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.http.impl.client.HttpClients;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.reactive.function.client.WebClient;

@RunWith(Parameterized.class)
public class FeedHttpClientTest {

    @Rule
    public WireMockRule wireMock = new WireMockRule(wireMockConfig().dynamicPort());

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {new ApacheFeedHttpClient(HttpClients.createMinimal())},
            {new RestTemplateHttpClient(new RestTemplate())},
            {new WebfluxFeedHttpClient(WebClient.builder().build())}
        });
    }

    private final FeedHttpClient client;

    public FeedHttpClientTest(FeedHttpClient client) {
        this.client = client;
    }

    @After
    public void tearDown() throws Exception {
        if (client instanceof ApacheFeedHttpClient) {
            ((ApacheFeedHttpClient) client).getHttpClient().close();
        }
    }

    @Test
    public void can_call_endpoint() throws Exception {
        stubFor(get(urlPathEqualTo("/foo/bar"))
            .withQueryParam("qux", equalTo("baz"))
            .withHeader("a", equalTo("b"))
            .withHeader("Accept", equalTo("type"))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("header", "value")
                .withBody("body")));

        assertThat(client.<String>get(new FeedHttpClient.Request(
            new URL("http://localhost:" + wireMock.port() + "/foo"),
            "/{foo}"
        ).withSubstitution("foo", "bar").withQuery("qux", "baz"), "type", Collections.singletonMap("a", "b"), response -> {
            assertThat(response.getStatus()).isEqualTo(200);
            assertThat(response.getHeader("header")).contains("value");
            try (InputStream inputStream = response.getContent()) {
                assertThat(new String(inputStream.readAllBytes(), StandardCharsets.UTF_8)).isEqualTo("body");
            }
            return "result";
        })).isEqualTo("result");
    }
}
