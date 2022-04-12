package no.skatteetaten.fastsetting.formueinntekt.felles.feed.api;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Optional;

import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;

public class ApacheFeedHttpClient implements FeedHttpClient {

    private final CloseableHttpClient httpClient;

    public ApacheFeedHttpClient(CloseableHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public Response get(Request request, Map<String, String> headers) throws IOException {
        HttpGet get = new HttpGet(request.resolve());
        headers.forEach(get::setHeader);
        return new ApacheResponse(httpClient.execute(get));
    }

    static class ApacheResponse implements Response {

        private final CloseableHttpResponse response;

        ApacheResponse(CloseableHttpResponse response) {
            this.response = response;
        }

        @Override
        public int getStatus() {
            return response.getStatusLine().getStatusCode();
        }

        @Override
        public Optional<Charset> getCharset() {
            return Optional.ofNullable(response.getEntity().getContentEncoding()).map(value -> Charset.forName(value.getValue()));
        }

        @Override
        public InputStream getContent() throws IOException {
            return response.getEntity().getContent();
        }

        @Override
        public Optional<String> getHeader(String name) {
            return Optional.ofNullable(response.getFirstHeader(name)).map(Header::getValue);
        }

        @Override
        public void close() throws IOException {
            response.close();
        }

        @Override
        public String toString() {
            return response.getStatusLine().toString();
        }
    }
}
