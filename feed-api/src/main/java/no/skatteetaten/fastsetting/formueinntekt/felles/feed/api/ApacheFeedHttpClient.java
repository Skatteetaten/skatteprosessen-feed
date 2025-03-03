package no.skatteetaten.fastsetting.formueinntekt.felles.feed.api;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;

public class ApacheFeedHttpClient implements FeedHttpClient {

    private static final Pattern PATTERN = Pattern.compile("([ ]*)(\\{([\\w.]+)})");

    private final CloseableHttpClient httpClient;

    public ApacheFeedHttpClient(CloseableHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public <RESULT> RESULT get(Request request, String type, Map<String, String> headers, RequestMapper<RESULT> mapper) throws IOException {
        HttpGet get;
        try {
            URIBuilder builder = new URIBuilder(request.getEndpoint().toURI());
            Matcher matcher = PATTERN.matcher(request.getTemplate().startsWith("/")
                ? request.getTemplate().substring(1)
                : request.getTemplate());
            StringBuilder path = new StringBuilder();
            if (builder.getPath() != null && !builder.getPath().isEmpty()) {
                path.append(builder.getPath());
                if (!builder.getPath().endsWith("/") && !request.getTemplate().isEmpty()) {
                    path.append("/");
                }
            }
            while (matcher.find()) {
                String replacement = request.getSubstitutions().get(matcher.group(3));
                if (replacement == null) {
                    throw new IllegalArgumentException("Unknown variable: " + matcher.group(3));
                } else if (!replacement.isEmpty()) {
                    matcher.appendReplacement(path, matcher.group(1) + replacement
                        .replace("\\", "\\\\")
                        .replace("\n", "\n" + matcher.group(1)));
                } else {
                    matcher.appendReplacement(path, "");
                }
            }
            matcher.appendTail(path);
            builder.setPath(path.toString());
            for (Map.Entry<String, ?> entry : request.getQuery().entrySet()) {
                builder.addParameter(entry.getKey(), entry.getValue().toString());
            }
            get = new HttpGet(builder.build());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
        get.setHeader(HttpHeaders.ACCEPT, type);
        headers.forEach(get::setHeader);
        return httpClient.execute(get, response -> mapper.apply(new ApacheResponse(response)));
    }

    CloseableHttpClient getHttpClient() {
        return httpClient;
    }

    static class ApacheResponse implements Response {

        private final HttpResponse response;

        ApacheResponse(HttpResponse response) {
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
    }
}
