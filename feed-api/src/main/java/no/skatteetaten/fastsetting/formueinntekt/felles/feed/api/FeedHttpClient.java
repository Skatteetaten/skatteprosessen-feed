package no.skatteetaten.fastsetting.formueinntekt.felles.feed.api;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.springframework.http.HttpHeaders;

public interface FeedHttpClient {

    String ANY = "*/*",
        APPLICATION_JSON = "application/json",
        APPLICATION_XML = "application/xml",
        APPLICATION_ATOM_XML = "application/atom+xml",
        APPLICATION_OCTET_STREAM = "application/octet-stream",
        TEXT_PLAIN = "text/plain";

    default <RESULT> RESULT get(Request request, RequestMapper<RESULT> mapper) throws IOException {
        return get(request, Collections.emptyMap(), mapper);
    }

    default <RESULT> RESULT get(Request request, String type, RequestMapper<RESULT> mapper) throws IOException {
        return get(request, type, Collections.emptyMap(), mapper);
    }

    default <RESULT> RESULT get(Request request, Map<String, String> headers, RequestMapper<RESULT> mapper) throws IOException {
        return get(request, ANY, headers, mapper);
    }

    <RESULT> RESULT get(Request request, String type, Map<String, String> headers, RequestMapper<RESULT> mapper) throws IOException;

    @FunctionalInterface
    interface RequestMapper<RESULT> {

        RESULT apply(Response response) throws IOException;
    }

    class Request {

        private final URL endpoint;
        private final String template;
        private final Map<String, String> substitutions;
        private final Map<String, String> query;

        public Request(URL endpoint, String template) {
            this.endpoint = endpoint;
            this.template = template;
            this.substitutions = new HashMap<>();
            this.query = new LinkedHashMap<>();
        }

        public URL getEndpoint() {
            return endpoint;
        }

        public String getTemplate() {
            return template;
        }

        public Map<String, String> getSubstitutions() {
            return substitutions;
        }

        public Map<String, String> getQuery() {
            return query;
        }

        public Request withSubstitution(String name, Object value) {
            String string = value.toString();
            if (string.contains("/")) {
                throw new IllegalArgumentException("Variable for " + name + " contains '/': " + string);
            }
            substitutions.put(name, string);
            return this;
        }

        public Request withQuery(String key, Object value) {
            query.put(key, value.toString());
            return this;
        }

        @Override
        public String toString() {
            return "Request{" +
                "target=" + endpoint +
                ", template='" + template + '\'' +
                ", substitutions=" + substitutions +
                ", query=" + query +
                '}';
        }
    }

    interface Response {

        int getStatus();

        default Optional<Charset> getCharset() {
            return getHeader(HttpHeaders.CONTENT_ENCODING).map(Charset::forName);
        }

        InputStream getContent() throws IOException;

        Optional<String> getHeader(String name);

        default RuntimeException toException() {
            String message;
            try {
                message = new String(getContent().readAllBytes(), getCharset().orElse(StandardCharsets.UTF_8));
            } catch (Exception e) {
                message = e.getMessage();
            }
            return new RuntimeException("Unexpected response with status " + getStatus() + ": " + message);
        }
    }
}
