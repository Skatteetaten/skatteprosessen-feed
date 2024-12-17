package no.skatteetaten.fastsetting.formueinntekt.felles.feed.api;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

public class RestTemplateHttpClient implements FeedHttpClient {

    private final RestTemplate template;

    public RestTemplateHttpClient(RestTemplate template) {
        this.template = template;
    }

    @Override
    public <RESULT> RESULT get(Request request, String type, Map<String, String> headers, RequestMapper<RESULT> mapper) throws IOException {
        Map<String, Object> substitutions = new HashMap<>(request.getSubstitutions());
        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(request.getEndpoint().toString()).path(
            (request.getTemplate().startsWith("/") ? "" : "/") + request.getTemplate()
        );
        request.getQuery().forEach((name, value) -> {
            int index = 0;
            String resolved = name;
            while (substitutions.putIfAbsent(resolved, value) != null) {
                resolved = name + "_" + ++index;
            }
            builder.queryParam(name, "{" + resolved + "}");
        });
        try {
            return template.execute(
                builder.encode().toUriString(),
                HttpMethod.GET,
                target -> {
                    target.getHeaders().add(HttpHeaders.ACCEPT, type);
                    headers.forEach((name, value) -> target.getHeaders().add(name, value));
                },
                response -> mapper.apply(new RestTemplateResponse(response.getStatusCode().value(), response)),
                substitutions
            );
        } catch (HttpClientErrorException exception) {
            return mapper.apply(new ErrorResponse(exception));
        }
    }

    private static class RestTemplateResponse implements Response {

        private final int status;
        private final ClientHttpResponse response;

        private RestTemplateResponse(int status, ClientHttpResponse response) {
            this.status = status;
            this.response = response;
        }

        @Override
        public int getStatus() {
            return status;
        }

        @Override
        public InputStream getContent() throws IOException {
            return response.getBody();
        }

        @Override
        public Optional<String> getHeader(String name) {
            return Optional.ofNullable(response.getHeaders().getFirst(name));
        }
    }

    private static class ErrorResponse implements Response {

        private final HttpClientErrorException exception;

        public ErrorResponse(HttpClientErrorException exception) {
            this.exception = exception;
        }

        @Override
        public int getStatus() {
            return exception.getStatusCode().value();
        }

        @Override
        public InputStream getContent() {
            return new ByteArrayInputStream(exception.getResponseBodyAsByteArray());
        }

        @Override
        public Optional<String> getHeader(String name) {
            HttpHeaders headers = exception.getResponseHeaders();
            return headers == null ? Optional.empty() : Optional.ofNullable(headers.getFirst(name));
        }
    }
}
