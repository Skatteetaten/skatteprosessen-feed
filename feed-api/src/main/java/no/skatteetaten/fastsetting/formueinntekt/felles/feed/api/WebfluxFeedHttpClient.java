package no.skatteetaten.fastsetting.formueinntekt.felles.feed.api;

import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class WebfluxFeedHttpClient implements FeedHttpClient {

    private final WebClient webClient;

    public WebfluxFeedHttpClient(WebClient webClient) {
        this.webClient = webClient;
    }

    @Override
    public <RESULT> RESULT get(Request request, String type, Map<String, String> headers, RequestMapper<RESULT> mapper) throws IOException {
        WebClient.RequestHeadersSpec<?> spec = webClient.get().uri(builder -> {
            StringBuilder path = new StringBuilder();
            if (request.getEndpoint().getPath() != null) {
                path.append(request.getEndpoint().getPath());
                if (!request.getEndpoint().getPath().endsWith("/") && !request.getTemplate().isEmpty()) {
                    path.append("/");
                }
            }
            Map<String, Object> substitutions = new HashMap<>(request.getSubstitutions());
            builder.host(request.getEndpoint().getHost())
                .port(request.getEndpoint().getPort())
                .scheme(request.getEndpoint().getProtocol())
                .path(path.append(request.getTemplate().startsWith("/")
                    ? request.getTemplate().substring(1)
                    : request.getTemplate()).toString());
            if (request.getEndpoint().getQuery() != null) {
                for (String query : request.getEndpoint().getQuery().split("&")) {
                    String[] arguments = query.split("=", 1);
                    builder.queryParam(arguments[0], arguments.length == 1 ? "" : arguments[1]);
                }
            }
            for (Map.Entry<String, ?> entry : request.getQuery().entrySet()) {
                int index = 0;
                String resolved = entry.getKey();
                while (substitutions.putIfAbsent(resolved, entry.getValue()) != null) {
                    resolved = entry.getKey() + "_" + ++index;
                }
                builder.queryParam(entry.getKey(), "{" + resolved + "}");
            }
            return builder.build(substitutions);
        });
        spec.header(HttpHeaders.ACCEPT, type);
        headers.forEach(spec::header);
        return mapper.apply(spec.retrieve().toEntity(byte[].class)
            .map(response -> (Response) new WebClientResponse(response))
            .onErrorResume(WebClientResponseException.class, exception -> Mono.just(new ErrorResponse(exception)))
            .block());
    }

    private static class WebClientResponse implements Response {

        private final ResponseEntity<byte[]> response;

        private WebClientResponse(ResponseEntity<byte[]> response) {
            this.response = response;
        }

        @Override
        public int getStatus() {
            return response.getStatusCode().value();
        }

        @Override
        public InputStream getContent() {
            return response.getBody() == null
                ? new ByteArrayInputStream(new byte[0])
                : new ByteArrayInputStream(response.getBody());
        }

        @Override
        public Optional<String> getHeader(String name) {
            return Optional.ofNullable(response.getHeaders().getFirst(name));
        }
    }

    private static class ErrorResponse implements Response {

        private final WebClientResponseException exception;

        public ErrorResponse(WebClientResponseException exception) {
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
            return Optional.ofNullable(exception.getHeaders().getFirst(name));
        }
    }
}
