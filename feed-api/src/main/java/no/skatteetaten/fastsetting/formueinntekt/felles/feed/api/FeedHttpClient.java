package no.skatteetaten.fastsetting.formueinntekt.felles.feed.api;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public interface FeedHttpClient {

    default Response get(Request request) throws IOException {
        return get(request, Collections.emptyMap());
    };

    Response get(Request request, Map<String, String> headers) throws IOException;

    class Request {

        private static final Pattern PATTERN = Pattern.compile("([ ]*)(\\{([\\w.]+)})");

        private final URL target;
        private final String template;
        private final Map<String, String> substitutions;
        private final Map<String, String> query;

        public Request(URL target, String template) {
            this.target = target;
            this.template = template;
            this.substitutions = new HashMap<>();
            this.query = new LinkedHashMap<>();
        }

        public URL getTarget() {
            return target;
        }

        public String getTemplate() {
            return template;
        }

        public Map<String, ?> getSubstitutions() {
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

        public URI resolve() {
            try {
                StringBuilder path = new StringBuilder();
                if (target.getPath() != null) {
                    path.append(target.getPath());
                    if (!target.getPath().endsWith("/") && !this.template.isEmpty()) {
                        path.append("/");
                    }
                }
                Matcher matcher = PATTERN.matcher(template.startsWith("/") ? template.substring(1) : template);
                while (matcher.find()) {
                    String replacement = substitutions.get(matcher.group(3));
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
                boolean first = true;
                StringBuilder query = new StringBuilder();
                if (target.getQuery() != null && !target.getQuery().isEmpty()) {
                    query.append(target.getQuery());
                    first = false;
                }
                for (Map.Entry<String, String> entry : this.query.entrySet()) {
                    if (first) {
                        first = false;
                    } else {
                        query.append("&");
                    }
                    query.append(URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8));
                    query.append("=");
                    query.append(URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8));
                }
                return new URI(target.getProtocol(),
                    null,
                    target.getHost(),
                    target.getPort(),
                    path.toString(),
                    first ? null : query.toString(),
                    null);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to concatenate URI " + target + " with " + template);
            }
        }

        @Override
        public String toString() {
            return "Request{" +
                "target=" + target +
                ", template='" + template + '\'' +
                ", substitutions=" + substitutions +
                ", query=" + query +
                '}';
        }
    }

    interface Response extends Closeable {

        int getStatus();

        Optional<Charset> getCharset();

        InputStream getContent() throws IOException;

        Optional<String> getHeader(String name);
    }
}
