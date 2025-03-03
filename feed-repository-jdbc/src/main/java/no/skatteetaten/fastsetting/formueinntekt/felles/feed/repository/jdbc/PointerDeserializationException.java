package no.skatteetaten.fastsetting.formueinntekt.felles.feed.repository.jdbc;

import java.util.Map;

public class PointerDeserializationException extends IllegalArgumentException {

    private final Map<String, String> pointer;

    public PointerDeserializationException(Map<String, String> pointer, Throwable cause) {
        super("Failed to deserialize " + pointer, cause);
        this.pointer = pointer;
    }

    public Map<String, String> getPointer() {
        return pointer;
    }
}
