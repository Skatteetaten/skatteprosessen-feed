package no.skatteetaten.fastsetting.formueinntekt.felles.feed.stream;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedEntry;

public class StreamEntry implements FeedEntry<Long> {

    private final long line;
    private final String payload;

    public StreamEntry(long line, String payload) {
        this.line = line;
        this.payload = payload;
    }

    @Override
    public Long getLocation() {
        return line;
    }

    public String getPayload() {
        return payload;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        StreamEntry streamEntry = (StreamEntry) object;
        return line == streamEntry.line;
    }

    @Override
    public int hashCode() {
        return (int) (line ^ (line >>> 32));
    }

    @Override
    public String toString() {
        return String.valueOf(line);
    }
}
