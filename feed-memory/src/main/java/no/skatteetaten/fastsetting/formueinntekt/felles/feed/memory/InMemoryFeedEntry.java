package no.skatteetaten.fastsetting.formueinntekt.felles.feed.memory;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedEntry;

public class InMemoryFeedEntry<PAYLOAD> implements FeedEntry<Integer> {

    private final int location;

    private final PAYLOAD payload;

    InMemoryFeedEntry(int location, PAYLOAD payload) {
        this.location = location;
        this.payload = payload;
    }

    @Override
    public Integer getLocation() {
        return location;
    }

    public PAYLOAD getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return location + " -> " + payload;
    }
}
