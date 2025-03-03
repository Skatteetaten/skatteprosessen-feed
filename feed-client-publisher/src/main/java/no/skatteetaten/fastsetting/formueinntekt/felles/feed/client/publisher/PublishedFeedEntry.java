package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.publisher;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedEntry;

public class PublishedFeedEntry<PAYLOAD> implements FeedEntry<PublishedFeedLocation> {

    private final long sequence;

    private final PAYLOAD payload;

    PublishedFeedEntry(long sequence, PAYLOAD payload) {
        this.sequence = sequence;
        this.payload = payload;
    }

    @Override
    public PublishedFeedLocation getLocation() {
        return new PublishedFeedLocation(sequence, PublishedFeedLocation.Direction.BACKWARD);
    }

    public PAYLOAD getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "entry@" + sequence;
    }
}
