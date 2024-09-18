package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.skifteattest;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedEntry;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.skifteattest.dto.SkifteattestHendelse;

public class SkifteattestFeedEntry<T> implements FeedEntry<Long> {
    private final SkifteattestHendelse<T> skifteattestHendelse;

    public SkifteattestFeedEntry(SkifteattestHendelse<T> skifteattestHendelse) {
        this.skifteattestHendelse = skifteattestHendelse;
    }

    @Override
    public Long getLocation() {
        return skifteattestHendelse.getSekvensnummer();
    }

    public SkifteattestHendelse<T> getPayload() {
        return skifteattestHendelse;
    }

    @Override
    public String toString() {
        return "entry@" + skifteattestHendelse.getSekvensnummer();
    }
}
