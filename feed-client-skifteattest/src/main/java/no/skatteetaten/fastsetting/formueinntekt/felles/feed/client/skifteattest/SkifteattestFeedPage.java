package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.skifteattest;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedPage;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.skifteattest.dto.SkifteattestHendelse;

public class SkifteattestFeedPage<T> implements FeedPage<Long, SkifteattestFeedEntry<T>> {
    private final List<SkifteattestHendelse<T>> skifteattestHendelses;

    public SkifteattestFeedPage(List<SkifteattestHendelse<T>> skifteattestHendelses) {
        this.skifteattestHendelses = skifteattestHendelses;
    }

    @Override
    public Long getLocation() {
        return skifteattestHendelses.get(0).getSekvensnummer() - 1;
    }

    @Override
    public Optional<Long> getPreviousLocation() {
        throw new UnsupportedOperationException("getPreviousLocation");
    }

    @Override
    public Optional<Long> getNextLocation() {
        return Optional.of(skifteattestHendelses.get(skifteattestHendelses.size() - 1).getSekvensnummer());
    }

    @Override
    public List<SkifteattestFeedEntry<T>> getEntries() {
        return skifteattestHendelses.stream().map(SkifteattestFeedEntry::new).collect(Collectors.toList());
    }

    @Override
    public boolean hasLocation(Long location) {
        return skifteattestHendelses.get(0).getSekvensnummer() <= location && location <= skifteattestHendelses.get(
            skifteattestHendelses.size() - 1).getSekvensnummer();
    }

    @Override
    public Optional<String> toDisplayString() {
        StringBuilder sb = new StringBuilder();
        skifteattestHendelses.forEach(
            hendelse -> sb.append(
                hendelse.getSekvensnummer())
                .append('\n')
                .append(hendelse.getHendelse().toString())
                .append('\n')
                .append(hendelse.getSkifteattestaggregat().toString())
                .append('\n')
                .append('\n'));
        return Optional.of(sb.toString());
    }

    @Override
    public String toString() {
        return "page@" + getLocation();
    }
}

