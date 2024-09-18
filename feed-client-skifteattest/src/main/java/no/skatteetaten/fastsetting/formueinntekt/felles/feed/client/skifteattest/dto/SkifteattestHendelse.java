package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.skifteattest.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SkifteattestHendelse<T> {
    private final long sekvensnummer;
    private final Hendelse hendelse;
    private final T skifteattestaggregat;

    public SkifteattestHendelse(
        @JsonProperty("sekvensnummer") long sekvensnummer,
        @JsonProperty("hendelse") Hendelse hendelse,
        @JsonProperty("skifteattestaggregat") T skifteattestaggregat) {
        this.sekvensnummer = sekvensnummer;
        this.hendelse = hendelse;
        this.skifteattestaggregat = skifteattestaggregat;
    }

    public long getSekvensnummer() {
        return sekvensnummer;
    }

    public Hendelse getHendelse() {
        return hendelse;
    }

    public T getSkifteattestaggregat() {
        return skifteattestaggregat;
    }

    @Override
    public String toString() {
        return "SkifteattestFeedDto{" +
            "sekvensnummer=" + sekvensnummer +
            ", hendelse=" + hendelse +
            ", skifteattestaggregat=" + skifteattestaggregat +
            '}';
    }
}


