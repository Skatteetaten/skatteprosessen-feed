package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.skifteattest.dto;

public class Hendelse {
    private String ajourholdstidspunkt;
    private String hendelsestype;

    public String getAjourholdstidspunkt() {
        return ajourholdstidspunkt;
    }

    public void setAjourholdstidspunkt(String ajourholdstidspunkt) {
        this.ajourholdstidspunkt = ajourholdstidspunkt;
    }

    public String getHendelsestype() {
        return hendelsestype;
    }

    public void setHendelsestype(String hendelsestype) {
        this.hendelsestype = hendelsestype;
    }

    @Override
    public String toString() {
        return "Hendelse[" +
            "ajourholdstidspunkt=" + ajourholdstidspunkt + ", " +
            "hendelsestype=" + hendelsestype + ']';
    }
}
