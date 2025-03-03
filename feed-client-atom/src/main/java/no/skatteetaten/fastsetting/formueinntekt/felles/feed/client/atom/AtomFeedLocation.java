package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom;

import java.util.Optional;

public class AtomFeedLocation {

    private final String page, entry, etag;

    public AtomFeedLocation(String page) {
        this.page = page;
        entry = null;
        etag = null;
    }

    public AtomFeedLocation(String page, String entry) {
        this.page = page;
        this.entry = entry;
        etag = null;
    }

    public AtomFeedLocation(String page, String entry, String etag) {
        this.page = page;
        this.entry = entry;
        this.etag = etag;
    }

    public String getPage() {
        return page;
    }

    public Optional<String> getLastEntry() {
        return Optional.ofNullable(entry);
    }

    public Optional<String> getEtag() {
        return Optional.ofNullable(etag);
    }

    public AtomFeedLocation withoutEtag() {
        return etag == null ? this : new AtomFeedLocation(page, entry);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        AtomFeedLocation that = (AtomFeedLocation) object;
        if (!page.equals(that.page)) {
            return false;
        }
        return entry != null ? entry.equals(that.entry) : that.entry == null;
    }

    @Override
    public int hashCode() {
        int result = page.hashCode();
        result = 31 * result + (entry != null ? entry.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return page
            + (entry == null ? "" : "/" + entry)
            + (etag == null ? "" : "[" + etag + "]");
    }
}
