package no.skatteetaten.fastsetting.formueinntekt.felles.feed.api;

import java.util.Optional;

public interface FeedEndpoint<LOCATION,
    ENTRY extends FeedEntry<LOCATION>,
    PAGE extends FeedPage<LOCATION, ENTRY>> {

    Optional<PAGE> getFirstPage();

    default Optional<PAGE> getFirstPage(LOCATION boundary) {
        return getFirstPage().filter(page -> !page.getLocation().equals(boundary));
    }

    Optional<PAGE> getLastPage();

    default Optional<PAGE> getLastPage(LOCATION boundary) {
        return getLastPage().filter(page -> !page.getLocation().equals(boundary));
    }

    default Optional<LOCATION> getLastLocation() {
        return getLastPage().map(FeedPage::getLocation);
    }

    Optional<PAGE> getPage(LOCATION location);

    default Optional<PAGE> getPage(LOCATION location, LOCATION boundary) {
        return location.equals(boundary) ? Optional.empty() : getPage(location);
    }
}
