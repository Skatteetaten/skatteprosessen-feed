package no.skatteetaten.fastsetting.formueinntekt.felles.feed.api;

import java.util.List;
import java.util.Optional;

public interface FeedPage<LOCATION, ENTRY extends FeedEntry<LOCATION>> {

    LOCATION getLocation();

    Optional<LOCATION> getPreviousLocation();

    Optional<LOCATION> getNextLocation();

    List<ENTRY> getEntries();

    default Optional<LOCATION> getPageLocation() {
        return Optional.empty();
    }

    default boolean hasPreviousLocation() {
        return getPreviousLocation().isPresent();
    }

    default boolean hasNextLocation() {
        return getNextLocation().isPresent();
    }

    default boolean hasLocation(LOCATION location) {
        return getEntries().stream().anyMatch(entry -> entry.getLocation().equals(location));
    }

    default List<ENTRY> getEntriesAfter(LOCATION location) {
        List<ENTRY> entries = getEntries();
        int limit = 0;
        for (ENTRY entry : entries) {
            if (entry.getLocation().equals(location)) {
                break;
            } else {
                limit++;
            }
        }
        if (limit == entries.size()) {
            throw new IllegalArgumentException("Page at location " + getLocation() + " does not include " + location);
        }
        return entries.subList(limit + 1, entries.size());
    }

    default List<ENTRY> getEntriesUntil(LOCATION location) {
        List<ENTRY> entries = getEntries();
        int limit = 0;
        for (ENTRY entry : entries) {
            if (entry.getLocation().equals(location)) {
                break;
            } else {
                limit++;
            }
        }
        if (limit == entries.size()) {
            throw new IllegalArgumentException("Page at location " + getLocation() + " does not include " + location);
        }
        return entries.subList(0, limit + 1);
    }

    default List<ENTRY> getEntriesBetween(LOCATION lower, LOCATION upper) {
        List<ENTRY> entries = getEntries();
        int lowerLimit = 0, upperLimit = 0;
        for (ENTRY entry : entries) {
            if (lower.equals(entry.getLocation())) {
                break;
            } else {
                lowerLimit++;
            }
        }
        for (ENTRY entry : entries) {
            if (upper.equals(entry.getLocation())) {
                break;
            } else {
                upperLimit++;
            }
        }
        if (upperLimit == entries.size() || lowerLimit == entries.size()) {
            throw new IllegalArgumentException(
                "Page at location " + getLocation() + " does not include either of " + lower + " or " + upper
            );
        }
        return entries.subList(lowerLimit + 1, upperLimit + 1);
    }

    default Optional<String> toDisplayString() {
        return Optional.empty();
    }
}
