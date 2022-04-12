package no.skatteetaten.fastsetting.formueinntekt.felles.feed.api;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;

public interface FeedRepository<LOCATION, POINTER, TRANSACTION> {

    Map<Category, LOCATION> read(POINTER pointer);

    default Optional<LOCATION> readCurrent(POINTER pointer) {
        Map<Category, LOCATION> locations = read(pointer);
        if (locations.isEmpty()) {
            return Optional.empty();
        } else if (locations.containsKey(Category.LOWER) || locations.containsKey(Category.UPPER)) {
            throw new IllegalArgumentException("Found lower or upper bound for " + pointer);
        } else {
            return Optional.of(locations.get(Category.CURRENT));
        }
    }

    default void update(TRANSACTION transaction, POINTER pointer, Map<Category, LOCATION> locations) {
        update(pointer, locations);
    }

    default void updateCurrent(TRANSACTION transaction, POINTER pointer, LOCATION location) {
        update(transaction, pointer, Collections.singletonMap(Category.CURRENT, location));
    }

    default void updateAll(TRANSACTION transaction, POINTER pointer, LOCATION lower, LOCATION current, LOCATION upper) {
        Map<Category, LOCATION> locations = new EnumMap<>(Category.class);
        if (lower != null) {
            locations.put(Category.LOWER, lower);
        }
        locations.put(Category.CURRENT, current);
        if (upper != null) {
            locations.put(Category.UPPER, upper);
        }
        update(transaction, pointer, locations);
    }

    void update(POINTER pointer, Map<Category, LOCATION> locations);

    default void updateCurrent(POINTER pointer, LOCATION location) {
        update(pointer, Collections.singletonMap(Category.CURRENT, location));
    }

    default void updateAll(POINTER pointer, LOCATION lower, LOCATION current, LOCATION upper) {
        Map<Category, LOCATION> locations = new EnumMap<>(Category.class);
        if (lower != null) {
            locations.put(Category.LOWER, lower);
        }
        locations.put(Category.CURRENT, current);
        if (upper != null) {
            locations.put(Category.UPPER, upper);
        }
        update(pointer, locations);
    }

    void delete(POINTER pointer);

    interface ForwardOnly<LOCATION,
        POINTER,
        TRANSACTION extends AutoCloseable> extends FeedRepository<LOCATION, POINTER, TRANSACTION> {

        @Override
        default Map<Category, LOCATION> read(POINTER pointer) {
            return readCurrent(pointer)
                .map(location -> Collections.singletonMap(Category.CURRENT, location))
                .orElse(Collections.emptyMap());
        }

        @Override
        Optional<LOCATION> readCurrent(POINTER pointer);

        @Override
        default void update(TRANSACTION transaction, POINTER pointer, Map<Category, LOCATION> locations) {
            if (locations.isEmpty()) {
                throw new IllegalArgumentException("Cannot store empty location map");
            } else if (locations.containsKey(Category.LOWER) || locations.containsKey(Category.UPPER)) {
                throw new UnsupportedOperationException("Cannot store bounded location for " + pointer);
            } else {
                updateCurrent(transaction, pointer, locations.get(Category.CURRENT));
            }
        }

        @Override
        void updateCurrent(TRANSACTION transaction, POINTER pointer, LOCATION location);
    }

    enum Category {
        LOWER,
        CURRENT,
        UPPER
    }
}
