package no.skatteetaten.fastsetting.formueinntekt.felles.feed.api;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

public interface FeedRepository<LOCATION, POINTER, TRANSACTION> {

    Map<Category, LOCATION> read(POINTER pointer);

    default Optional<LOCATION> readCurrent(POINTER pointer) {
        Map<Category, LOCATION> locations = read(pointer);
        if (locations.containsKey(Category.LOWER) || locations.containsKey(Category.UPPER)) {
            throw new IllegalArgumentException("Found lower or upper bound for " + pointer);
        } else {
            return Optional.ofNullable(locations.get(Category.CURRENT));
        }
    }

    void update(POINTER pointer, Map<Category, LOCATION> locations, Consumer<? super TRANSACTION> callback);

    default void updateCurrent(POINTER pointer, LOCATION location, Consumer<? super TRANSACTION> callback) {
        update(pointer, Map.of(Category.CURRENT, location), callback);
    }

    default void updateAll(POINTER pointer, LOCATION lower, LOCATION current, LOCATION upper, Consumer<? super TRANSACTION> callback) {
        Map<Category, LOCATION> locations = new EnumMap<>(Map.of(Category.CURRENT, current));
        if (lower != null) {
            locations.put(Category.LOWER, lower);
        }
        if (upper != null) {
            locations.put(Category.UPPER, upper);
        }
        update(pointer, locations, callback);
    }

    void delete(POINTER pointer, Consumer<? super TRANSACTION> callback);

    void transit(TRANSACTION transaction, POINTER pointer, Map<Category, LOCATION> from, Map<Category, LOCATION> to);

    default void transitCurrent(TRANSACTION transaction, POINTER pointer, LOCATION from, LOCATION to) {
        transit(transaction, pointer, from == null ? Map.of() : Map.of(Category.CURRENT, from), Map.of(Category.CURRENT, to));
    }

    default Map<Category, LOCATION> transitAll(
        TRANSACTION transaction, POINTER pointer,
        LOCATION fromLower, LOCATION fromCurrent, LOCATION fromUpper,
        LOCATION toLower, LOCATION toCurrent, LOCATION toUpper
    ) {
        Map<Category, LOCATION> from = new EnumMap<>(Category.class), to = new EnumMap<>(Map.of(Category.CURRENT, toCurrent));
        if (fromLower != null) {
            from.put(Category.LOWER, fromLower);
        }
        if (fromCurrent != null) {
            from.put(Category.CURRENT, fromCurrent);
        }
        if (fromUpper != null) {
            from.put(Category.UPPER, fromUpper);
        }
        if (toLower != null) {
            to.put(Category.LOWER, toLower);
        }
        if (toUpper != null) {
            to.put(Category.UPPER, toUpper);
        }
        transit(transaction, pointer, from, to);
        return to;
    }

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
        default void update(POINTER pointer, Map<Category, LOCATION> locations, Consumer<? super TRANSACTION> callback) {
            if (locations.containsKey(Category.LOWER) || locations.containsKey(Category.UPPER)) {
                throw new IllegalArgumentException("Found lower or upper bound for " + pointer);
            } else {
                updateCurrent(pointer, locations.get(Category.CURRENT), callback);
            }
        }

        @Override
        void updateCurrent(POINTER pointer, LOCATION location, Consumer<? super TRANSACTION> callback);

        @Override
        default void transit(TRANSACTION transaction, POINTER pointer, Map<Category, LOCATION> from, Map<Category, LOCATION> to) {
            if (from.containsKey(Category.LOWER) || from.containsKey(Category.UPPER)) {
                throw new IllegalArgumentException("Found lower or upper bound in source location for " + pointer);
            } else if (to.containsKey(Category.LOWER) || to.containsKey(Category.UPPER)) {
                throw new IllegalArgumentException("Found lower or upper bound in target location for " + pointer);
            } else {
                transitCurrent(transaction, pointer, from.get(Category.CURRENT), to.get(Category.CURRENT));
            }
        }

        @Override
        void transitCurrent(TRANSACTION transaction, POINTER pointer, LOCATION from, LOCATION to);
    }

    enum Category {
        LOWER,
        CURRENT,
        UPPER
    }
}
