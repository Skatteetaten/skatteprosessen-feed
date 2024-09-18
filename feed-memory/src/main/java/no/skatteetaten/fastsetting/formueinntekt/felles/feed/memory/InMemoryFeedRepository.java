package no.skatteetaten.fastsetting.formueinntekt.felles.feed.memory;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedRepository;

public class InMemoryFeedRepository<LOCATION, POINTER> implements FeedRepository<LOCATION, POINTER, Void> {

    private final ConcurrentMap<POINTER, Map<Category, LOCATION>> pointers = new ConcurrentHashMap<>();

    @Override
    public Map<Category, LOCATION> read(POINTER pointer) {
        return Collections.unmodifiableMap(pointers.getOrDefault(pointer, Collections.emptyMap()));
    }

    @Override
    public void update(POINTER pointer, Map<Category, LOCATION> locations, Consumer<? super Void> callback) {
        pointers.put(pointer, new EnumMap<>(locations));
        callback.accept(null);
    }

    @Override
    public void delete(POINTER pointer, Consumer<? super Void> callback) {
        pointers.remove(pointer);
        callback.accept(null);
    }

    @Override
    public void transit(Void unused, POINTER pointer, Map<Category, LOCATION> from, Map<Category, LOCATION> to) {
        Map<Category, LOCATION> locations = pointers.computeIfAbsent(pointer, ignored -> Map.of());
        for (Map.Entry<Category, LOCATION> entry : from.entrySet()) {
            if (!Objects.equals(locations.get(entry.getKey()), entry.getValue())) {
                throw new IllegalArgumentException("Did not find " + entry + " in " + locations + " for " + pointer);
            }
        }
        if (locations.size() != from.size()) {
            throw new IllegalArgumentException("Expected " + from.size() + " locations but found " + locations.size() + " for " + pointer);
        }
        if (!pointers.replace(pointer, locations, new EnumMap<>(to))) {
            throw new IllegalArgumentException("Concurrent replacement of " + pointer);
        }
    }

    public Map<POINTER, Map<Category, LOCATION>> getPointers() {
        return Collections.unmodifiableMap(pointers);
    }
}
