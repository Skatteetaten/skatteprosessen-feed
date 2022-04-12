package no.skatteetaten.fastsetting.formueinntekt.felles.feed.memory;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedRepository;

public class InMemoryFeedRepository<LOCATION, POINTER> implements FeedRepository<LOCATION, POINTER, Void> {

    private final ConcurrentMap<POINTER, Map<Category, LOCATION>> pointers = new ConcurrentHashMap<>();

    @Override
    public Map<Category, LOCATION> read(POINTER pointer) {
        return Collections.unmodifiableMap(pointers.getOrDefault(pointer, Collections.emptyMap()));
    }

    @Override
    public void update(POINTER pointer, Map<Category, LOCATION> locations) {
        pointers.put(pointer, new EnumMap<>(locations));
    }

    @Override
    public void delete(POINTER pointer) {
        pointers.remove(pointer);
    }

    public Map<POINTER, Map<Category, LOCATION>> getPointers() {
        return Collections.unmodifiableMap(pointers);
    }
}
