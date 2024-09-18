package no.skatteetaten.fastsetting.formueinntekt.felles.feed.api;

import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public enum FeedDirection {

    FORWARD,
    BACKWARD,
    RECOVERY;

    public boolean isBackward() {
        return this != FORWARD;
    }

    public <ENTRY> Iterable<ENTRY> iteration(List<ENTRY> entries) {
        if (this == FORWARD) {
            return entries;
        } else {
            return () -> {
                ListIterator<ENTRY> iterator = entries.listIterator(entries.size());
                return new Iterator<>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasPrevious();
                    }

                    @Override
                    public ENTRY next() {
                        return iterator.previous();
                    }

                    @Override
                    public void remove() {
                        iterator.remove();
                    }
                };
            };
        }
    }
}
