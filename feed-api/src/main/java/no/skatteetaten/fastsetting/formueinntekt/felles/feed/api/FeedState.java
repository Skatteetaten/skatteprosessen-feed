package no.skatteetaten.fastsetting.formueinntekt.felles.feed.api;

import java.util.Optional;

public class FeedState<POINTER> {

    private final POINTER pointer;
    private final boolean alive;
    private final Throwable lastError;

    public FeedState(POINTER pointer, boolean alive, Throwable lastError) {
        this.pointer = pointer;
        this.alive = alive;
        this.lastError = lastError;
    }

    public POINTER getPointer() {
        return pointer;
    }

    public boolean isAlive() {
        return alive;
    }

    public Optional<Throwable> getLastError() {
        return Optional.ofNullable(lastError);
    }
}
