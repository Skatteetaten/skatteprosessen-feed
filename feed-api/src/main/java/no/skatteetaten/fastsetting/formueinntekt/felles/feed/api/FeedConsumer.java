package no.skatteetaten.fastsetting.formueinntekt.felles.feed.api;

import java.util.List;
import java.util.function.Consumer;

@FunctionalInterface
public interface FeedConsumer<LOCATION, ENTRY extends FeedEntry<LOCATION>, TRANSACTION> {

    default void onStart() { }

    @SuppressWarnings("unused")
    default void onSeries(LOCATION location, FeedDirection direction) { }

    void onPage(
        Consumer<TRANSACTION> commitment,
        LOCATION location,
        FeedDirection direction,
        boolean completed,
        List<ENTRY> entries
    );

    @SuppressWarnings("unused")
    default void onError(Throwable throwable) { }

    @SuppressWarnings("unused")
    default void onSuccess(LOCATION location) { }

    @SuppressWarnings("unused")
    default void onFailure(Throwable throwable) { }

    @SuppressWarnings("unused")
    default void onReset(TRANSACTION transaction) { }

    @SuppressWarnings("unused")
    default void onComplete(TRANSACTION transaction, LOCATION location) { }
}
