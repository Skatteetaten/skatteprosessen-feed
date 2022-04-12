package no.skatteetaten.fastsetting.formueinntekt.felles.feed.reader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedDirection;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedEndpoint;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedEntry;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedRepository;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedConsumer;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedPage;

public enum FeedReader {

    FORWARD {
        @Override
        <LOCATION,
            ENTRY extends FeedEntry<LOCATION>,
            PAGE extends FeedPage<LOCATION, ENTRY>,
            POINTER,
            TRANSACTION>
        Optional<LOCATION> apply(
            FeedEndpoint<LOCATION, ENTRY, PAGE> endpoint,
            FeedRepository<LOCATION, POINTER, TRANSACTION> repository,
            POINTER pointer,
            LOCATION location,
            FeedConsumer<? super LOCATION, ? super ENTRY, ? extends TRANSACTION> consumer,
            BooleanSupplier isAlive
        ) throws InterruptedException {
            return FeedReader.<LOCATION, ENTRY, PAGE, POINTER, TRANSACTION>doReadForward(endpoint, repository, location, pointer, consumer, isAlive);
        }
    },

    BACKWARD {
        @Override
        <LOCATION,
            ENTRY extends FeedEntry<LOCATION>,
            PAGE extends FeedPage<LOCATION, ENTRY>,
            POINTER,
            TRANSACTION>
        Optional<LOCATION> apply(
            FeedEndpoint<LOCATION, ENTRY, PAGE> endpoint,
            FeedRepository<LOCATION, POINTER, TRANSACTION> repository,
            POINTER pointer,
            LOCATION location,
            FeedConsumer<? super LOCATION, ? super ENTRY, ? extends TRANSACTION> consumer,
            BooleanSupplier isAlive
        ) throws InterruptedException {
            Optional<LOCATION> current;
            do {
                current = FeedReader.<LOCATION, ENTRY, PAGE, POINTER, TRANSACTION>doReadBackward(endpoint, repository, location, null, null, pointer, consumer, isAlive);
                LOCATION previous = location;
                location = current.filter(limit -> previous == null || !previous.equals(limit)).orElse(null);
            } while (location != null);
            return current;
        }
    },

    BACKWARD_INITIAL_ONLY {
        @Override
        <LOCATION,
            ENTRY extends FeedEntry<LOCATION>,
            PAGE extends FeedPage<LOCATION, ENTRY>,
            POINTER,
            TRANSACTION>
        Optional<LOCATION> apply(
            FeedEndpoint<LOCATION, ENTRY, PAGE> endpoint,
            FeedRepository<LOCATION, POINTER, TRANSACTION> repository,
            POINTER pointer,
            LOCATION location,
            FeedConsumer<? super LOCATION, ? super ENTRY, ? extends TRANSACTION> consumer,
            BooleanSupplier isAlive
        ) throws InterruptedException {
            if (location == null) {
                location = FeedReader.<LOCATION, ENTRY, PAGE, POINTER, TRANSACTION>doReadBackward(endpoint, repository, null, null, null, pointer, consumer, isAlive).orElse(null);
            }
            if (location != null) {
                return FeedReader.<LOCATION, ENTRY, PAGE, POINTER, TRANSACTION>doReadForward(endpoint, repository, location, pointer, consumer, isAlive);
            } else {
                return Optional.empty();
            }
        }
    },

    BACKWARD_ONCE_ONLY {
        @Override
        <LOCATION,
            ENTRY extends FeedEntry<LOCATION>,
            PAGE extends FeedPage<LOCATION, ENTRY>,
            POINTER,
            TRANSACTION>
        Optional<LOCATION> apply(
            FeedEndpoint<LOCATION, ENTRY, PAGE> endpoint,
            FeedRepository<LOCATION, POINTER, TRANSACTION> repository,
            POINTER pointer,
            LOCATION location,
            FeedConsumer<? super LOCATION, ? super ENTRY, ? extends TRANSACTION> consumer,
            BooleanSupplier isAlive
        ) throws InterruptedException {
            if (location == null) {
                return FeedReader.<LOCATION, ENTRY, PAGE, POINTER, TRANSACTION>doReadBackward(endpoint, repository, null, null, null, pointer, consumer, isAlive);
            } else {
                return Optional.of(location);
            }
        }
    };

    abstract <LOCATION,
        ENTRY extends FeedEntry<LOCATION>,
        PAGE extends FeedPage<LOCATION, ENTRY>,
        POINTER,
        TRANSACTION>
    Optional<LOCATION> apply(
        FeedEndpoint<LOCATION, ENTRY, PAGE> endpoint,
        FeedRepository<LOCATION, POINTER, TRANSACTION> repository,
        POINTER pointer,
        LOCATION location,
        FeedConsumer<? super LOCATION, ? super ENTRY, ? extends TRANSACTION> consumer,
        BooleanSupplier isAlive
    ) throws InterruptedException;

    <LOCATION,
        ENTRY extends FeedEntry<LOCATION>,
        PAGE extends FeedPage<LOCATION, ENTRY>,
        POINTER,
        TRANSACTION>
    void run(
        FeedEndpoint<LOCATION, ENTRY, PAGE> endpoint,
        FeedRepository<LOCATION, POINTER, TRANSACTION> repository,
        FeedContinuation continuation,
        POINTER pointer,
        long pause,
        TimeUnit timeUnit,
        Supplier<FeedConsumer<? super LOCATION, ? super ENTRY, ? extends TRANSACTION>> consumerFactory,
        Runnable onStarted,
        Predicate<Throwable> onError,
        BooleanSupplier isAlive
    ) {
        while (isAlive.getAsBoolean()) {
            try {
                FeedConsumer<? super LOCATION, ? super ENTRY, ? extends TRANSACTION> consumer = consumerFactory.get();
                try {
                    consumer.onStart();
                    onStarted.run();
                    consumer.onSuccess(this.<LOCATION, ENTRY, PAGE, POINTER, TRANSACTION>apply(
                        endpoint,
                        repository,
                        pointer,
                        continuation.<LOCATION, ENTRY, PAGE, POINTER, TRANSACTION>resume(endpoint, repository, pointer, consumer, isAlive).orElse(null),
                        consumer,
                        isAlive
                    ).orElse(null));
                    timeUnit.sleep(pause);
                } catch (InterruptedException ignored) {
                    break;
                } catch (Throwable throwable) {
                    try {
                        consumer.onError(throwable);
                    } catch (Throwable chained) {
                        throwable.addSuppressed(chained);
                    }
                    if (onError.test(throwable)) {
                        try {
                            timeUnit.sleep(pause);
                        } catch (InterruptedException ignored) {
                            break;
                        }
                    } else {
                        break;
                    }
                }
            } catch (Throwable throwable) {
                if (onError.test(throwable)) {
                    try {
                        timeUnit.sleep(pause);
                    } catch (InterruptedException ignored) {
                        break;
                    }
                } else {
                    break;
                }
            }
        }
    }

    static <LOCATION,
        ENTRY extends FeedEntry<LOCATION>,
        PAGE extends FeedPage<LOCATION, ENTRY>,
        POINTER,
        TRANSACTION>
    Optional<LOCATION> doReadForward(
        FeedEndpoint<LOCATION, ENTRY, PAGE> endpoint,
        FeedRepository<LOCATION, POINTER, TRANSACTION> repository,
        LOCATION location,
        POINTER pointer,
        FeedConsumer<? super LOCATION, ? super ENTRY, ? extends TRANSACTION> consumer,
        BooleanSupplier isAlive
    ) throws InterruptedException {
        consumer.onSeries(location, FeedDirection.FORWARD);
        return FeedIterator.FORWARD.read(endpoint, location, null, (page, origin) -> {
            consumer.onPage(
                transaction -> repository.updateCurrent(
                    transaction,
                    pointer,
                    page.getNextLocation().orElseGet(page::getLocation)
                ),
                page.getLocation(),
                FeedDirection.FORWARD,
                !page.hasNextLocation(),
                safeDownCast(location != null && page.hasLocation(location)
                    ? page.getEntriesAfter(location)
                    : page.getEntries())
            );
            return true;
        }, isAlive);
    }

    static <LOCATION,
        ENTRY extends FeedEntry<LOCATION>,
        PAGE extends FeedPage<LOCATION, ENTRY>,
        POINTER,
        TRANSACTION>
    Optional<LOCATION> doReadBackward(
        FeedEndpoint<LOCATION, ENTRY, PAGE> endpoint,
        FeedRepository<LOCATION, POINTER, TRANSACTION> repository,
        LOCATION lower,
        LOCATION continuation,
        LOCATION upper,
        POINTER pointer,
        FeedConsumer<? super LOCATION, ? super ENTRY, ? extends TRANSACTION> consumer,
        BooleanSupplier isAlive
    ) throws InterruptedException {
        class State {
            private boolean recovery = continuation != null;
        }
        State state = new State();
        consumer.onSeries(upper, state.recovery ? FeedDirection.RECOVERY : FeedDirection.BACKWARD);
        return FeedIterator.BACKWARD.read(endpoint, upper, lower, (page, origin) -> {
            LOCATION appliedUpper;
            if (continuation != null && page.getPageLocation().map(continuation::equals).orElse(false)) {
                state.recovery = false;
                appliedUpper = null;
                consumer.onSeries(upper, FeedDirection.BACKWARD);
            } else if (continuation != null && page.hasLocation(continuation)) {
                consumer.onPage(
                    transaction -> { },
                    page.getLocation(),
                    FeedDirection.RECOVERY,
                    false,
                    safeDownCast(upper != null && page.hasLocation(upper)
                        ? page.getEntriesBetween(continuation, upper)
                        : page.getEntriesAfter(continuation))
                );
                state.recovery = false;
                appliedUpper = continuation;
                consumer.onSeries(upper, FeedDirection.BACKWARD);
            } else if (upper != null && page.hasLocation(upper)) {
                appliedUpper = upper;
            } else {
                appliedUpper = null;
            }
            boolean completed;
            List<ENTRY> entries;
            if (lower != null && page.getPageLocation().map(lower::equals).orElse(false)) {
                completed = true;
                entries = page.getEntries();
            } else if (lower != null && page.hasLocation(lower)) {
                completed = true;
                entries = appliedUpper == null
                    ? page.getEntriesAfter(lower)
                    : page.getEntriesBetween(lower, appliedUpper);
            } else {
                completed = lower == null
                    ? !page.hasPreviousLocation()
                    : page.getPreviousLocation().map(lower::equals).orElse(true);
                entries = appliedUpper == null ? page.getEntries() : page.getEntriesUntil(appliedUpper);
            }
            boolean recovery = state.recovery;
            consumer.onPage(
                transaction -> {
                    if (completed) {
                        repository.updateCurrent(transaction, pointer, upper == null ? origin : upper);
                    } else if (!recovery) {
                        repository.updateAll(
                            transaction,
                            pointer,
                            lower,
                            page.getPreviousLocation().orElse(lower),
                            upper == null ? origin : upper
                        );
                    }
                },
                page.getLocation(),
                recovery ? FeedDirection.RECOVERY : FeedDirection.BACKWARD,
                completed,
                safeDownCast(entries)
            );
            return !completed;
        }, isAlive).map(location -> upper == null ? location : upper);
    }

    @SuppressWarnings("unchecked")
    private static <ENTRY> List<ENTRY> safeDownCast(List<? extends ENTRY> entries) {
        return (List<ENTRY>) entries;
    }

    private static <ENTRY> List<ENTRY> reverse(List<? extends ENTRY> entries) {
        List<ENTRY> reversed = new ArrayList<>(entries);
        Collections.reverse(reversed);
        return reversed;
    }
}
