package no.skatteetaten.fastsetting.formueinntekt.felles.feed.reader;

import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.*;

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
            FeedTransactor transactor,
            POINTER pointer,
            LOCATION location,
            FeedConsumer<? super LOCATION, ? super ENTRY, ? extends TRANSACTION> consumer,
            BooleanSupplier isAlive
        ) throws InterruptedException {
            return FeedReader.<LOCATION, ENTRY, PAGE, POINTER, TRANSACTION>doReadForward(
                endpoint,
                repository,
                transactor,
                location,
                pointer,
                consumer,
                isAlive
            );
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
            FeedTransactor transactor,
            POINTER pointer,
            LOCATION location,
            FeedConsumer<? super LOCATION, ? super ENTRY, ? extends TRANSACTION> consumer,
            BooleanSupplier isAlive
        ) throws InterruptedException {
            Optional<LOCATION> current;
            do {
                current = FeedReader.<LOCATION, ENTRY, PAGE, POINTER, TRANSACTION>doReadBackward(
                    endpoint,
                    repository,
                    transactor,
                    location,
                    null,
                    null,
                    pointer,
                    consumer,
                    isAlive,
                    location == null ? Map.of() : Map.of(FeedRepository.Category.CURRENT, location)
                );
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
            FeedTransactor transactor,
            POINTER pointer,
            LOCATION location,
            FeedConsumer<? super LOCATION, ? super ENTRY, ? extends TRANSACTION> consumer,
            BooleanSupplier isAlive
        ) throws InterruptedException {
            if (location == null) {
                location = FeedReader.<LOCATION, ENTRY, PAGE, POINTER, TRANSACTION>doReadBackward(
                    endpoint,
                    repository,
                    transactor,
                    null,
                    null,
                    null,
                    pointer,
                    consumer,
                    isAlive,
                    Map.of()
                ).orElse(null);
            }
            if (location != null) {
                return FeedReader.<LOCATION, ENTRY, PAGE, POINTER, TRANSACTION>doReadForward(
                    endpoint,
                    repository,
                    transactor,
                    location,
                    pointer,
                    consumer,
                    isAlive
                );
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
            FeedTransactor transactor,
            POINTER pointer,
            LOCATION location,
            FeedConsumer<? super LOCATION, ? super ENTRY, ? extends TRANSACTION> consumer,
            BooleanSupplier isAlive
        ) throws InterruptedException {
            if (location == null) {
                return FeedReader.<LOCATION, ENTRY, PAGE, POINTER, TRANSACTION>doReadBackward(
                    endpoint,
                    repository,
                    transactor,
                    null,
                    null,
                    null,
                    pointer,
                    consumer,
                    isAlive,
                    Map.of()
                );
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
        FeedTransactor transactor,
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
        FeedTransactor transactor,
        FeedContinuation continuation,
        POINTER pointer,
        long pause,
        TimeUnit timeUnit,
        FeedConsumer<? super LOCATION, ? super ENTRY, TRANSACTION> consumer,
        Runnable onStarted,
        Predicate<Throwable> onError,
        BooleanSupplier isAlive
    ) {
        while (isAlive.getAsBoolean()) {
            try {
                consumer.onStart();
                onStarted.run();
                consumer.onSuccess(this.<LOCATION, ENTRY, PAGE, POINTER, TRANSACTION>apply(
                    endpoint,
                    repository,
                    transactor,
                    pointer,
                    continuation.<LOCATION, ENTRY, PAGE, POINTER, TRANSACTION>resume(
                        endpoint,
                        repository,
                        transactor,
                        pointer,
                        consumer,
                        isAlive
                    ).orElse(null),
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
                try {
                    if (onError.test(throwable)) {
                        try {
                            timeUnit.sleep(pause);
                        } catch (InterruptedException ignored) {
                            break;
                        }
                    } else {
                        consumer.onFailure(throwable);
                        break;
                    }
                } catch (Throwable ignored) {
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
        FeedTransactor transactor,
        LOCATION location,
        POINTER pointer,
        FeedConsumer<? super LOCATION, ? super ENTRY, ? extends TRANSACTION> consumer,
        BooleanSupplier isAlive
    ) throws InterruptedException {
        class State {
            private LOCATION previous = location;
        }
        State state = new State();
        consumer.onSeries(location, FeedDirection.FORWARD);
        return FeedIterator.FORWARD.read(endpoint, transactor, location, null, (page, origin) -> {
            consumer.onPage(
                transaction -> {
                    LOCATION previous = state.previous;
                    repository.transitCurrent(
                        transaction,
                        pointer,
                        previous,
                        page.getNextLocation().orElseGet(page::getLocation)
                    );
                },
                page.getLocation(),
                FeedDirection.FORWARD,
                !page.hasNextLocation(),
                safeDownCast(location != null && page.hasLocation(location)
                    ? page.getEntriesAfter(location)
                    : page.getEntries())
            );
            state.previous = page.getNextLocation().orElseGet(page::getLocation);
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
        FeedTransactor transactor,
        LOCATION lower,
        LOCATION continuation,
        LOCATION upper,
        POINTER pointer,
        FeedConsumer<? super LOCATION, ? super ENTRY, ? extends TRANSACTION> consumer,
        BooleanSupplier isAlive,
        Map<FeedRepository.Category, LOCATION> locations
    ) throws InterruptedException {
        class State {
            private boolean recovery = continuation != null;
            private Map<FeedRepository.Category, LOCATION> previous = locations;
        }
        State state = new State();
        consumer.onSeries(upper, state.recovery ? FeedDirection.RECOVERY : FeedDirection.BACKWARD);
        return FeedIterator.BACKWARD.read(endpoint, transactor, upper, lower, (page, origin) -> {
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
            Map<FeedRepository.Category, LOCATION> previous = state.previous;
            consumer.onPage(
                transaction -> {
                    if (completed) {
                        repository.transit(
                            transaction,
                            pointer,
                            previous,
                            Map.of(FeedRepository.Category.CURRENT, upper == null ? origin : upper)
                        );
                    } else if (!recovery) {
                        repository.transitAll(
                            transaction,
                            pointer,
                            previous.get(FeedRepository.Category.LOWER),
                            previous.get(FeedRepository.Category.CURRENT),
                            previous.get(FeedRepository.Category.UPPER),
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
            if (completed) {
                return false;
            } else {
                state.previous = new EnumMap<>(Collections.singletonMap(FeedRepository.Category.CURRENT, page.getPreviousLocation().orElse(lower)));
                if (lower != null) {
                    state.previous.put(FeedRepository.Category.LOWER, lower);
                }
                if (upper != null || origin != null) {
                    state.previous.put(FeedRepository.Category.UPPER, upper == null ? origin : upper);
                }
                return true;
            }
        }, isAlive).map(location -> upper == null ? location : upper);
    }

    @SuppressWarnings("unchecked")
    private static <ENTRY> List<ENTRY> safeDownCast(List<? extends ENTRY> entries) {
        return (List<ENTRY>) entries;
    }
}
