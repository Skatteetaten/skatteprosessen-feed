package no.skatteetaten.fastsetting.formueinntekt.felles.feed.reader;

import java.util.Map;
import java.util.Optional;
import java.util.function.BooleanSupplier;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.*;

public enum FeedContinuation {

    REPEATING {
        @Override
        <LOCATION,
            ENTRY extends FeedEntry<LOCATION>,
            PAGE extends FeedPage<LOCATION, ENTRY>,
            POINTER,
            TRANSACTION>
        Optional<LOCATION> resume(
            FeedEndpoint<LOCATION, ENTRY, PAGE> endpoint,
            FeedRepository<LOCATION, POINTER, TRANSACTION> repository,
            FeedTransactor transactor,
            POINTER pointer,
            FeedConsumer<? super LOCATION, ? super ENTRY, ? extends TRANSACTION> consumer,
            BooleanSupplier isAlive
        ) throws InterruptedException {
            Map<FeedRepository.Category, LOCATION> locations = transactor.apply(
                FeedTransactor.Type.INITIALIZE,
                () -> repository.read(pointer)
            );
            if (locations.isEmpty()) {
                return Optional.empty();
            } else if (locations.containsKey(FeedRepository.Category.UPPER)) {
                return FeedReader.<LOCATION, ENTRY, PAGE, POINTER, TRANSACTION>doReadBackward(
                    endpoint,
                    repository,
                    transactor,
                    locations.get(FeedRepository.Category.LOWER),
                    locations.get(FeedRepository.Category.CURRENT),
                    locations.get(FeedRepository.Category.UPPER),
                    pointer,
                    consumer,
                    isAlive,
                    locations
                );
            } else {
                return Optional.of(locations.get(FeedRepository.Category.CURRENT));
            }
        }
    },

    RESUMING {
        @Override
        <LOCATION,
            ENTRY extends FeedEntry<LOCATION>,
            PAGE extends FeedPage<LOCATION, ENTRY>,
            POINTER,
            TRANSACTION>
        Optional<LOCATION> resume(
            FeedEndpoint<LOCATION, ENTRY, PAGE> endpoint,
            FeedRepository<LOCATION, POINTER, TRANSACTION> repository,
            FeedTransactor transactor,
            POINTER pointer,
            FeedConsumer<? super LOCATION, ? super ENTRY, ? extends TRANSACTION> consumer,
            BooleanSupplier isAlive
        ) throws InterruptedException {
            Map<FeedRepository.Category, LOCATION> locations = transactor.apply(
                FeedTransactor.Type.INITIALIZE,
                () -> repository.read(pointer)
            );
            if (locations.isEmpty()) {
                return Optional.empty();
            } else if (locations.containsKey(FeedRepository.Category.UPPER)) {
                FeedReader.<LOCATION, ENTRY, PAGE, POINTER, TRANSACTION>doReadBackward(
                    endpoint,
                    repository,
                    transactor,
                    locations.get(FeedRepository.Category.LOWER),
                    null,
                    locations.get(FeedRepository.Category.CURRENT),
                    pointer,
                    consumer,
                    isAlive,
                    locations
                );
                return Optional.of(locations.get(FeedRepository.Category.UPPER));
            } else {
                return Optional.of(locations.get(FeedRepository.Category.CURRENT));
            }
        }
    },

    FAILING {
        @Override
        <LOCATION,
            ENTRY extends FeedEntry<LOCATION>,
            PAGE extends FeedPage<LOCATION, ENTRY>,
            POINTER,
            TRANSACTION>
        Optional<LOCATION> resume(
            FeedEndpoint<LOCATION, ENTRY, PAGE> endpoint,
            FeedRepository<LOCATION, POINTER, TRANSACTION> repository,
            FeedTransactor transactor,
            POINTER pointer,
            FeedConsumer<? super LOCATION, ? super ENTRY, ? extends TRANSACTION> consumer,
            BooleanSupplier isAlive
        ) {
            Map<FeedRepository.Category, LOCATION> locations = transactor.apply(
                FeedTransactor.Type.INITIALIZE,
                () -> repository.read(pointer)
            );
            if (locations.isEmpty()) {
                return Optional.empty();
            } else if (locations.size() == 1 && locations.containsKey(FeedRepository.Category.CURRENT)) {
                return Optional.of(locations.get(FeedRepository.Category.CURRENT));
            } else {
                throw new IllegalStateException("Cannot resume feed for " + pointer + " in reverse direction");
            }
        }
    };

    abstract <LOCATION,
        ENTRY extends FeedEntry<LOCATION>,
        PAGE extends FeedPage<LOCATION, ENTRY>,
        POINTER,
        TRANSACTION>
    Optional<LOCATION> resume(
        FeedEndpoint<LOCATION, ENTRY, PAGE> endpoint,
        FeedRepository<LOCATION, POINTER, TRANSACTION> repository,
        FeedTransactor transactor,
        POINTER pointer,
        FeedConsumer<? super LOCATION, ? super ENTRY, ? extends TRANSACTION> consumer,
        BooleanSupplier isAlive
    ) throws InterruptedException;
}
