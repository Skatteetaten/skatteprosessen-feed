package no.skatteetaten.fastsetting.formueinntekt.felles.feed.reader;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.*;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

public class FeedManager<LOCATION,
    ENTRY extends FeedEntry<LOCATION>,
    PAGE extends FeedPage<LOCATION, ENTRY>,
    POINTER,
    TRANSACTION> extends FeedProcessor.Simple<POINTER> {

    private final FeedEndpoint<LOCATION, ENTRY, PAGE> endpoint;

    private final FeedReader reader;
    private final FeedContinuation continuation;

    private final Predicate<Throwable> onError;
    private final long pause;
    private final TimeUnit timeUnit;

    private final FeedRepository<LOCATION, POINTER, TRANSACTION> repository;
    private final FeedTransactor transactor;
    private final FeedConsumer<? super LOCATION, ? super ENTRY, TRANSACTION> consumer;

    public FeedManager(
        POINTER pointer,
        Executor executor,
        FeedEndpoint<LOCATION, ENTRY, PAGE> endpoint,
        FeedReader reader,
        FeedContinuation continuation,
        Predicate<Throwable> onError,
        long pause,
        TimeUnit timeUnit,
        FeedRepository<LOCATION, POINTER, TRANSACTION> repository,
        FeedTransactor transactor,
        FeedConsumer<? super LOCATION, ? super ENTRY, TRANSACTION> consumer
    ) {
        super(pointer, executor);
        this.endpoint = endpoint;
        this.reader = reader;
        this.continuation = continuation;
        this.onError = onError;
        this.pause = pause;
        this.timeUnit = timeUnit;
        this.repository = repository;
        this.transactor = transactor;
        this.consumer = consumer;
    }

    @Override
    protected void doStart(BooleanSupplier isAlive) {
        reader.<LOCATION, ENTRY, PAGE, POINTER, TRANSACTION>run(
            endpoint,
            repository,
            transactor,
            continuation,
            pointer,
            pause,
            timeUnit,
            consumer,
            () -> lastError = null,
            throwable -> {
                lastError = throwable;
                return onError.test(throwable);
            },
            isAlive);
    }

    @Override
    protected void onReset() {
        repository.delete(pointer, consumer::onReset);
    }

    @Override
    protected void onComplete() {
        Optional<LOCATION> location = endpoint.getLastLocation();
        if (location.isPresent()) {
            repository.updateCurrent(pointer, location.get(), transaction -> consumer.onComplete(transaction, location.get()));
        } else {
            repository.delete(pointer, transaction -> consumer.onComplete(transaction, null));
        }
    }
}
