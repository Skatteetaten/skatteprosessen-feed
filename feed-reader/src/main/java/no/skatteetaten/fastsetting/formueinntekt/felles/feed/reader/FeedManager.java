package no.skatteetaten.fastsetting.formueinntekt.felles.feed.reader;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.*;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class FeedManager<LOCATION,
    ENTRY extends FeedEntry<LOCATION>,
    PAGE extends FeedPage<LOCATION, ENTRY>,
    POINTER,
    TRANSACTION> extends FeedProcessor.Simple<LOCATION, POINTER, TRANSACTION> {

    private final FeedEndpoint<LOCATION, ENTRY, PAGE> endpoint;

    private final FeedReader reader;
    private final FeedContinuation continuation;

    private final Predicate<Throwable> onError;
    private final long pause;
    private final TimeUnit timeUnit;

    private final Supplier<FeedConsumer<? super LOCATION, ? super ENTRY, ? extends TRANSACTION>> consumerFactory;

    public FeedManager(
        POINTER pointer,
        FeedRepository<LOCATION, POINTER, TRANSACTION> repository,
        Executor executor,
        FeedEndpoint<LOCATION, ENTRY, PAGE> endpoint,
        FeedReader reader,
        FeedContinuation continuation,
        Predicate<Throwable> onError,
        long pause,
        TimeUnit timeUnit,
        Supplier<FeedConsumer<? super LOCATION, ? super ENTRY, ? extends TRANSACTION>> consumerFactory
    ) {
        super(pointer, repository, executor);
        this.endpoint = endpoint;
        this.reader = reader;
        this.continuation = continuation;
        this.onError = onError;
        this.pause = pause;
        this.timeUnit = timeUnit;
        this.consumerFactory = consumerFactory;
    }

    @Override
    protected void doStart(BooleanSupplier isAlive) {
        reader.run(endpoint, repository, continuation, pointer, pause, timeUnit, consumerFactory, () -> lastError = null, throwable -> {
            lastError = throwable;
            return onError.test(throwable);
        }, isAlive);
    }

    @Override
    protected void onComplete() {
        Optional<LOCATION> location = endpoint.getLastLocation();
        if (location.isPresent()) {
            repository.updateCurrent(pointer, location.get());
        } else {
            repository.delete(pointer);
        }
    }
}
