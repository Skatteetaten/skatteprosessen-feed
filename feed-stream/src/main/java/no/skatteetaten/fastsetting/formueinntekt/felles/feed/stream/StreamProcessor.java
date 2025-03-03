package no.skatteetaten.fastsetting.formueinntekt.felles.feed.stream;

import java.io.BufferedReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedProcessor;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedDirection;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedRepository;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedConsumer;

public class StreamProcessor<POINTER, TRANSACTION> extends FeedProcessor.Simple<POINTER> {

    private final Supplier<Reader> sourceProvider;

    private final int batch;

    private final boolean abortOnError;
    private final long pause;

    private final FeedRepository<Long, POINTER, TRANSACTION> repository;
    private final FeedConsumer<? super Long, ? super StreamEntry, TRANSACTION> consumer;

    public StreamProcessor(
        POINTER pointer,
        Executor executor,
        Supplier<Reader> sourceProvider,
        int batch,
        boolean abortOnError,
        long pause,
        FeedRepository<Long, POINTER, TRANSACTION> repository,
        FeedConsumer<? super Long, ? super StreamEntry, TRANSACTION> consumer
    ) {
        super(pointer, executor);
        this.sourceProvider = sourceProvider;
        this.batch = batch;
        this.abortOnError = abortOnError;
        this.pause = pause;
        this.repository = repository;
        this.consumer = consumer;
    }

    @Override
    protected void doStart(BooleanSupplier isAlive) {
        while (isAlive.getAsBoolean()) {
            try {
                long location = 0, previous = repository.readCurrent(pointer).orElse(-1L), origin = Math.max(0, previous);
                consumer.onStart();
                if (origin == Long.MAX_VALUE) {
                    consumer.onSuccess(Long.MAX_VALUE);
                    return;
                }
                List<StreamEntry> entries = new ArrayList<>(batch);
                try (BufferedReader reader = new BufferedReader(sourceProvider.get())) {
                    lastError = null;
                    String line = reader.readLine(), next;
                    boolean initial = true;
                    while (line != null) {
                        if (Thread.interrupted()) {
                            return;
                        }
                        next = reader.readLine();
                        if (++location > origin) {
                            if (initial) {
                                consumer.onSeries(location, FeedDirection.FORWARD);
                                initial = false;
                            }
                            entries.add(new StreamEntry(location, line));
                            if (entries.size() == batch) {
                                long from = previous, to = next == null ? Long.MAX_VALUE : location;
                                consumer.onPage(
                                    conn -> repository.transitCurrent(conn, pointer, from == -1 ? null : from, to),
                                    location,
                                    FeedDirection.FORWARD,
                                    next == null,
                                    safeDownCast(entries)
                                );
                                entries.clear();
                                previous = to;
                            }
                        }
                        line = next;
                    }
                }
                if (!entries.isEmpty()) {
                    long from = previous;
                    consumer.onPage(
                        conn -> repository.transitCurrent(conn, pointer, from == -1 ? null : from, Long.MAX_VALUE),
                        location,
                        FeedDirection.FORWARD,
                        true,
                        safeDownCast(entries)
                    );
                }
                consumer.onSuccess(location);
                return;
            } catch (Throwable throwable) {
                try {
                    consumer.onError(throwable);
                } catch (Throwable suppressed) {
                    throwable.addSuppressed(suppressed);
                }
                lastError = throwable;
                if (abortOnError) {
                    return;
                } else {
                    try {
                        Thread.sleep(pause);
                    } catch (InterruptedException ignored) {
                        return;
                    }
                }
            }
        }
    }

    @Override
    protected void onReset() {
        repository.delete(pointer, consumer::onReset);
    }

    @Override
    protected void onComplete() {
        repository.updateCurrent(pointer, Long.MAX_VALUE, transaction -> consumer.onComplete(transaction, Long.MAX_VALUE));
    }

    @SuppressWarnings("unchecked")
    private static <ENTRY> List<ENTRY> safeDownCast(List<? extends ENTRY> entries) {
        return (List<ENTRY>) entries;
    }
}