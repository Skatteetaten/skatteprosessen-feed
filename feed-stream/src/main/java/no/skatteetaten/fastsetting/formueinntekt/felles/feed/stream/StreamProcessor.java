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

public class StreamProcessor<POINTER, TRANSACTION> extends FeedProcessor.Simple<Long, POINTER, TRANSACTION> {

    private final Supplier<Reader> sourceProvider;

    private final int batch;

    private final boolean abortOnError;
    private final long pause;

    private final FeedConsumer<? super Long, ? super StreamEntry, ? extends TRANSACTION> consumer;

    public StreamProcessor(
        POINTER pointer,
        FeedRepository<Long, POINTER, TRANSACTION> repository,
        Executor executor,
        Supplier<Reader> sourceProvider,
        int batch,
        boolean abortOnError,
        long pause,
        FeedConsumer<? super Long, ? super StreamEntry, ? extends TRANSACTION> consumer
    ) {
        super(pointer, repository, executor);
        this.sourceProvider = sourceProvider;
        this.batch = batch;
        this.abortOnError = abortOnError;
        this.pause = pause;
        this.consumer = consumer;
    }

    @Override
    protected void doStart(BooleanSupplier isAlive) {
        while (isAlive.getAsBoolean()) {
            try {
                long location = 0, initial = repository.readCurrent(pointer).orElse(0L);
                consumer.onStart();
                if (initial == Long.MAX_VALUE) {
                    consumer.onSuccess(Long.MAX_VALUE);
                    return;
                }
                try (BufferedReader reader = new BufferedReader(sourceProvider.get())) {
                    lastError = null;
                    boolean initiate = true;
                    List<StreamEntry> entries = new ArrayList<>(batch);
                    String line = reader.readLine(), next;
                    while (line != null) {
                        if (Thread.interrupted()) {
                            return;
                        }
                        next = reader.readLine();
                        if (++location > initial) {
                            if (initiate) {
                                consumer.onSeries(location, FeedDirection.FORWARD);
                                initiate = false;
                            }
                            entries.add(new StreamEntry(location, line));
                            if (entries.size() == batch) {
                                long target = next == null ? Long.MAX_VALUE : location;
                                consumer.onPage(
                                    conn -> repository.updateCurrent(conn, pointer, target),
                                    location,
                                    FeedDirection.FORWARD,
                                    next == null,
                                    safeDownCast(entries)
                                );
                                entries.clear();
                            }
                        }
                        line = next;
                    }
                    if (!entries.isEmpty()) {
                        consumer.onPage(
                            conn -> repository.updateCurrent(conn, pointer, Long.MAX_VALUE),
                            location,
                            FeedDirection.FORWARD,
                            true,
                            safeDownCast(entries)
                        );
                    }
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
    protected void onComplete() {
        repository.updateCurrent(pointer, Long.MAX_VALUE);
    }

    @SuppressWarnings("unchecked")
    private static <ENTRY> List<ENTRY> safeDownCast(List<? extends ENTRY> entries) {
        return (List<ENTRY>) entries;
    }
}