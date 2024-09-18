package no.skatteetaten.fastsetting.formueinntekt.felles.feed.stream;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.memory.InMemoryFeedRepository;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class StreamProcessorTest {

    private InMemoryFeedRepository<Long, String> repository = new InMemoryFeedRepository<>();

    private ExecutorService executorService;

    @Before
    public void setUp() {
        executorService = Executors.newSingleThreadExecutor(job -> {
            Thread thread = new Thread(job);
            thread.setName("feed-stream-test");
            thread.setDaemon(true);
            thread.setUncaughtExceptionHandler((t, exception) -> exception.printStackTrace());
            return thread;
        });
    }

    @After
    public void tearDown() throws InterruptedException {
        executorService.shutdownNow();
        assertThat(executorService.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void can_read_stream() {
        List<StreamEntry> result = new ArrayList<>();
        new StreamProcessor<>(
            "foobar",
            executorService,
            () -> new StringReader(String.join("\n", "foo", "bar", "qux")),
            2,
            false,
            100,
            repository,
            (commitment, location, direction, completed, entries) -> {
                result.addAll(entries);
                commitment.accept(null);
            }
        ).doStart(() -> true);
        assertThat(result).hasSize(3);
        assertThat(result).extracting(StreamEntry::getLocation).containsExactly(1L, 2L, 3L);
        assertThat(result).extracting(StreamEntry::getPayload).containsExactly("foo", "bar", "qux");
        assertThat(repository.readCurrent("foobar")).contains(Long.MAX_VALUE);
    }

    @Test
    public void can_read_stream_from_previous_state() {
        List<StreamEntry> result = new ArrayList<>();
        repository.updateCurrent("foobar", 1L, transaction -> { });
        new StreamProcessor<>(
            "foobar",
            executorService,
            () -> new StringReader(String.join("\n", "foo", "bar", "qux")),
            2,
            false,
            100,
            repository,
            (commitment, location, direction, completed, entries) -> {
                result.addAll(entries);
                commitment.accept(null);
            }
        ).doStart(() -> true);
        assertThat(result).hasSize(2);
        assertThat(result).extracting(StreamEntry::getLocation).containsExactly(2L, 3L);
        assertThat(result).extracting(StreamEntry::getPayload).containsExactly("bar", "qux");
        assertThat(repository.readCurrent("foobar")).contains(Long.MAX_VALUE);
    }

    @Test
    public void does_recover_from_error() {
        List<StreamEntry> result = new ArrayList<>();
        AtomicBoolean errorIndicator = new AtomicBoolean(true);
        new StreamProcessor<>(
            "foobar",
            executorService,
            () -> new StringReader(String.join("\n", "foo", "bar", "qux")),
            2,
            false,
            100,
            repository,
            (commitment, location, direction, completed, entries) -> {
                if (errorIndicator.getAndSet(false)) {
                    throw new RuntimeException("This is an expected error", null, false, false) { };
                }
                result.addAll(entries);
                commitment.accept(null);
            }
        ).doStart(() -> true);
        assertThat(result).hasSize(3);
        assertThat(result).extracting(StreamEntry::getLocation).containsExactly(1L, 2L, 3L);
        assertThat(result).extracting(StreamEntry::getPayload).containsExactly("foo", "bar", "qux");
        assertThat(repository.readCurrent("foobar")).contains(Long.MAX_VALUE);
    }

    @Test
    public void does_not_read_complete_stream() {
        repository.updateCurrent("foobar", Long.MAX_VALUE, transaction -> { });
        new StreamProcessor<>(
            "foobar",
            executorService,
            () -> new StringReader(String.join("\n", "foo", "bar", "qux")),
            2,
            false,
            100,
            repository,
            (commitment, location, direction, completed, entries) -> {
                throw new AssertionError();
            }
        ).doStart(() -> true);
        assertThat(repository.readCurrent("foobar")).contains(Long.MAX_VALUE);
    }

    @Test
    public void can_complete_stream() {
        new StreamProcessor<>(
            "foobar",
            executorService,
            () -> {
                throw new AssertionError();
            },
            2,
            false,
            100,
            repository,
            (commitment, location, direction, completed, entries) -> {
                throw new AssertionError();
            }
        ).onComplete();
        assertThat(repository.readCurrent("foobar")).contains(Long.MAX_VALUE);
    }
}
