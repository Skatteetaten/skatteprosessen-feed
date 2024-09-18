package no.skatteetaten.fastsetting.formueinntekt.felles.feed.reader;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedProcessor;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedTransactor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.memory.InMemoryFeedEndpoint;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.memory.InMemoryFeedRepository;

public class FeedManagerTest {

    private InMemoryFeedEndpoint<String> endpoint = new InMemoryFeedEndpoint<>(2);
    private InMemoryFeedRepository<Integer, String> repository = new InMemoryFeedRepository<>();

    private FeedProcessor<String> manager;

    private ExecutorService executorService;

    @Before
    public void setUp() {
        executorService = Executors.newSingleThreadExecutor(job -> {
            Thread thread = new Thread(job);
            thread.setName("feed-manager-test");
            thread.setDaemon(true);
            thread.setUncaughtExceptionHandler((t, exception) -> exception.printStackTrace());
            return thread;
        });
        manager = new FeedManager<>(
            "foobar",
            executorService,
            endpoint,
            FeedReader.FORWARD,
            FeedContinuation.FAILING,
            throwable -> false,
            10,
            TimeUnit.MILLISECONDS,
            repository,
            new FeedTransactor.NoOp(),
            (commitment, location, direction, completed, entries) -> commitment.accept(null)
        );
    }

    @After
    public void tearDown() throws InterruptedException {
        executorService.shutdownNow();
        assertThat(executorService.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void can_start_and_stop_feed() throws TimeoutException, InterruptedException {
        assertThat(manager.start(5, TimeUnit.SECONDS)).isTrue();
        assertThat(manager.getCurrentState().isAlive()).isTrue();
        assertThat(manager.start(5, TimeUnit.SECONDS)).isFalse();
        assertThat(manager.stop(5, TimeUnit.SECONDS)).isTrue();
        assertThat(manager.getCurrentState().isAlive()).isFalse();
        assertThat(manager.stop(5, TimeUnit.SECONDS)).isFalse();
    }

    @Test
    public void can_reset_feed() throws TimeoutException, InterruptedException {
        repository.updateCurrent("foobar", 10, transaction -> { });
        assertThat(manager.start(500, TimeUnit.MILLISECONDS)).isTrue();
        manager.reset(1_000, TimeUnit.MILLISECONDS);
        assertThat(manager.getCurrentState().isAlive()).isFalse();
        assertThat(repository.read("foobar")).isEmpty();
    }

    @Test
    public void can_complete_feed() throws TimeoutException, InterruptedException {
        endpoint.add("foo", "bar", "qux", "baz");
        manager.complete(1_000, TimeUnit.MILLISECONDS);
        assertThat(manager.getCurrentState().isAlive()).isFalse();
        assertThat(repository.readCurrent("foobar")).contains(3);
    }
}
