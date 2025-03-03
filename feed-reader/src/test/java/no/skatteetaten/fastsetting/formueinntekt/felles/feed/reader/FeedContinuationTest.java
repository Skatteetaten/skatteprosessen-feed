package no.skatteetaten.fastsetting.formueinntekt.felles.feed.reader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedDirection;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedTransactor;
import org.junit.Test;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.memory.InMemoryFeedEndpoint;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.memory.InMemoryFeedEntry;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.memory.InMemoryFeedRepository;

public class FeedContinuationTest {

    private InMemoryFeedEndpoint<String> endpoint = new InMemoryFeedEndpoint<>(2);

    private InMemoryFeedRepository<Integer, String> repository = new InMemoryFeedRepository<>();

    private List<InMemoryFeedEntry<String>> entries = new ArrayList<>();

    @Test
    public void can_read_empty_failing() throws InterruptedException {
        assertThat(FeedContinuation.FAILING.resume(endpoint, repository, new FeedTransactor.NoOp(), "foobar", (commitment, location, direction, completed, entries) -> {
            throw new AssertionError();
        }, () -> true)).isNotPresent();
        assertThat(repository.readCurrent("foobar")).isNotPresent();
    }

    @Test
    public void can_read_forward_failing() throws InterruptedException {
        repository.updateCurrent("foobar", 1, transaction -> { });
        assertThat(FeedContinuation.FAILING.resume(endpoint, repository, new FeedTransactor.NoOp(), "foobar", (commitment, location, direction, completed, entries) -> {
            throw new AssertionError();
        }, () -> true)).contains(1);
        assertThat(repository.readCurrent("foobar")).contains(1);
    }

    @Test
    public void can_not_read_backward_failing() {
        repository.updateAll("foobar", 1, 2, 3, transaction -> { });
        assertThatThrownBy(() -> FeedContinuation.FAILING.resume(endpoint, repository, new FeedTransactor.NoOp(), "foobar", (commitment, location, direction, completed, entries) -> {
            throw new AssertionError();
        }, () -> true)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void can_read_empty_resuming() throws InterruptedException {
        assertThat(FeedContinuation.RESUMING.resume(endpoint, repository, new FeedTransactor.NoOp(), "foobar", (commitment, location, direction, completed, entries) -> {
            throw new AssertionError();
        }, () -> true)).isNotPresent();
        assertThat(repository.readCurrent("foobar")).isNotPresent();
    }

    @Test
    public void can_read_forward_resuming() throws InterruptedException {
        repository.updateCurrent("foobar", 1, transaction -> { });
        assertThat(FeedContinuation.RESUMING.resume(endpoint, repository, new FeedTransactor.NoOp(), "foobar", (commitment, location, direction, completed, entries) -> {
            throw new AssertionError();
        }, () -> true)).contains(1);
        assertThat(repository.readCurrent("foobar")).contains(1);
    }

    @Test
    public void can_read_backward_resuming() throws InterruptedException {
        repository.updateAll("foobar", 0, 2, 2, transaction -> { });
        endpoint.add("foo", "bar", "qux", "baz");
        assertThat(FeedContinuation.RESUMING.resume(endpoint, repository, new FeedTransactor.NoOp(), "foobar", (commitment, location, direction, completed, entries) -> {
            assertThat(completed).isEqualTo(!this.entries.isEmpty());
            this.entries.addAll(entries);
            assertThat(direction).isEqualTo(FeedDirection.BACKWARD);
            commitment.accept(null);
        }, () -> true)).contains(2);
        assertThat(repository.readCurrent("foobar")).contains(2);
        assertThat(entries).extracting(InMemoryFeedEntry::getPayload).containsExactly("qux", "bar");
    }

    @Test
    public void can_read_backward_no_lower_limit_resuming() throws InterruptedException {
        repository.updateAll("foobar", null, 2 , 2, transaction -> { });
        endpoint.add("foo", "bar", "qux", "baz");
        assertThat(FeedContinuation.RESUMING.resume(endpoint, repository, new FeedTransactor.NoOp(), "foobar", (commitment, location, direction, completed, entries) -> {
            assertThat(completed).isEqualTo(!this.entries.isEmpty());
            this.entries.addAll(entries);
            assertThat(direction).isEqualTo(FeedDirection.BACKWARD);
            commitment.accept(null);
        }, () -> true)).contains(2);
        assertThat(repository.readCurrent("foobar")).contains(2);
        assertThat(entries).extracting(InMemoryFeedEntry::getPayload).containsExactly("qux", "foo", "bar");
    }

    @Test
    public void can_read_forward_repeating() throws InterruptedException {
        repository.updateCurrent("foobar", 1, transaction -> { });
        assertThat(FeedContinuation.REPEATING.resume(endpoint, repository, new FeedTransactor.NoOp(), "foobar", (commitment, location, direction, completed, entries) -> {
            throw new AssertionError();
        }, () -> true)).contains(1);
        assertThat(repository.readCurrent("foobar")).contains(1);
    }

    @Test
    public void can_read_backward_repeating() throws InterruptedException {
        repository.updateAll("foobar", 0, 1, 2, transaction -> { });
        endpoint.add("foo", "bar", "qux", "baz");
        List<InMemoryFeedEntry<String>> recovery = new ArrayList<>();
        assertThat(FeedContinuation.REPEATING.resume(endpoint, repository, new FeedTransactor.NoOp(), "foobar", (commitment, location, direction, completed, entries) -> {
            this.entries.addAll(entries);
            if (direction == FeedDirection.RECOVERY) {
                recovery.addAll(entries);
            }
            commitment.accept(null);
        }, () -> true)).contains(2);
        assertThat(repository.readCurrent("foobar")).contains(2);
        assertThat(entries).extracting(InMemoryFeedEntry::getPayload).containsExactly("qux", "bar");
        assertThat(recovery).extracting(InMemoryFeedEntry::getPayload).containsExactly("qux");
    }

    @Test
    public void can_read_backward_no_lower_limit_repeating() throws InterruptedException {
        repository.updateAll("foobar", null, 1, 2, transaction -> { });
        endpoint.add("foo", "bar", "qux", "baz");
        List<InMemoryFeedEntry<String>> recovery = new ArrayList<>();
        assertThat(FeedContinuation.REPEATING.resume(endpoint, repository, new FeedTransactor.NoOp(), "foobar", (commitment, location, direction, completed, entries) -> {
            this.entries.addAll(entries);
            if (direction == FeedDirection.RECOVERY) {
                recovery.addAll(entries);
            }
            commitment.accept(null);
        }, () -> true)).contains(2);
        assertThat(repository.readCurrent("foobar")).contains(2);
        assertThat(entries).extracting(InMemoryFeedEntry::getPayload).containsExactly("qux", "foo", "bar");
        assertThat(recovery).extracting(InMemoryFeedEntry::getPayload).containsExactly("qux");
    }
}
