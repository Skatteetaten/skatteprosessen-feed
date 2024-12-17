package no.skatteetaten.fastsetting.formueinntekt.felles.feed.reader;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedDirection;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedTransactor;
import org.junit.Test;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.memory.InMemoryFeedEndpoint;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.memory.InMemoryFeedEntry;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.memory.InMemoryFeedRepository;

public class FeedReaderTest {

    private InMemoryFeedEndpoint<String> endpoint = new InMemoryFeedEndpoint<>(2);
    private InMemoryFeedRepository<Integer, String> repository = new InMemoryFeedRepository<>();
    private List<InMemoryFeedEntry<String>> entries = new ArrayList<>();

    @Test
    public void can_read_empty_forward() throws InterruptedException {
        assertThat(FeedReader.FORWARD.apply(endpoint, repository, new FeedTransactor.NoOp(), "foobar", null, (commitment, location, direction, completed, entries) -> {
            throw new AssertionError();
        }, () -> true)).isNotPresent();
        assertThat(repository.readCurrent("foobar")).isNotPresent();
    }

    @Test
    public void can_read_single_page_forward() throws InterruptedException {
        endpoint.add("foo", "bar");
        assertThat(FeedReader.FORWARD.apply(endpoint, repository, new FeedTransactor.NoOp(), "foobar", null, (commitment, location, direction, completed, entries) -> {
            this.entries.addAll(entries);
            assertThat(direction).isEqualTo(FeedDirection.FORWARD);
            commitment.accept(null);
        }, () -> true)).contains(1);
        assertThat(entries).extracting(InMemoryFeedEntry::getPayload).containsExactly("foo", "bar");
        assertThat(repository.readCurrent("foobar")).contains(1);
    }

    @Test
    public void can_read_multiple_pages_forward() throws InterruptedException {
        endpoint.add("foo", "bar", "qux");
        assertThat(FeedReader.FORWARD.apply(endpoint, repository, new FeedTransactor.NoOp(), "foobar", null, (commitment, location, direction, completed, entries) -> {
            this.entries.addAll(entries);
            assertThat(direction).isEqualTo(FeedDirection.FORWARD);
            commitment.accept(null);
        }, () -> true)).contains(2);
        assertThat(entries).extracting(InMemoryFeedEntry::getPayload).containsExactly("foo", "bar", "qux");
        assertThat(repository.readCurrent("foobar")).contains(2);
    }

    @Test
    public void can_read_multiple_pages_previous_location_forward() throws InterruptedException {
        endpoint.add("foo", "bar", "qux");
        repository.updateCurrent("foobar", 0, transaction -> { });
        assertThat(FeedReader.FORWARD.apply(endpoint, repository, new FeedTransactor.NoOp(), "foobar", 0, (commitment, location, direction, completed, entries) -> {
            this.entries.addAll(entries);
            assertThat(direction).isEqualTo(FeedDirection.FORWARD);
            commitment.accept(null);
        }, () -> true)).contains(2);
        assertThat(entries).extracting(InMemoryFeedEntry::getPayload).containsExactly("bar", "qux");
        assertThat(repository.readCurrent("foobar")).contains(2);
    }

    @Test
    public void can_read_empty_backward() throws InterruptedException {
        assertThat(FeedReader.BACKWARD.apply(endpoint, repository, new FeedTransactor.NoOp(), "foobar", null, (commitment, location, direction, completed, entries) -> {
            throw new AssertionError();
        }, () -> true)).isNotPresent();
        assertThat(repository.readCurrent("foobar")).isNotPresent();
    }

    @Test
    public void can_read_single_page_backward() throws InterruptedException {
        endpoint.add("foo", "bar");
        assertThat(FeedReader.BACKWARD.apply(endpoint, repository, new FeedTransactor.NoOp(), "foobar", null, (commitment, location, direction, completed, entries) -> {
            this.entries.addAll(entries);
            assertThat(direction).isEqualTo(FeedDirection.BACKWARD);
            commitment.accept(null);
        }, () -> true)).contains(1);
        assertThat(entries).extracting(InMemoryFeedEntry::getPayload).containsExactly("foo", "bar");
        assertThat(repository.readCurrent("foobar")).contains(1);
    }

    @Test
    public void can_read_multiple_pages_backward() throws InterruptedException {
        endpoint.add("foo", "bar", "qux");
        assertThat(FeedReader.BACKWARD.apply(endpoint, repository, new FeedTransactor.NoOp(), "foobar", null, (commitment, location, direction, completed, entries) -> {
            this.entries.addAll(entries);
            assertThat(direction).isEqualTo(FeedDirection.BACKWARD);
            commitment.accept(null);
        }, () -> true)).contains(2);
        assertThat(entries).extracting(InMemoryFeedEntry::getPayload).containsExactly("qux", "foo", "bar");
        assertThat(repository.readCurrent("foobar")).contains(2);
    }

    @Test
    public void can_read_multiple_pages_previous_location_backward() throws InterruptedException {
        endpoint.add("foo", "bar", "qux");
        repository.updateCurrent("foobar", 0, transaction -> { });
        assertThat(FeedReader.BACKWARD.apply(endpoint, repository, new FeedTransactor.NoOp(), "foobar", 0, (commitment, location, direction, completed, entries) -> {
            this.entries.addAll(entries);
            assertThat(direction).isEqualTo(FeedDirection.BACKWARD);
            commitment.accept(null);
        }, () -> true)).contains(2);
        assertThat(entries).extracting(InMemoryFeedEntry::getPayload).containsExactly("qux", "bar");
        assertThat(repository.readCurrent("foobar")).contains(2);
    }

    @Test
    public void can_read_empty_backward_initial_only() throws InterruptedException {
        assertThat(FeedReader.BACKWARD_INITIAL_ONLY.apply(endpoint, repository, new FeedTransactor.NoOp(), "foobar", null, (commitment, location, direction, completed, entries) -> {
            throw new AssertionError();
        }, () -> true)).isNotPresent();
        assertThat(repository.readCurrent("foobar")).isNotPresent();
    }

    @Test
    public void can_read_single_page_backward_initial_only() throws InterruptedException {
        endpoint.add("foo", "bar");
        assertThat(FeedReader.BACKWARD_INITIAL_ONLY.apply(endpoint, repository, new FeedTransactor.NoOp(), "foobar", null, (commitment, location, direction, completed, entries) -> {
            this.entries.addAll(entries);
            assertThat(direction).isEqualTo(entries.isEmpty() ? FeedDirection.FORWARD : FeedDirection.BACKWARD);
            commitment.accept(null);
        }, () -> true)).contains(1);
        assertThat(entries).extracting(InMemoryFeedEntry::getPayload).containsExactly("foo", "bar");
        assertThat(repository.readCurrent("foobar")).contains(1);
    }

    @Test
    public void can_read_multiple_pages_backward_initial_only() throws InterruptedException {
        endpoint.add("foo", "bar", "qux");
        assertThat(FeedReader.BACKWARD_INITIAL_ONLY.apply(endpoint, repository, new FeedTransactor.NoOp(), "foobar", null, (commitment, location, direction, completed, entries) -> {
            this.entries.addAll(entries);
            assertThat(direction).isEqualTo(entries.isEmpty() ? FeedDirection.FORWARD : FeedDirection.BACKWARD);
            commitment.accept(null);
        }, () -> true)).contains(2);
        assertThat(entries).extracting(InMemoryFeedEntry::getPayload).containsExactly("qux", "foo", "bar");
        assertThat(repository.readCurrent("foobar")).contains(2);
    }

    @Test
    public void can_read_multiple_pages_previous_location_backward_initial_only() throws InterruptedException {
        endpoint.add("foo", "bar", "qux");
        repository.updateCurrent("foobar", 0, transaction -> { });
        assertThat(FeedReader.BACKWARD_INITIAL_ONLY.apply(endpoint, repository, new FeedTransactor.NoOp(), "foobar", 0, (commitment, location, direction, completed, entries) -> {
            this.entries.addAll(entries);
            assertThat(direction).isEqualTo(FeedDirection.FORWARD);
            commitment.accept(null);
        }, () -> true)).contains(2);
        assertThat(entries).extracting(InMemoryFeedEntry::getPayload).containsExactly("bar", "qux");
        assertThat(repository.readCurrent("foobar")).contains(2);
    }

    @Test
    public void can_read_empty_backward_once_only() throws InterruptedException {
        assertThat(FeedReader.BACKWARD_ONCE_ONLY.apply(endpoint, repository, new FeedTransactor.NoOp(), "foobar", null, (commitment, location, direction, completed, entries) -> {
            throw new AssertionError();
        }, () -> true)).isNotPresent();
        assertThat(repository.readCurrent("foobar")).isNotPresent();
    }

    @Test
    public void can_read_single_page_backward_once_only() throws InterruptedException {
        endpoint.add("foo", "bar");
        assertThat(FeedReader.BACKWARD_ONCE_ONLY.apply(endpoint, repository, new FeedTransactor.NoOp(), "foobar", null, (commitment, location, direction, completed, entries) -> {
            this.entries.addAll(entries);
            assertThat(direction).isEqualTo(entries.isEmpty() ? FeedDirection.FORWARD : FeedDirection.BACKWARD);
            commitment.accept(null);
        }, () -> true)).contains(1);
        assertThat(entries).extracting(InMemoryFeedEntry::getPayload).containsExactly("foo", "bar");
        assertThat(repository.readCurrent("foobar")).contains(1);
    }

    @Test
    public void can_read_multiple_pages_backward_once_only() throws InterruptedException {
        endpoint.add("foo", "bar", "qux");
        assertThat(FeedReader.BACKWARD_ONCE_ONLY.apply(endpoint, repository, new FeedTransactor.NoOp(), "foobar", null, (commitment, location, direction, completed, entries) -> {
            this.entries.addAll(entries);
            assertThat(direction).isEqualTo(entries.isEmpty() ? FeedDirection.FORWARD : FeedDirection.BACKWARD);
            commitment.accept(null);
        }, () -> true)).contains(2);
        assertThat(entries).extracting(InMemoryFeedEntry::getPayload).containsExactly("qux", "foo", "bar");
        assertThat(repository.readCurrent("foobar")).contains(2);
    }

    @Test
    public void does_not_read_again_backwards_once_only() throws InterruptedException {
        endpoint.add("foo", "bar", "qux");
        assertThat(FeedReader.BACKWARD_ONCE_ONLY.apply(endpoint, repository, new FeedTransactor.NoOp(), "foobar", 0, (commitment, location, direction, completed, entries) -> {
            this.entries.addAll(entries);
            assertThat(direction).isEqualTo(FeedDirection.FORWARD);
            commitment.accept(null);
        }, () -> true)).contains(0);
        assertThat(entries).isEmpty();
        assertThat(repository.readCurrent("foobar")).isEmpty();
    }
}
