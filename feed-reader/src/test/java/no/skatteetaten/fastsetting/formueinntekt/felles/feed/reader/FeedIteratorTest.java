package no.skatteetaten.fastsetting.formueinntekt.felles.feed.reader;

import org.junit.Test;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedPage;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.memory.InMemoryFeedEndpoint;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.memory.InMemoryFeedEntry;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class FeedIteratorTest {

    private InMemoryFeedEndpoint<String> endpoint = new InMemoryFeedEndpoint<>(2);
    private List<FeedPage<Integer, InMemoryFeedEntry<String>>> pages = new ArrayList<>();

    @Test
    public void can_read_empty_feed_forward() throws InterruptedException {
        assertThat(FeedIterator.FORWARD.read(
            endpoint, null, null,
            (page, origin) -> pages.add(page),
            () -> true
        )).isNotPresent();
        assertThat(pages).isEmpty();
    }

    @Test
    public void can_read_single_page_forward() throws InterruptedException {
        assertThat(FeedIterator.FORWARD.read(
            endpoint.add("foo"), null, null,
            (page, origin) -> pages.add(page),
            () -> true
        )).contains(0);
        assertThat(pages).hasSize(1);
        assertThat(pages.stream()
            .flatMap(page -> page.getEntries().stream())
            .map(InMemoryFeedEntry::getPayload)).containsExactly("foo");
    }

    @Test
    public void can_read_single_page_with_location_forward() throws InterruptedException {
        assertThat(FeedIterator.FORWARD.read(
            endpoint.add("foo", "bar"), 0, null,
            (page, origin) -> pages.add(page),
            () -> true
        )).contains(1);
        assertThat(pages).hasSize(1);
        assertThat(pages.stream()
            .flatMap(page -> page.getEntries().stream())
            .map(InMemoryFeedEntry::getPayload)).containsExactly("foo", "bar");
    }

    @Test
    public void can_read_single_page_with_boundary_forward() throws InterruptedException {
        assertThat(FeedIterator.FORWARD.read(endpoint.add("foo", "bar"), null, 1, (page, origin) -> {
            throw new AssertionError();
        }, () -> true)).contains(1);
    }

    @Test
    public void can_read_single_page_complete_forward() throws InterruptedException {
        assertThat(FeedIterator.FORWARD.read(
            endpoint.add("foo", "bar"), null, null,
            (page, origin) -> pages.add(page),
            () -> true
        )).contains(1);
        assertThat(pages).hasSize(1);
        assertThat(pages.stream()
            .flatMap(page -> page.getEntries().stream())
            .map(InMemoryFeedEntry::getPayload)).containsExactly("foo", "bar");
    }

    @Test
    public void can_read_multiple_page_forward() throws InterruptedException {
        assertThat(FeedIterator.FORWARD.read(
            endpoint.add("foo", "bar", "qux"), null, null,
            (page, origin) -> pages.add(page),
            () -> true
        )).contains(2);
        assertThat(pages).hasSize(2);
        assertThat(pages.stream()
            .flatMap(page -> page.getEntries().stream())
            .map(InMemoryFeedEntry::getPayload)).containsExactly("foo", "bar", "qux");
    }

    @Test
    public void can_skip_page_forward() throws InterruptedException {
        assertThat(FeedIterator.FORWARD.read(
            endpoint.add("foo", "bar", "qux", "baz"), 2, null,
            (page, origin) -> pages.add(page),
            () -> true
        )).contains(3);
        assertThat(pages).hasSize(1);
        assertThat(pages.stream()
            .flatMap(page -> page.getEntries().stream())
            .map(InMemoryFeedEntry::getPayload)).containsExactly("qux", "baz");
    }

    @Test
    public void can_abort_reading_forward() throws InterruptedException {
        assertThat(FeedIterator.FORWARD.read(endpoint.add("foo", "bar", "qux"), null, null, (page, origin) -> {
            pages.add(page);
            return false;
        }, () -> true)).contains(2);
        assertThat(pages).hasSize(1);
        assertThat(pages.stream()
            .flatMap(page -> page.getEntries().stream())
            .map(InMemoryFeedEntry::getPayload)).containsExactly("foo", "bar");
    }

    @Test
    public void can_read_empty_feed_backward() throws InterruptedException {
        assertThat(FeedIterator.BACKWARD.read(
            endpoint, null, null,
            (page, origin) -> pages.add(page),
            () -> true
        )).isNotPresent();
        assertThat(pages).isEmpty();
    }

    @Test
    public void can_read_single_page_backward() throws InterruptedException {
        assertThat(FeedIterator.BACKWARD.read(
            endpoint.add("foo"), null, null,
            (page, origin) -> pages.add(page),
            () -> true
        )).contains(0);
        assertThat(pages).hasSize(1);
        assertThat(pages.stream()
            .flatMap(page -> page.getEntries().stream())
            .map(InMemoryFeedEntry::getPayload)).containsExactly("foo");
    }

    @Test
    public void can_read_single_page_with_location_backward() throws InterruptedException {
        assertThat(FeedIterator.BACKWARD.read(
            endpoint.add("foo", "bar"), 0, null,
            (page, origin) -> pages.add(page),
            () -> true
        )).contains(1);
        assertThat(pages).hasSize(1);
        assertThat(pages.stream()
            .flatMap(page -> page.getEntries().stream())
            .map(InMemoryFeedEntry::getPayload)).containsExactly("foo", "bar");
    }

    @Test
    public void can_read_single_page_with_boundary_backward() throws InterruptedException {
        assertThat(FeedIterator.BACKWARD.read(endpoint.add("foo", "bar"), null, 1, (page, origin) -> {
            throw new AssertionError();
        }, () -> true)).contains(1);
    }

    @Test
    public void can_read_single_page_complete_backward() throws InterruptedException {
        assertThat(FeedIterator.BACKWARD.read(
            endpoint.add("foo", "bar"), null, null,
            (page, origin) -> pages.add(page),
            () -> true
        )).contains(1);
        assertThat(pages).hasSize(1);
        assertThat(pages.stream()
            .flatMap(page -> page.getEntries().stream())
            .map(InMemoryFeedEntry::getPayload)).containsExactly("foo", "bar");
    }

    @Test
    public void can_read_multiple_page_backward() throws InterruptedException {
        assertThat(FeedIterator.BACKWARD.read(
            endpoint.add("foo", "bar", "qux"), null, null,
            (page, origin) -> pages.add(page),
            () -> true
        )).contains(2);
        assertThat(pages).hasSize(2);
        assertThat(pages.stream()
            .flatMap(page -> page.getEntries().stream())
            .map(InMemoryFeedEntry::getPayload)).containsExactly("qux", "foo", "bar");
    }

    @Test
    public void can_skip_page_backward() throws InterruptedException {
        assertThat(FeedIterator.BACKWARD.read(
            endpoint.add("foo", "bar", "qux"), 2, null,
            (page, origin) -> pages.add(page),
            () -> true
        )).contains(2);
        assertThat(pages).hasSize(2);
        assertThat(pages.stream()
            .flatMap(page -> page.getEntries().stream())
            .map(InMemoryFeedEntry::getPayload)).containsExactly("qux", "foo", "bar");
    }

    @Test
    public void can_abort_reading_backward() throws InterruptedException {
        assertThat(FeedIterator.BACKWARD.read(endpoint.add("foo", "bar", "qux"), null, null, (page, origin) -> {
            pages.add(page);
            return false;
        }, () -> true)).contains(2);
        assertThat(pages).hasSize(1);
        assertThat(pages.stream()
            .flatMap(page -> page.getEntries().stream())
            .map(InMemoryFeedEntry::getPayload)).containsExactly("qux");
    }
}
