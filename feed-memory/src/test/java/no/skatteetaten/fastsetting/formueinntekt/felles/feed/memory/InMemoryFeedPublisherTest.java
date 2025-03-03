package no.skatteetaten.fastsetting.formueinntekt.felles.feed.memory;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedPublisher;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class InMemoryFeedPublisherTest {

    private FeedPublisher<Integer, Void> publisher;

    @Before
    public void setUp() {
        publisher = new InMemoryFeedPublisher<>();
    }

    @Test
    public void can_initialize() {
        assertThat(publisher.initialize(null)).isFalse();
    }

    @Test
    public void can_publish_and_lookup() {
        assertThat(publisher.payload(null, -1)).isEmpty();
        assertThat(publisher.payload(null, 0)).isEmpty();
        assertThat(publisher.payload(null, 1)).isEmpty();
        assertThat(publisher.payload(null, 2)).isEmpty();
        publisher.publish(null, -1);
        assertThat(publisher.payload(null, 0)).isEmpty();
        assertThat(publisher.payload(null, 1)).contains(-1);
        assertThat(publisher.payload(null, 2)).isEmpty();
    }

    @Test
    public void can_page_forwards() {
        publisher.publish(null, -1, -2, -3);
        assertThat(publisher.page(null, 0, 2, false)).contains(
            new FeedPublisher.Entry<>(1, -1),
            new FeedPublisher.Entry<>(2, -2)
        );
        assertThat(publisher.page(null, 2, 2, false)).contains(
            new FeedPublisher.Entry<>(3, -3)
        );
        assertThat(publisher.page(null, 3, 2, false)).isEmpty();
    }

    @Test
    public void can_page_backwards() {
        publisher.publish(null, -1, -2, -3);
        assertThat(publisher.page(null, Long.MAX_VALUE, 2, true)).contains(
            new FeedPublisher.Entry<>(3, -3),
            new FeedPublisher.Entry<>(2, -2)
        );
        assertThat(publisher.page(null, 1, 2, true)).contains(
            new FeedPublisher.Entry<>(1, -1)
        );
        assertThat(publisher.page(null, 0, 2, true)).isEmpty();
    }

    @Test
    public void can_request_zero_size() {
        publisher.publish(null, -1, -2, -3);
        assertThat(publisher.page(null, 0, 0, false)).isEmpty();
    }

    @Test(expected = IllegalArgumentException.class)
    public void cannot_request_negative_size() {
        assertThat(publisher.page(null, 0, -1, false)).isEmpty();
    }

    @Test
    public void can_find_limit() {
        assertThat(publisher.limit(null)).isEqualTo(0);
        publisher.publish(null, 1, 2, 3);
        assertThat(publisher.limit(null)).isEqualTo(3);
    }

    @Test
    public void can_reset() {
        publisher.publish(null, 1, 2, 3);
        assertThat(publisher.limit(null)).isEqualTo(3);
        publisher.reset(null);
        assertThat(publisher.limit(null)).isEqualTo(0);
    }
}
