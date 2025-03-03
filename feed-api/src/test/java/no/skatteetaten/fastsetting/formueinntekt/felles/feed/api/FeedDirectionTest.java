package no.skatteetaten.fastsetting.formueinntekt.felles.feed.api;

import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;

public class FeedDirectionTest {

    @Test
    public void forwards_iteration() {
        Iterator<String> it = FeedDirection.FORWARD.iteration(Arrays.asList("foo", "bar", "qux")).iterator();
        assertThat(it.hasNext()).isTrue();
        assertThat(it.next()).isEqualTo("foo");
        assertThat(it.hasNext()).isTrue();
        assertThat(it.next()).isEqualTo("bar");
        assertThat(it.hasNext()).isTrue();
        assertThat(it.next()).isEqualTo("qux");
        assertThat(it.hasNext()).isFalse();
    }

    @Test
    public void backwards_iteration() {
        Iterator<String> it = FeedDirection.BACKWARD.iteration(Arrays.asList("foo", "bar", "qux")).iterator();
        assertThat(it.hasNext()).isTrue();
        assertThat(it.next()).isEqualTo("qux");
        assertThat(it.hasNext()).isTrue();
        assertThat(it.next()).isEqualTo("bar");
        assertThat(it.hasNext()).isTrue();
        assertThat(it.next()).isEqualTo("foo");
        assertThat(it.hasNext()).isFalse();
    }

    @Test
    public void recovery_iteration() {
        Iterator<String> it = FeedDirection.RECOVERY.iteration(Arrays.asList("foo", "bar", "qux")).iterator();
        assertThat(it.hasNext()).isTrue();
        assertThat(it.next()).isEqualTo("qux");
        assertThat(it.hasNext()).isTrue();
        assertThat(it.next()).isEqualTo("bar");
        assertThat(it.hasNext()).isTrue();
        assertThat(it.next()).isEqualTo("foo");
        assertThat(it.hasNext()).isFalse();
    }
}