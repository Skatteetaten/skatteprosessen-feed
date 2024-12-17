package no.skatteetaten.fastsetting.formueinntekt.felles.feed.memory;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class InMemoryFeedEndpointTest {

    private InMemoryFeedEndpoint<String> endpoint = new InMemoryFeedEndpoint<>(3);

    @Test
    public void can_read_empty_page() {
        assertThat(endpoint.getFirstPage()).isNotPresent();
        assertThat(endpoint.getLastPage()).isNotPresent();
        assertThat(endpoint.getPage(0)).isNotPresent();
    }

    @Test
    public void handles_single_page_incomplete() {
        endpoint.add("foo");
        assertThat(endpoint.getFirstPage()).hasValueSatisfying(page -> {
            assertThat(page.getLocation()).isEqualTo(0);
            assertThat(page.getPreviousLocation()).isNotPresent();
            assertThat(page.getNextLocation()).isNotPresent();
            assertThat(page.getEntries()).extracting(InMemoryFeedEntry::getLocation).containsExactly(0);
            assertThat(page.getEntries()).extracting(InMemoryFeedEntry::getPayload).containsExactly("foo");
        });
        assertThat(endpoint.getLastPage()).hasValueSatisfying(page -> assertThat(page.getLocation()).isEqualTo(0));
        assertThat(endpoint.getPage(0)).hasValueSatisfying(page -> assertThat(page.getLocation()).isEqualTo(0));
        assertThat(endpoint.getPage(1)).isNotPresent();
    }

    @Test
    public void handles_single_page_complete() {
        endpoint.add("foo", "bar", "qux");
        assertThat(endpoint.getFirstPage()).hasValueSatisfying(page -> {
            assertThat(page.getLocation()).isEqualTo(2);
            assertThat(page.getPreviousLocation()).isNotPresent();
            assertThat(page.getNextLocation()).isNotPresent();
            assertThat(page.getEntries()).extracting(InMemoryFeedEntry::getLocation).containsExactly(0, 1, 2);
            assertThat(page.getEntries()).extracting(InMemoryFeedEntry::getPayload)
                .containsExactly("foo", "bar", "qux");
        });
        assertThat(endpoint.getLastPage()).hasValueSatisfying(page -> assertThat(page.getLocation()).isEqualTo(2));
        assertThat(endpoint.getPage(0)).hasValueSatisfying(page -> assertThat(page.getLocation()).isEqualTo(2));
        assertThat(endpoint.getPage(1)).hasValueSatisfying(page -> assertThat(page.getLocation()).isEqualTo(2));
        assertThat(endpoint.getPage(2)).hasValueSatisfying(page -> assertThat(page.getLocation()).isEqualTo(2));
        assertThat(endpoint.getPage(3)).isNotPresent();
    }

    @Test
    public void handles_double_page() {
        endpoint.add("foo", "bar", "qux", "baz");
        assertThat(endpoint.getFirstPage()).hasValueSatisfying(page -> {
            assertThat(page.getLocation()).isEqualTo(2);
            assertThat(page.getPreviousLocation()).isNotPresent();
            assertThat(page.getNextLocation()).contains(3);
            assertThat(page.getEntries()).extracting(InMemoryFeedEntry::getLocation).containsExactly(0, 1, 2);
            assertThat(page.getEntries()).extracting(InMemoryFeedEntry::getPayload)
                .containsExactly("foo", "bar", "qux");
        });
        assertThat(endpoint.getLastPage()).hasValueSatisfying(page -> {
            assertThat(page.getLocation()).isEqualTo(3);
            assertThat(page.getPreviousLocation()).contains(2);
            assertThat(page.getNextLocation()).isNotPresent();
            assertThat(page.getEntries()).extracting(InMemoryFeedEntry::getLocation).containsExactly(3);
            assertThat(page.getEntries()).extracting(InMemoryFeedEntry::getPayload).containsExactly("baz");
        });
        assertThat(endpoint.getPage(0)).hasValueSatisfying(page -> assertThat(page.getLocation()).isEqualTo(2));
        assertThat(endpoint.getPage(1)).hasValueSatisfying(page -> assertThat(page.getLocation()).isEqualTo(2));
        assertThat(endpoint.getPage(2)).hasValueSatisfying(page -> assertThat(page.getLocation()).isEqualTo(2));
        assertThat(endpoint.getPage(3)).hasValueSatisfying(page -> assertThat(page.getLocation()).isEqualTo(3));
        assertThat(endpoint.getPage(4)).isNotPresent();
    }

    @Test
    public void can_investigate_page() {
        assertThat(endpoint.add("foo", "bar", "qux").getPage(2)).hasValueSatisfying(page -> {
            assertThat(page.hasLocation(0)).isTrue();
            assertThat(page.hasLocation(1)).isTrue();
            assertThat(page.hasLocation(2)).isTrue();
            assertThat(page.hasLocation(3)).isFalse();
            assertThat(page.getEntriesUntil(0)).extracting(InMemoryFeedEntry::getPayload).containsExactly("foo");
            assertThatThrownBy(() -> page.getEntriesUntil(3)).isInstanceOf(IllegalArgumentException.class);
            assertThat(page.getEntriesAfter(1)).extracting(InMemoryFeedEntry::getPayload).containsExactly("qux");
            assertThatThrownBy(() -> page.getEntriesAfter(3)).isInstanceOf(IllegalArgumentException.class);
            assertThat(page.getEntriesBetween(1, 2)).extracting(InMemoryFeedEntry::getPayload).containsExactly("qux");
            assertThatThrownBy(() -> page.getEntriesBetween(3, 3)).isInstanceOf(IllegalArgumentException.class);
        });
    }
}
