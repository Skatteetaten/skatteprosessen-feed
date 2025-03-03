package no.skatteetaten.fastsetting.formueinntekt.felles.feed.memory;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.Test;

public class InMemoryFeedRepositoryTest {

    private InMemoryFeedRepository<Integer, String> repository = new InMemoryFeedRepository<>();

    @Test
    public void can_store_read_and_delete_pointer() {
        assertThat(repository.readCurrent("foo")).isNotPresent();
        repository.updateCurrent("foo", 1, transaction -> { });
        assertThat(repository.readCurrent("foo")).contains(1);
        repository.delete("foo", transaction -> { });
        assertThat(repository.readCurrent("foo")).isNotPresent();
        assertThat(repository.getPointers()).isEmpty();
    }

    @Test
    public void can_transit_pointer() {
        repository.transitCurrent(null, "foo", null, 1);
        repository.transitCurrent(null, "foo", 1, 2);
        assertThat(repository.readCurrent("foo")).contains(2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void can_fail_transit_different_deletion() {
        repository.transitCurrent(null, "foo", 1, 2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void can_fail_transit_unknown_deletion() {
        repository.transitCurrent(null, "foo", null, 1);
        repository.transitCurrent(null, "foo", 2, 3);
    }
}
