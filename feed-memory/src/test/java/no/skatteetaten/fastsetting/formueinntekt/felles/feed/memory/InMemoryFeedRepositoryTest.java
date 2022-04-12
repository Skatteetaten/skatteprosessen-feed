package no.skatteetaten.fastsetting.formueinntekt.felles.feed.memory;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class InMemoryFeedRepositoryTest {

    private InMemoryFeedRepository<Integer, String> repository = new InMemoryFeedRepository<>();

    @Test
    public void can_store_read_and_delete_pointer() {
        assertThat(repository.readCurrent("foo")).isNotPresent();
        repository.updateCurrent("foo", 1);
        assertThat(repository.readCurrent("foo")).contains(1);
        repository.delete("foo");
        assertThat(repository.readCurrent("foo")).isNotPresent();
        assertThat(repository.getPointers()).isEmpty();
    }
}
