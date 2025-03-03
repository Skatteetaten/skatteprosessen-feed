package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom.serialization;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom.AtomFeedLocation;
import org.junit.Test;

public class AtomFeedSerializationTest {

    @Test
    public void kan_serialize_atom_location() {
        Map<String, String> serialization = new AtomFeedSerializer().apply(new AtomFeedLocation("foo", "bar", "qux"));
        assertThat(serialization).containsEntry("PAGE", "foo");
        assertThat(serialization).containsEntry("ENTRY", "bar");
        assertThat(serialization).containsEntry("ETAG", "qux");
    }

    @Test
    public void kan_deserialize_atom_location() {
        Map<String, String> serialization = new HashMap<>();
        serialization.put("PAGE", "foo");
        serialization.put("ENTRY", "bar");
        serialization.put("ETAG", "qux");
        AtomFeedLocation location = new AtomFeedDeserializer().apply(serialization);
        assertThat(location).isEqualTo(new AtomFeedLocation("foo", "bar"));
        assertThat(location.getEtag()).contains("qux");
    }
}
