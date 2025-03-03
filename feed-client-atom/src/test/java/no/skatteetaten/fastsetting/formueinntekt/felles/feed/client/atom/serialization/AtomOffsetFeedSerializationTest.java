package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom.serialization;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom.offset.AtomOffsetFeedLocation;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class AtomOffsetFeedSerializationTest {

    @Test
    public void kan_serialization_atom_offset_location() {
        Map<String, String> serialization = new AtomOffsetFeedSerializer().apply(new AtomOffsetFeedLocation(
            42, AtomOffsetFeedLocation.Direction.FORWARD
        ));
        assertThat(serialization).containsEntry("OFFSET", "42");
        assertThat(serialization).containsEntry("DIRECTION", "FORWARD");
    }

    @Test
    public void kan_deserialize_atom_offset_location() {
        Map<String, String> serialization = new HashMap<>();
        serialization.put("OFFSET", "42");
        serialization.put("DIRECTION", "FORWARD");
        Assertions.assertThat(new AtomOffsetFeedDeserializer().apply(serialization)).isEqualTo(new AtomOffsetFeedLocation(
            42, AtomOffsetFeedLocation.Direction.FORWARD
        )).extracting(AtomOffsetFeedLocation::getDirection).isEqualTo(AtomOffsetFeedLocation.Direction.FORWARD);
    }
}
