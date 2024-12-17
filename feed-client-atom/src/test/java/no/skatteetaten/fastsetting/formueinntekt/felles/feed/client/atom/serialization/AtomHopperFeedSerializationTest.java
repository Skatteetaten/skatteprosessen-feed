package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom.serialization;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom.hopper.AtomHopperFeedLocation;
import org.junit.Test;

public class AtomHopperFeedSerializationTest {

    @Test
    public void kan_serialize_atom_hopper_location() {
        Map<String, String> serialization = new AtomHopperFeedSerializer().apply(new AtomHopperFeedLocation(
            "foo", AtomHopperFeedLocation.Direction.FORWARD
        ));
        assertThat(serialization).containsEntry("MARKER", "foo");
        assertThat(serialization).containsEntry("DIRECTION", "FORWARD");
    }

    @Test
    public void kan_deserialize_atom_hopper_location() {
        Map<String, String> serialization = new HashMap<>();
        serialization.put("MARKER", "foo");
        serialization.put("DIRECTION", "FORWARD");
        assertThat(new AtomHopperFeedDeserializer().apply(serialization)).isEqualTo(new AtomHopperFeedLocation(
            "foo", AtomHopperFeedLocation.Direction.FORWARD
        )).extracting(AtomHopperFeedLocation::getDirection).isEqualTo(AtomHopperFeedLocation.Direction.FORWARD);
    }
}
