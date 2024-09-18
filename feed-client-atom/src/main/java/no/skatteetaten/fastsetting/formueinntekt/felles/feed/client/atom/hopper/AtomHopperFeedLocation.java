package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom.hopper;

public class AtomHopperFeedLocation {

    private final String marker;
    private final Direction direction;

    public AtomHopperFeedLocation(String marker, Direction direction) {
        this.marker = marker;
        this.direction = direction;
    }

    public String getMarker() {
        return marker;
    }

    public Direction getDirection() {
        return direction;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        AtomHopperFeedLocation that = (AtomHopperFeedLocation) object;
        return marker.equals(that.marker);
    }

    @Override
    public int hashCode() {
        return marker.hashCode();
    }

    @Override
    public String toString() {
        return marker + ":" + direction;
    }

    public enum Direction {
        FORWARD,
        BACKWARD
    }
}
