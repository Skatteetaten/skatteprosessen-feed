package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom.offset;

public class AtomOffsetFeedLocation implements Comparable<AtomOffsetFeedLocation> {

    private final long offset;
    private final Direction direction;

    public AtomOffsetFeedLocation(long offset, Direction direction) {
        this.offset = offset;
        this.direction = direction;
    }

    public long getOffset() {
        return offset;
    }

    long getRequestOffset() {
        return direction == Direction.FORWARD ? offset + 1 : offset;
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
        AtomOffsetFeedLocation that = (AtomOffsetFeedLocation) object;
        return offset == that.offset;
    }

    @Override
    public int hashCode() {
        return (int) (offset ^ (offset >>> 32));
    }

    @Override
    public int compareTo(AtomOffsetFeedLocation other) {
        return Long.compare(offset, other.offset);
    }

    @Override
    public String toString() {
        return offset + ":" + direction;
    }

    public enum Direction {
        FORWARD,
        BACKWARD
    }
}
