package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.publisher;

public class PublishedFeedLocation {

    private final long sequence;
    private final Direction direction;

    public PublishedFeedLocation(long sequence, Direction direction) {
        this.sequence = sequence;
        this.direction = direction;
    }

    public long getSequence() {
        return sequence;
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
        PublishedFeedLocation that = (PublishedFeedLocation) object;
        return sequence == that.sequence;
    }

    @Override
    public int hashCode() {
        return (int) (sequence ^ (sequence >>> 32));
    }

    @Override
    public String toString() {
        return sequence + ":" + direction;
    }

    public enum Direction {

        FORWARD,
        BACKWARD
    }
}
