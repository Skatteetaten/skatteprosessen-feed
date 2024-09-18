package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.filesystem;

import java.util.Objects;
import java.util.stream.IntStream;

public class FileSystemLocation {

    private final String file;
    private final Direction direction;

    public FileSystemLocation(String file, Direction direction) {
        this.file = file;
        this.direction = direction;
    }

    public String getFile() {
        return file;
    }

    public Direction getDirection() {
        return direction;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        FileSystemLocation that = (FileSystemLocation) other;
        return Objects.equals(file, that.file) && direction == that.direction;
    }

    @Override
    public int hashCode() {
        return Objects.hash(file, direction);
    }

    @Override
    public String toString() {
        return file + "/" + direction.name();
    }

    public enum Direction {

        FORWARD(false, 1),
        BACKWARD_INCLUSIVE(true, -1, 0),
        BACKWARD_EXCLUSIVE(true, -1);

        private final boolean reversed;

        private final int[] values;

        Direction(boolean reversed, int... values) {
            this.reversed = reversed;
            this.values = values;
        }

        boolean isReversed() {
            return reversed;
        }

        boolean includes(int comparison) {
            return IntStream.of(values).anyMatch(value -> comparison == value);
        }
    }
}
