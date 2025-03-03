package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.sql;

public class SqlFeedLocation {

    private final String value;
    private final Direction direction;

    public SqlFeedLocation(String value, Direction direction) {
        this.value = value;
        this.direction = direction;
    }

    public String getValue() {
        return value;
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
        SqlFeedLocation that = (SqlFeedLocation) object;
        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public String toString() {
        return value + ":" + direction;
    }

    public enum Direction {

        FORWARD(">", "ASC"),
        BACKWARD_INCLUSIVE("<=", "DESC"),
        BACKWARD_EXCLUSIVE("<", "DESC");

        private final String operator, ordering;

        Direction(String operator, String ordering) {
            this.operator = operator;
            this.ordering = ordering;
        }

        String getOperator() {
            return operator;
        }

        String getOrdering() {
            return ordering;
        }
    }
}
