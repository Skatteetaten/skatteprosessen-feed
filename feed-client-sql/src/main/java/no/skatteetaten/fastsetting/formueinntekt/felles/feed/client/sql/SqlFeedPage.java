package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.sql;

import java.util.List;
import java.util.Optional;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedPage;

public class SqlFeedPage implements FeedPage<SqlFeedLocation, SqlFeedEntry> {

    private final SqlFeedLocation location;

    private final List<SqlFeedEntry> entries;

    private final boolean first;

    SqlFeedPage(SqlFeedLocation location, List<SqlFeedEntry> entries, boolean first) {
        this.location = location;
        this.entries = entries;
        this.first = first;
    }

    @Override
    public SqlFeedLocation getLocation() {
        return location;
    }

    @Override
    public Optional<SqlFeedLocation> getPreviousLocation() {
        return first || entries.isEmpty() ? Optional.empty() : Optional.of(new SqlFeedLocation(
            entries.get(0).getLocation().getValue(),
            SqlFeedLocation.Direction.BACKWARD_EXCLUSIVE
        ));
    }

    @Override
    public Optional<SqlFeedLocation> getNextLocation() {
        return entries.isEmpty() ? Optional.empty() : Optional.of(new SqlFeedLocation(
            entries.get(entries.size() - 1).getLocation().getValue(),
            SqlFeedLocation.Direction.FORWARD
        ));
    }

    @Override
    public List<SqlFeedEntry> getEntries() {
        return entries;
    }

    @Override
    public Optional<String> toDisplayString() {
        StringBuilder sb = new StringBuilder();
        entries.forEach(entry -> sb.append(entry.getLocation())
            .append('\n')
            .append(entry.getValues())
            .append('\n')
            .append('\n'));
        return Optional.of(sb.toString());
    }

    @Override
    public String toString() {
        return "page@" + getLocation();
    }
}
