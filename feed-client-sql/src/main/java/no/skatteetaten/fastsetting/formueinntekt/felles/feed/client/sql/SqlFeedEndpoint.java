package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.sql.DataSource;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedEndpoint;

public class SqlFeedEndpoint implements FeedEndpoint<SqlFeedLocation, SqlFeedEntry, SqlFeedPage> {

    private final DataSource dataSource;
    private final String table;
    private final List<String> columns;

    // DB2 does not allow the size to be set as a query parameter, therefore it is encoded as a literal.
    private final int size;

    private final SqlValueReader reader;
    private final SqlValueWriter writer;

    public SqlFeedEndpoint(DataSource dataSource, String table, String column, int size) {
        this.dataSource = dataSource;
        this.table = table;
        columns = Collections.singletonList(column);
        this.size = size;
        reader = ResultSet::getString;
        writer = PreparedStatement::setString;
    }

    public SqlFeedEndpoint(
        DataSource dataSource, String table, List<String> columns, int size,
        SqlValueReader reader, SqlValueWriter writer
    ) {
        this.dataSource = dataSource;
        this.table = table;
        this.columns = columns;
        this.size = size;
        this.reader = reader;
        this.writer = writer;
    }

    @Override
    public Optional<SqlFeedPage> getFirstPage() {
        try (
            Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(
                "SELECT " + (columns.size() == 1 ? "DISTINCT " : "") + String.join(", ", columns) + " "
                    + "FROM " + table + " "
                    + "ORDER BY " + columns.get(0) + " ASC "
                    + "FETCH FIRST " + size + " ROWS ONLY"
            )
        ) {
            List<SqlFeedEntry> entries = new ArrayList<>(size);
            while (rs.next()) {
                entries.add(SqlFeedEntry.of(columns, reader, SqlFeedLocation.Direction.BACKWARD_INCLUSIVE, rs));
            }
            return entries.isEmpty() ? Optional.empty() : Optional.of(new SqlFeedPage(
                entries.get(entries.size() - 1).getLocation(), entries, true
            ));
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Optional<SqlFeedPage> getLastPage() {
        try (
            Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(
                "SELECT " + (columns.size() == 1 ? "DISTINCT " : "") + String.join(", ", columns) + " "
                    + "FROM " + table + " "
                    + "ORDER BY " + columns.get(0) + " DESC "
                    + "FETCH FIRST " + size + " ROWS ONLY"
            )
        ) {
            List<SqlFeedEntry> entries = new ArrayList<>(size);
            while (rs.next()) {
                entries.add(SqlFeedEntry.of(columns, reader, SqlFeedLocation.Direction.BACKWARD_INCLUSIVE, rs));
            }
            Collections.reverse(entries);
            return entries.isEmpty() ? Optional.empty() : Optional.of(new SqlFeedPage(
                entries.get(entries.size() - 1).getLocation(), entries, false
            ));
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Optional<SqlFeedLocation> getLastLocation() {
        try (
            Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT MAX(" + columns.get(0) + ") FROM " + table)
        ) {
            if (rs.next()) {
                Object value = reader.apply(rs, 1);
                return value == null
                    ? Optional.empty()
                    : Optional.of(new SqlFeedLocation(value.toString(), SqlFeedLocation.Direction.FORWARD));
            } else {
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Optional<SqlFeedPage> getPage(SqlFeedLocation location) {
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(
            "SELECT " + (columns.size() == 1 ? "DISTINCT " : "") + String.join(", ", columns) + " "
                + "FROM " + table + " "
                + "WHERE " + columns.get(0) + " " + location.getDirection().getOperator() + " ? "
                + "ORDER BY " + columns.get(0) + " " + location.getDirection().getOrdering() + " "
                + "FETCH FIRST " + size + " ROWS ONLY"
        )) {
            writer.apply(ps, 1, location.getValue());
            try (ResultSet rs = ps.executeQuery()) {
                List<SqlFeedEntry> entries = new ArrayList<>(size);
                while (rs.next()) {
                    entries.add(SqlFeedEntry.of(columns, reader, location.getDirection(), rs));
                }
                if (location.getDirection() != SqlFeedLocation.Direction.FORWARD) {
                    Collections.reverse(entries);
                }
                return entries.isEmpty()
                    ? Optional.empty()
                    : Optional.of(new SqlFeedPage(location, entries, false));
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @FunctionalInterface
    public interface SqlValueReader {
        Object apply(ResultSet rs, int column) throws SQLException;
    }

    @FunctionalInterface
    public interface SqlValueWriter {
        void apply(PreparedStatement ps, int column, String value) throws SQLException;
    }
}
