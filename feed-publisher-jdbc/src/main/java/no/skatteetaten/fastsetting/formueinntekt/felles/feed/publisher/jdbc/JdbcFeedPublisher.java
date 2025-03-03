package no.skatteetaten.fastsetting.formueinntekt.felles.feed.publisher.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedPublisher;

public class JdbcFeedPublisher<PAYLOAD> implements FeedPublisher<PAYLOAD, Connection> {

    public static final String CHANGE_LOG = "liquibase/feedPublisherChangeLog.xml";

    final String name;

    private final boolean concurrent;

    final Function<PAYLOAD, String> serializer;

    final Function<String, PAYLOAD> deserializer;

    public JdbcFeedPublisher(
        String name,
        boolean concurrent,
        Function<PAYLOAD, String> serializer,
        Function<String, PAYLOAD> deserializer
    ) {
        this.name = name;
        this.concurrent = concurrent;
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    public static FeedPublisher<String, Connection> ofTextualPayload(String name, boolean concurrent) {
        return new JdbcFeedPublisher<>(name, concurrent, Function.identity(), Function.identity());
    }

    @Override
    public boolean initialize(Connection conn) {
        try (PreparedStatement ps = conn.prepareStatement(
            "MERGE INTO FEED_PUBLICATION_SEQUENCE "
                + "USING (VALUES ?) AS TARGET (NAME) "
                + "ON FEED_PUBLICATION_SEQUENCE.NAME = TARGET.NAME "
                + "WHEN NOT MATCHED THEN INSERT (NAME) VALUES (?)"
        )) {
            ps.setString(1, name);
            ps.setString(2, name);
            return ps.executeUpdate() == 1;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void publish(Connection conn, Collection<? extends PAYLOAD> payloads) {
        if (payloads.isEmpty()) {
            return;
        }
        try {
            long offset;
            try (PreparedStatement ps = conn.prepareStatement("SELECT SEQUENCE FROM FEED_PUBLICATION_SEQUENCE WHERE NAME = ?" + (concurrent ? " FOR UPDATE" : ""))) {
                ps.setString(1, name);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw new IllegalStateException("Did not find sequence entry for " + name);
                    }
                    offset = rs.getLong("SEQUENCE");
                }
            }
            doInsert(conn, payloads, offset);
            try (PreparedStatement ps = conn.prepareStatement("UPDATE FEED_PUBLICATION_SEQUENCE SET SEQUENCE = ? WHERE NAME = ? AND SEQUENCE = ?")) {
                ps.setLong(1, offset + payloads.size());
                ps.setString(2, name);
                ps.setLong(3, offset);
                if (ps.executeUpdate() != 1) {
                    throw new IllegalStateException("Failed to increment offset for " + name + " from " + offset + " by " + payloads.size());
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    void doInsert(Connection conn, Collection<? extends PAYLOAD> payloads, long offset) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO FEED_PUBLICATION (NAME, SEQUENCE, PAYLOAD) VALUES (?, ?, ?)")) {
            for (PAYLOAD payload : payloads) {
                ps.setString(1, name);
                ps.setLong(2, ++offset);
                ps.setString(3, serializer.apply(payload));
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    @Override
    public List<Entry<PAYLOAD>> page(Connection conn, long offset, int size, boolean backwards) {
        if (size < 0) {
            throw new IllegalArgumentException();
        } else if (size == 0) {
            return Collections.emptyList();
        } else if (backwards ? offset <= INCEPTION : offset == Long.MAX_VALUE) {
            return Collections.emptyList();
        }
        List<Entry<PAYLOAD>> entries = new ArrayList<>(size);
        try (PreparedStatement ps = conn.prepareStatement(
            "SELECT SEQUENCE, PAYLOAD "
                + "FROM FEED_PUBLICATION "
                + "WHERE NAME = ? "
                + "AND SEQUENCE " + (backwards ? "<=" : ">") + " ? "
                + "ORDER BY SEQUENCE " + (backwards ? "DESC" : "ASC") + " "
                + "FETCH FIRST ? ROWS ONLY"
        )) {
            ps.setString(1, name);
            ps.setLong(2, offset);
            ps.setInt(3, size);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    entries.add(new Entry<>(rs.getLong("SEQUENCE"), deserializer.apply(rs.getString("PAYLOAD"))));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
        return entries;
    }

    @Override
    public long limit(Connection conn) {
        try (PreparedStatement ps = conn.prepareStatement("SELECT SEQUENCE FROM FEED_PUBLICATION_SEQUENCE WHERE NAME = ?")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("Did not find sequence entry for " + name);
                }
                return rs.getLong("SEQUENCE");
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void reset(Connection conn) {
        long updated;
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM FEED_PUBLICATION WHERE NAME = ?")) {
            ps.setString(1, name);
            updated = ps.executeLargeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
        if (updated > 0) {
            try (PreparedStatement ps = conn.prepareStatement("UPDATE FEED_PUBLICATION_SEQUENCE SET SEQUENCE = ? WHERE NAME = ?")) {
                ps.setInt(1, 0);
                ps.setString(2, name);
                if (ps.executeUpdate() != 1) {
                    throw new IllegalStateException("Expected to update exactly one feed publication sequence for " + name);
                }
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}

