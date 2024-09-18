package no.skatteetaten.fastsetting.formueinntekt.felles.feed.repository.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedRepository;

public class JdbcFeedRepository<LOCATION> implements FeedRepository<LOCATION, String, Connection> {

    public static final String CHANGE_LOG = "liquibase/feedStateChangeLog.xml";

    private final DataSource dataSource;

    private final Function<LOCATION, Map<String, String>> serializer;
    private final Function<Map<String, String>, LOCATION> deserializer;

    private final boolean transitAssertion;

    public JdbcFeedRepository(
        DataSource dataSource,
        Function<LOCATION, Map<String, String>> serializer,
        Function<Map<String, String>, LOCATION> deserializer
    ) {
        this.dataSource = dataSource;
        this.serializer = serializer;
        this.deserializer = deserializer;
        transitAssertion = true;
    }

    public JdbcFeedRepository(
        DataSource dataSource,
        Function<LOCATION, Map<String, String>> serializer,
        Function<Map<String, String>, LOCATION> deserializer,
        boolean transitAssertion
    ) {
        this.dataSource = dataSource;
        this.serializer = serializer;
        this.deserializer = deserializer;
        this.transitAssertion = transitAssertion;
    }

    public static JdbcFeedRepository<String> ofSimpleState(DataSource dataSource) {
        return ofSimpleState(dataSource, Function.identity(), Function.identity());
    }

    public static JdbcFeedRepository<Long> ofSimpleNumericState(DataSource dataSource) {
        return ofSimpleState(dataSource, location -> Long.toString(location), Long::valueOf);
    }

    public static <LOCATION> JdbcFeedRepository<LOCATION> ofSimpleState(
        DataSource dataSource, Function<LOCATION, String> serializer, Function<String, LOCATION> deserializer
    ) {
        return new JdbcFeedRepository<>(
            dataSource,
            serializer.andThen(value -> Collections.singletonMap("LOCATION", value)),
            ((Function<Map<String, String>, String>) values -> values.get("LOCATION")).andThen(deserializer)
        );
    }

    @Override
    public Map<Category, LOCATION> read(String pointer) {
        try (Connection conn = dataSource.getConnection()) {
            return doRead(conn, pointer);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    Map<Category, LOCATION> doRead(Connection conn, String pointer) {
        try (PreparedStatement ps = conn.prepareStatement(
            "SELECT CATEGORY, KEY, VALUE FROM FEED_STATE WHERE POINTER = ?"
        )) {
            ps.setString(1, pointer);
            Map<Category, Map<String, String>> categories = new EnumMap<>(Category.class);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String key = rs.getString("KEY"), value = rs.getString("VALUE");
                    categories.compute(Category.valueOf(rs.getString("CATEGORY")), (category, state) -> {
                        if (state == null) {
                            state = new HashMap<>();
                        }
                        state.put(key, value);
                        return state;
                    });
                }
                return categories.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                    try {
                        return deserializer.apply(entry.getValue());
                    } catch (Exception e) {
                        throw new PointerDeserializationException(entry.getValue(), e);
                    }
                }));
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void update(String pointer, Map<Category, LOCATION> locations, Consumer<? super Connection> callback) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                delete(conn, pointer);
                doInsert(conn, pointer, locations);
                callback.accept(conn);
                conn.commit();
            } catch (Throwable t) {
                conn.rollback();
                throw t;
            } finally {
                conn.setAutoCommit(false);
            }
        } catch (SQLException e) {
            throw new IllegalStateException();
        }
    }

    void doInsert(Connection conn, String pointer, Map<Category, LOCATION> locations) {
        try (PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO FEED_STATE (POINTER, CATEGORY, KEY, VALUE) VALUES (?, ?, ?, ?)"
        )) {
            for (Map.Entry<Category, LOCATION> location : locations.entrySet()) {
                for (Map.Entry<String, String> entry : serializer.apply(location.getValue()).entrySet()) {
                    ps.setString(1, pointer);
                    ps.setString(2, location.getKey().name());
                    ps.setString(3, entry.getKey());
                    ps.setString(4, entry.getValue());
                    ps.addBatch();
                }
            }
            ps.executeBatch();
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void delete(String pointer, Consumer<? super Connection> callback) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                delete(conn, pointer);
                callback.accept(conn);
                conn.commit();
            } catch (Throwable t) {
                conn.rollback();
                throw t;
            } finally {
                conn.setAutoCommit(false);
            }
        } catch (SQLException e) {
            throw new IllegalStateException();
        }
    }

    @Override
    public void transit(Connection conn, String pointer, Map<Category, LOCATION> from, Map<Category, LOCATION> to) {
        if (to.isEmpty()) {
            throw new IllegalArgumentException("Cannot set update to empty location: " + to);
        }
        if (transitAssertion) {
            if (!from.isEmpty()) {
                int deletions = 0, deleted;
                try (PreparedStatement ps = conn.prepareStatement(
                    "DELETE FROM FEED_STATE WHERE POINTER = ? AND CATEGORY = ? AND KEY = ? AND VALUE = ?"
                )) {
                    for (Map.Entry<Category, LOCATION> location : from.entrySet()) {
                        Map<String, String> values = serializer.apply(location.getValue());
                        for (Map.Entry<String, String> entry : values.entrySet()) {
                            ps.setString(1, pointer);
                            ps.setString(2, location.getKey().name());
                            ps.setString(3, entry.getKey());
                            ps.setString(4, entry.getValue());
                            ps.addBatch();
                        }
                        deletions += values.size();
                    }
                    deleted = IntStream.of(ps.executeBatch()).sum();
                } catch (SQLException e) {
                    throw new IllegalStateException(e);
                }
                if (deleted != deletions) {
                    throw new IllegalArgumentException("Expected to delete " + deletions + " previous locations instead of " + deleted + ": " + from);
                }
            }
            Map<Category, LOCATION> locations = doRead(conn, pointer);
            if (!locations.isEmpty()) {
                throw new IllegalArgumentException("Expected that " + from + " contained all previous locations but found " + locations);
            }
        } else {
            try (PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM FEED_STATE WHERE POINTER = ?"
            )) {
                ps.setString(1, pointer);
                ps.executeUpdate();
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }
        doInsert(conn, pointer, to);
    }

    public static void delete(DataSource dataSource, String pointer) {
        try (Connection conn = dataSource.getConnection()) {
            delete(conn, pointer);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    public static void delete(Connection conn, String pointer) {
        try (PreparedStatement ps = conn.prepareStatement(
            "DELETE FROM FEED_STATE WHERE POINTER = ?"
        )) {
            ps.setString(1, pointer);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
