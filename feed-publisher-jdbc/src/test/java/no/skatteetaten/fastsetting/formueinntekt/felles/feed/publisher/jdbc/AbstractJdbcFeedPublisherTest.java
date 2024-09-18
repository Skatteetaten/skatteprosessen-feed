package no.skatteetaten.fastsetting.formueinntekt.felles.feed.publisher.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Function;

import javax.sql.DataSource;
import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedPublisher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public abstract class AbstractJdbcFeedPublisherTest<T extends DataSource> {

    private static final String FOO = "foo";

    private T dataSource;

    private FeedPublisher<Integer, Connection> publisher;

    @Before
    public void setUp() throws Exception {
        dataSource = dataSource();

        try (Connection conn = dataSource.getConnection()) {
            Liquibase liquibase = new Liquibase(JdbcFeedPublisher.CHANGE_LOG,
                new ClassLoaderResourceAccessor(),
                DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(conn)));
            liquibase.update(new Contexts());
        }

        publisher = publisher(FOO, i -> Integer.toString(i), Integer::parseInt);
    }

    @After
    public void tearDown() throws Exception {
        shutdown(dataSource);
    }

    protected abstract T dataSource() throws SQLException;

    protected abstract void shutdown(T dataSource) throws SQLException;

    protected abstract <S> FeedPublisher<S, Connection> publisher(String name, Function<S, String> serializer, Function<String, S> deserializer);

    @Test
    public void can_initialize() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            assertThat(publisher.initialize(conn)).isTrue();
            assertThat(publisher.initialize(conn)).isFalse();
            conn.commit();
        }
    }

    @Test
    public void can_publish_and_lookup() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            assertThat(publisher.initialize(conn)).isTrue();
            assertThat(publisher.payload(conn, -1)).isEmpty();
            assertThat(publisher.payload(conn, 0)).isEmpty();
            assertThat(publisher.payload(conn, 1)).isEmpty();
            assertThat(publisher.payload(conn, 2)).isEmpty();
            publisher.publish(conn, -1);
            assertThat(publisher.payload(conn, 0)).isEmpty();
            assertThat(publisher.payload(conn, 1)).contains(-1);
            assertThat(publisher.payload(conn, 2)).isEmpty();
            conn.commit();
        }
    }

    @Test
    public void can_page_forwards() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            assertThat(publisher.initialize(conn)).isTrue();
            publisher.publish(conn, -1, -2, -3);
            assertThat(publisher.page(conn, 0, 2, false)).contains(
                new FeedPublisher.Entry<>(1, -1),
                new FeedPublisher.Entry<>(2, -2)
            );
            assertThat(publisher.page(conn, 2, 2, false)).contains(
                new FeedPublisher.Entry<>(3, -3)
            );
            assertThat(publisher.page(conn, 3, 2, false)).isEmpty();
            conn.commit();
        }
    }

    @Test
    public void can_page_backwards() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            assertThat(publisher.initialize(conn)).isTrue();
            publisher.publish(conn, -1, -2, -3);
            assertThat(publisher.page(conn, Long.MAX_VALUE, 2, true)).contains(
                new FeedPublisher.Entry<>(3, -3),
                new FeedPublisher.Entry<>(2, -2)
            );
            assertThat(publisher.page(conn, 1, 2, true)).contains(
                new FeedPublisher.Entry<>(1, -1)
            );
            assertThat(publisher.page(conn, 0, 2, true)).isEmpty();
            conn.commit();
        }
    }

    @Test
    public void can_request_zero_size() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            assertThat(publisher.initialize(conn)).isTrue();
            publisher.publish(conn, -1, -2, -3);
            assertThat(publisher.page(conn, 0, 0, false)).isEmpty();
            conn.commit();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void cannot_request_negative_size() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            assertThat(publisher.initialize(conn)).isTrue();
            assertThat(publisher.page(conn, 0, -1, false)).isEmpty();
        }
    }

    @Test
    public void can_find_limit() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            assertThat(publisher.initialize(conn)).isTrue();
            assertThat(publisher.limit(conn)).isEqualTo(0);
            publisher.publish(conn, 1, 2, 3);
            assertThat(publisher.limit(conn)).isEqualTo(3);
            conn.commit();
        }
    }

    @Test
    public void can_reset() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            assertThat(publisher.initialize(conn)).isTrue();
            publisher.publish(conn, 1, 2, 3);
            assertThat(publisher.limit(conn)).isEqualTo(3);
            publisher.reset(conn);
            assertThat(publisher.limit(conn)).isEqualTo(0);
            conn.commit();
        }
    }
}
