package no.skatteetaten.fastsetting.formueinntekt.felles.feed.repository.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;

import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedRepository;
import org.hsqldb.jdbc.JDBCPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class JdbcFeedRepositoryTest {

    private FeedRepository<Long, String, Connection> repository;
    private JDBCPool dataSource;

    @Before
    public void setUp() throws Exception {
        dataSource = new JDBCPool();
        dataSource.setUrl("jdbc:hsqldb:mem:feed" + ThreadLocalRandom.current().nextLong());
        dataSource.setUser("sa");
        dataSource.setPassword("");

        try (Connection conn = dataSource.getConnection()) {
            Liquibase liquibase = new Liquibase(JdbcFeedRepository.CHANGE_LOG,
                new ClassLoaderResourceAccessor(),
                DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(conn)));
            liquibase.update(new Contexts());
        }

        repository = JdbcFeedRepository.ofSimpleNumericState(dataSource);
    }

    @After
    public void tearDown() throws Exception {
        dataSource.close(0);
    }

    @Test
    public void can_store_read_and_delete_pointer() {
        assertThat(repository.readCurrent("foo")).isNotPresent();
        repository.updateCurrent("foo", 1L, transaction -> { });
        assertThat(repository.readCurrent("foo")).contains(1L);
        repository.delete("foo", transaction -> { });
        assertThat(repository.readCurrent("foo")).isNotPresent();
    }

    @Test
    public void can_transit_pointer() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            repository.transitCurrent(conn, "foo", null, 1L);
            repository.transitCurrent(conn, "foo", 1L, 2L);
        }
        assertThat(repository.readCurrent("foo")).contains(2L);
    }

    @Test(expected = IllegalArgumentException.class)
    public void can_fail_transit_different_deletion() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            repository.transitCurrent(conn, "foo", 1L, 2L);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void can_fail_transit_unknown_deletion() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            repository.transitCurrent(conn, "foo", null, 1L);
            repository.transitCurrent(conn, "foo", 2L, 3L);
        }
    }
}
