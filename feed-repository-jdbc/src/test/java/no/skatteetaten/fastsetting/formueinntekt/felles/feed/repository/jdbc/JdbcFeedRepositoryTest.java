package no.skatteetaten.fastsetting.formueinntekt.felles.feed.repository.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.ThreadLocalRandom;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedRepository;
import org.hsqldb.jdbc.JDBCPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;

public class JdbcFeedRepositoryTest {

    private FeedRepository<Long, String, Connection> repository;
    private JDBCPool dataSource;

    @Before
    public void setUp() throws Exception {
        dataSource = new JDBCPool();
        dataSource.setUrl("jdbc:hsqldb:mem:feed" + ThreadLocalRandom.current().nextLong());
        dataSource.setUser("sa");
        dataSource.setPassword("");

        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("SET DATABASE SQL SYNTAX ORA TRUE");
        }

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
        repository.updateCurrent("foo", 1L);
        assertThat(repository.readCurrent("foo")).contains(1L);
        repository.delete("foo");
        assertThat(repository.readCurrent("foo")).isNotPresent();
    }
}