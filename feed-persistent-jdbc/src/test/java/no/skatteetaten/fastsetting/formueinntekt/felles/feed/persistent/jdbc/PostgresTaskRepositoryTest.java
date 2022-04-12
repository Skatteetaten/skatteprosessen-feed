package no.skatteetaten.fastsetting.formueinntekt.felles.feed.persistent.jdbc;

import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedProcessor;

@Category(PostgreSQLContainer.class)
public class PostgresTaskRepositoryTest extends AbstractJdbcPersistentFeedProcessorTest<HikariDataSource> {

    @Rule
    public JdbcDatabaseContainer<?> container = new PostgreSQLContainer<>("postgres:11");

    @Override
    protected HikariDataSource dataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(container.getJdbcUrl());
        config.setUsername(container.getUsername());
        config.setPassword(container.getPassword());
        return new HikariDataSource(config);
    }

    @Override
    protected void shutdown(HikariDataSource dataSource) {
        dataSource.close();
    }

    @Override
    protected JdbcPersistentFeedProcessor processor(FeedProcessor<String> delegate, HikariDataSource dataSource) {
        return new PostgresPersistentFeedProcessor(delegate, dataSource);
    }
}
