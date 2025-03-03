package no.skatteetaten.fastsetting.formueinntekt.felles.feed.publisher.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedProcessor;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedPublisher;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;

@Category(PostgreSQLContainer.class)
@RunWith(Parameterized.class)
public class PostgresTaskRepositoryTest extends AbstractJdbcFeedPublisherTest<HikariDataSource> {

    private final boolean concurrent;

    public PostgresTaskRepositoryTest(boolean concurrent) {
        this.concurrent = concurrent;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
            {false},
            {true}
        });
    }

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
    protected <S> FeedPublisher<S, Connection> publisher(String name, Function<S, String> serializer, Function<String, S> deserializer) {
        return new PostgresFeedPublisher<>(name, concurrent, serializer, deserializer);
    }
}
