package no.skatteetaten.fastsetting.formueinntekt.felles.feed.publisher.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedPublisher;
import org.hsqldb.jdbc.JDBCPool;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class HsqldbFeedPublisherTest extends AbstractJdbcFeedPublisherTest<JDBCPool> {

    private final boolean concurrent;

    public HsqldbFeedPublisherTest(boolean concurrent) {
        this.concurrent = concurrent;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
            {false},
            {true}
        });
    }

    @Override
    protected JDBCPool dataSource() {
        JDBCPool dataSource = new JDBCPool();
        dataSource.setUrl("jdbc:hsqldb:mem:feed" + ThreadLocalRandom.current().nextLong());
        dataSource.setUser("sa");
        dataSource.setPassword("");
        return dataSource;
    }

    @Override
    protected void shutdown(JDBCPool dataSource) throws SQLException {
        dataSource.close(0);
    }

    @Override
    protected <S> FeedPublisher<S, Connection> publisher(String name, Function<S, String> serializer, Function<String, S> deserializer) {
        return new JdbcFeedPublisher<>(name, concurrent, serializer, deserializer);
    }
}
