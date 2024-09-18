package no.skatteetaten.fastsetting.formueinntekt.felles.feed.persistent.jdbc;

import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedProcessor;
import org.hsqldb.jdbc.JDBCPool;

public class HsqldbFeedProcessorTest extends AbstractJdbcPersistentFeedProcessorTest<JDBCPool> {

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
    protected JdbcPersistentFeedProcessor processor(FeedProcessor<String> delegate, JDBCPool dataSource) {
        return new JdbcPersistentFeedProcessor(delegate, dataSource);
    }
}
