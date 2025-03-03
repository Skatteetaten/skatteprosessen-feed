package no.skatteetaten.fastsetting.formueinntekt.felles.feed.persistent.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedProcessor;

public class OraclePersistentFeedProcessor extends JdbcPersistentFeedProcessor {

    public OraclePersistentFeedProcessor(FeedProcessor<String> delegate, DataSource dataSource) {
        super(delegate, dataSource);
    }

    @Override
    protected void write(Activation activation) {
        String pointer = getCurrentState().getPointer();
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(
            "MERGE INTO FEED_ACTIVATION "
                + "USING DUAL "
                + "ON (POINTER = ?) "
                + "WHEN MATCHED THEN UPDATE SET ACTIVE = ? "
                + "WHEN NOT MATCHED THEN INSERT (POINTER, ACTIVE) VALUES (?, ?)"
        )) {
            ps.setString(1, pointer);
            ps.setBoolean(2, activation == Activation.ACTIVE);
            ps.setString(3, pointer);
            ps.setBoolean(4, activation == Activation.ACTIVE);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
