package no.skatteetaten.fastsetting.formueinntekt.felles.feed.persistent.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedProcessor;

public class PostgresPersistentFeedProcessor extends JdbcPersistentFeedProcessor {

    public PostgresPersistentFeedProcessor(FeedProcessor<String> delegate, DataSource dataSource) {
        super(delegate, dataSource);
    }

    @Override
    protected void write(Activation activation) {
        String pointer = getCurrentState().getPointer();
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO FEED_ACTIVATION (POINTER, ACTIVE) VALUES (?, ?) "
                + "ON CONFLICT (POINTER) "
                + "DO UPDATE SET ACTIVE = ?"
        )) {
            ps.setString(1, pointer);
            ps.setBoolean(2, activation == Activation.ACTIVE);
            ps.setBoolean(3, activation == Activation.ACTIVE);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
