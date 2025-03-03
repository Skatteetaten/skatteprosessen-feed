package no.skatteetaten.fastsetting.formueinntekt.felles.feed.persistent.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.sql.DataSource;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedProcessor;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedState;

public class JdbcPersistentFeedProcessor implements FeedProcessor<String> {

    public static final String CHANGE_LOG = "liquibase/feedAccessChangeLog.xml";

    private final FeedProcessor<String> delegate;
    protected final DataSource dataSource;

    public JdbcPersistentFeedProcessor(FeedProcessor<String> delegate, DataSource dataSource) {
        this.delegate = delegate;
        this.dataSource = dataSource;
    }

    @Override
    public synchronized boolean start(long timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException {
        if (delegate.start(timeout, timeUnit)) {
            write(Activation.ACTIVE);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public synchronized boolean stop(long timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException {
        if (delegate.stop(timeout, timeUnit)) {
            write(Activation.INACTIVE);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public synchronized void reset(long timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException {
        delegate.reset(timeout, timeUnit);
        write(Activation.INACTIVE);
    }

    @Override
    public synchronized void complete(long timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException {
        delegate.complete(timeout, timeUnit);
        write(Activation.INACTIVE);
    }

    @Override
    public FeedState<String> getCurrentState() {
        return delegate.getCurrentState();
    }

    @Override
    public Optional<Activation> findActivation() {
        String pointer = getCurrentState().getPointer();
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(
            "SELECT ACTIVE "
                + "FROM FEED_ACTIVATION "
                + "WHERE POINTER = ?")
        ) {
            ps.setString(1, pointer);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(rs.getBoolean("ACTIVE") ? Activation.ACTIVE : Activation.INACTIVE);
                } else {
                    return Optional.empty();
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public boolean initialize(long timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException {
        Optional<Activation> activation = findActivation();
        if (activation.isPresent()) {
            if (activation.get() == Activation.ACTIVE) {
                return delegate.initialize(timeout, timeUnit);
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean shutdown(long timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException {
        return delegate.shutdown(timeout, timeUnit);
    }

    protected void write(Activation activation) {
        String pointer = getCurrentState().getPointer();
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(
            "MERGE INTO FEED_ACTIVATION "
                + "USING (VALUES ?) AS TARGET (POINTER) "
                + "ON FEED_ACTIVATION.POINTER = TARGET.POINTER "
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
