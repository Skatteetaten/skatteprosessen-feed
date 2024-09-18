package no.skatteetaten.fastsetting.formueinntekt.felles.feed.publisher.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.Function;

public class OracleFeedPublisher<PAYLOAD> extends JdbcFeedPublisher<PAYLOAD> {

    public OracleFeedPublisher(
        String name,
        boolean concurrent,
        Function<PAYLOAD, String> serializer,
        Function<String, PAYLOAD> deserializer
    ) {
        super(name, concurrent, serializer, deserializer);
    }

    @Override
    public boolean initialize(Connection conn) {
        try (PreparedStatement ps = conn.prepareStatement(
            "MERGE INTO FEED_PUBLICATION_SEQUENCE "
                + "USING DUAL "
                + "ON (NAME = ?) "
                + "WHEN NOT MATCHED THEN INSERT (NAME) VALUES (?)"
        )) {
            ps.setString(1, name);
            ps.setString(2, name);
            return ps.executeUpdate() == 1;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
