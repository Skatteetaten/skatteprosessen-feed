package no.skatteetaten.fastsetting.formueinntekt.felles.feed.publisher.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class PostgresFeedPublisher<PAYLOAD> extends JdbcFeedPublisher<PAYLOAD> {

    public PostgresFeedPublisher(
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
            "INSERT INTO FEED_PUBLICATION_SEQUENCE (NAME) VALUES (?) ON CONFLICT DO NOTHING"
        )) {
            ps.setString(1, name);
            return ps.executeUpdate() == 1;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    void doInsert(Connection conn, Collection<? extends PAYLOAD> payloads, long offset) throws SQLException {
        List<PAYLOAD> remaining = new ArrayList<>(payloads);
        do {
            int bulkSize = (int) Math.pow(2, (int) (Math.log(remaining.size()) / Math.log(2)));
            do {
                try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO FEED_PUBLICATION (NAME, SEQUENCE, PAYLOAD) VALUES " + String.join(", ", Collections.nCopies(bulkSize, "(?, ?, ?)"))
                )) {
                    for (int index = 0; index < bulkSize; index++) {
                        ps.setString(3 * index + 1, name);
                        ps.setLong(3 * index + 2, ++offset);
                        ps.setString(3 * index +  3, serializer.apply(remaining.get(index)));
                    }
                    ps.executeUpdate();
                }
                remaining = remaining.subList(bulkSize, remaining.size());
            } while (remaining.size() >= bulkSize);
        } while (!remaining.isEmpty());
    }
}
