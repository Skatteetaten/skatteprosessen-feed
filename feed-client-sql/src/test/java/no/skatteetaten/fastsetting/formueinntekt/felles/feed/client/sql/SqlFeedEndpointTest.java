package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ThreadLocalRandom;

import org.hsqldb.jdbc.JDBCPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SqlFeedEndpointTest {

    private JDBCPool dataSource;

    private SqlFeedEndpoint endpoint;

    @Before
    public void setUp() throws Exception {
        dataSource = new JDBCPool();
        dataSource.setUrl("jdbc:hsqldb:mem:feed" + ThreadLocalRandom.current().nextLong());
        dataSource.setUser("sa");
        dataSource.setPassword("");

        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE FOO (BAR VARCHAR(100) NOT NULL)");
        }

        endpoint = new SqlFeedEndpoint(dataSource, "FOO", "BAR", 2);
    }

    @After
    public void tearDown() throws Exception {
        dataSource.close(0);
    }

    @Test
    public void can_read_first_page() throws SQLException {
        insert("1", "2", "3");

        assertThat(endpoint.getFirstPage()).hasValueSatisfying(page -> {
            assertThat(page.getLocation().getValue()).isEqualTo("2");
            assertThat(page.getLocation().getDirection()).isEqualTo(SqlFeedLocation.Direction.BACKWARD_INCLUSIVE);
            assertThat(page.getEntries()).extracting(entry -> entry.getLocation().getValue()).containsExactly("1", "2");
            assertThat(page.getPreviousLocation()).isNotPresent();
            assertThat(page.getNextLocation()).contains(new SqlFeedLocation("2", SqlFeedLocation.Direction.FORWARD));
        });
    }

    @Test
    public void can_read_first_page_empty() {
        assertThat(endpoint.getFirstPage()).isNotPresent();
    }

    @Test
    public void can_read_last_page() throws SQLException {
        insert("1", "2", "3");

        assertThat(endpoint.getLastPage()).hasValueSatisfying(page -> {
            assertThat(page.getLocation().getValue()).isEqualTo("3");
            assertThat(page.getLocation().getDirection()).isEqualTo(SqlFeedLocation.Direction.BACKWARD_INCLUSIVE);
            assertThat(page.getEntries()).extracting(entry -> entry.getLocation().getValue()).containsExactly("2", "3");
            assertThat(page.getPreviousLocation()).contains(new SqlFeedLocation(
                "2", SqlFeedLocation.Direction.BACKWARD_INCLUSIVE
            ));
            assertThat(page.getNextLocation()).contains(new SqlFeedLocation("3", SqlFeedLocation.Direction.FORWARD));
        });
    }

    @Test
    public void can_read_last_page_empty() {
        assertThat(endpoint.getLastPage()).isNotPresent();
    }

    @Test
    public void can_read_last_location() throws SQLException {
        insert("1", "2", "3");

        assertThat(endpoint.getLastLocation()).contains(new SqlFeedLocation("3", SqlFeedLocation.Direction.FORWARD));
    }

    @Test
    public void can_read_last_location_empty() {
        assertThat(endpoint.getLastLocation()).isNotPresent();
    }

    @Test
    public void can_read_page() throws SQLException {
        insert("1", "2", "3");

        assertThat(endpoint.getPage(
            new SqlFeedLocation("1", SqlFeedLocation.Direction.FORWARD)
        )).hasValueSatisfying(page -> {
            assertThat(page.getLocation().getValue()).isEqualTo("1");
            assertThat(page.getLocation().getDirection()).isEqualTo(SqlFeedLocation.Direction.FORWARD);
            assertThat(page.getEntries()).extracting(entry -> entry.getLocation().getValue()).containsExactly("2", "3");
            assertThat(page.getPreviousLocation()).contains(new SqlFeedLocation(
                "2", SqlFeedLocation.Direction.BACKWARD_INCLUSIVE
            ));
            assertThat(page.getNextLocation()).contains(new SqlFeedLocation("3", SqlFeedLocation.Direction.FORWARD));
        });
    }

    @Test
    public void can_read_page_duplicates() throws SQLException {
        insert("1", "1", "2", "2", "3", "3");

        assertThat(endpoint.getPage(
            new SqlFeedLocation("1", SqlFeedLocation.Direction.FORWARD)
        )).hasValueSatisfying(page -> {
            assertThat(page.getLocation().getValue()).isEqualTo("1");
            assertThat(page.getLocation().getDirection()).isEqualTo(SqlFeedLocation.Direction.FORWARD);
            assertThat(page.getEntries()).extracting(entry -> entry.getLocation().getValue()).containsExactly("2", "3");
            assertThat(page.getPreviousLocation()).contains(new SqlFeedLocation(
                "2", SqlFeedLocation.Direction.BACKWARD_INCLUSIVE
            ));
            assertThat(page.getNextLocation()).contains(new SqlFeedLocation("3", SqlFeedLocation.Direction.FORWARD));
        });
    }

    private void insert(String... values) throws SQLException {
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO FOO (BAR) VALUES (?)"
        )) {
            for (String value : values) {
                ps.setString(1, value);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }
}
