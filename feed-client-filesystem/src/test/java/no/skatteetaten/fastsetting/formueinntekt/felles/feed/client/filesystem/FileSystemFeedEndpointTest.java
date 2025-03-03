package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.filesystem;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

public class FileSystemFeedEndpointTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private Path folder;

    private FileSystemFeedEndpoint endpoint;

    @Before
    public void setUp() throws Exception {
        folder = temporaryFolder.newFolder().toPath();
        endpoint = FileSystemFeedEndpoint.ofFilesInDefaultOrder(folder, 2);
    }

    @Test
    public void can_read_first_page() throws IOException {
        Path first = Files.createFile(folder.resolve("1")), second = Files.createFile(folder.resolve("2"));

        assertThat(endpoint.getFirstPage()).hasValueSatisfying(page -> {
            assertThat(page.getEntries()).hasSize(2);
            assertThat(page.getEntries().get(0).getFile()).isEqualTo(folder.relativize(first));
            assertThat(page.getEntries().get(0).getLocation().getFile()).isEqualTo(folder.relativize(first).toString());
            assertThat(page.getEntries().get(0).getLocation().getDirection()).isEqualTo(FileSystemLocation.Direction.BACKWARD_INCLUSIVE);
            assertThat(page.getEntries().get(1).getFile()).isEqualTo(folder.relativize(second));
            assertThat(page.getEntries().get(1).getLocation().getFile()).isEqualTo(folder.relativize(second).toString());
            assertThat(page.getEntries().get(1).getLocation().getDirection()).isEqualTo(FileSystemLocation.Direction.BACKWARD_INCLUSIVE);
            assertThat(page.getNextLocation()).hasValueSatisfying(location -> {
                assertThat(location.getFile()).isEqualTo(folder.relativize(second).toString());
                assertThat(location.getDirection()).isEqualTo(FileSystemLocation.Direction.FORWARD);
            });
            assertThat(page.getLocation().getFile()).isEqualTo(folder.relativize(second).toString());
            assertThat(page.getLocation().getDirection()).isEqualTo(FileSystemLocation.Direction.BACKWARD_INCLUSIVE);
            assertThat(page.getPreviousLocation()).hasValueSatisfying(location -> {
                assertThat(location.getFile()).isEqualTo(folder.relativize(first).toString());
                assertThat(location.getDirection()).isEqualTo(FileSystemLocation.Direction.BACKWARD_EXCLUSIVE);
            });
        });
    }

    @Test
    public void can_read_last_page() throws IOException {
        Path first = Files.createFile(folder.resolve("1")), second = Files.createFile(folder.resolve("2"));

        assertThat(endpoint.getLastPage()).hasValueSatisfying(page -> {
            assertThat(page.getEntries()).hasSize(2);
            assertThat(page.getEntries().get(0).getFile()).isEqualTo(folder.relativize(first));
            assertThat(page.getEntries().get(0).getLocation().getFile()).isEqualTo(folder.relativize(first).toString());
            assertThat(page.getEntries().get(0).getLocation().getDirection()).isEqualTo(FileSystemLocation.Direction.BACKWARD_INCLUSIVE);
            assertThat(page.getEntries().get(1).getFile()).isEqualTo(folder.relativize(second));
            assertThat(page.getEntries().get(1).getLocation().getFile()).isEqualTo(folder.relativize(second).toString());
            assertThat(page.getEntries().get(1).getLocation().getDirection()).isEqualTo(FileSystemLocation.Direction.BACKWARD_INCLUSIVE);
            assertThat(page.getNextLocation()).hasValueSatisfying(location -> {
                assertThat(location.getFile()).isEqualTo(folder.relativize(second).toString());
                assertThat(location.getDirection()).isEqualTo(FileSystemLocation.Direction.FORWARD);
            });
            assertThat(page.getLocation().getFile()).isEqualTo(folder.relativize(second).toString());
            assertThat(page.getLocation().getDirection()).isEqualTo(FileSystemLocation.Direction.BACKWARD_INCLUSIVE);
            assertThat(page.getPreviousLocation()).hasValueSatisfying(location -> {
                assertThat(location.getFile()).isEqualTo(folder.relativize(first).toString());
                assertThat(location.getDirection()).isEqualTo(FileSystemLocation.Direction.BACKWARD_EXCLUSIVE);
            });
        });
    }

    @Test
    public void can_read_page() throws IOException {
        Path first = Files.createFile(folder.resolve("1")), second = Files.createFile(folder.resolve("2"));

        assertThat(endpoint.getPage(new FileSystemLocation(
            folder.relativize(first).toString(),
            FileSystemLocation.Direction.FORWARD
        ))).hasValueSatisfying(page -> {
            assertThat(page.getEntries()).hasSize(1);
            assertThat(page.getEntries().get(0).getFile()).isEqualTo(folder.relativize(second));
            assertThat(page.getEntries().get(0).getLocation().getFile()).isEqualTo(folder.relativize(second).toString());
            assertThat(page.getEntries().get(0).getLocation().getDirection()).isEqualTo(FileSystemLocation.Direction.BACKWARD_INCLUSIVE);
            assertThat(page.getNextLocation()).isEmpty();
            assertThat(page.getLocation().getFile()).isEqualTo(folder.relativize(second).toString());
            assertThat(page.getLocation().getDirection()).isEqualTo(FileSystemLocation.Direction.BACKWARD_INCLUSIVE);
            assertThat(page.getPreviousLocation()).hasValueSatisfying(location -> {
                assertThat(location.getFile()).isEqualTo(folder.relativize(second).toString());
                assertThat(location.getDirection()).isEqualTo(FileSystemLocation.Direction.BACKWARD_EXCLUSIVE);
            });
        });
    }

    @Test
    public void can_read_empty_first_page() {
        assertThat(endpoint.getFirstPage()).isEmpty();
    }

    @Test
    public void can_read_empty_last_page() {
        assertThat(endpoint.getFirstPage()).isEmpty();
    }

    @Test
    public void can_read_empty_page() throws IOException {
        Path first = Files.createFile(folder.resolve("1"));

        assertThat(endpoint.getPage(new FileSystemLocation(folder.relativize(first).toString(), FileSystemLocation.Direction.FORWARD))).isEmpty();
    }
}
