package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.filesystem;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedPage;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FileSystemFeedPage implements FeedPage<FileSystemLocation, FileSystemFeedEntry> {

    private final List<Path> files;
    private final boolean reversed, limited;

    FileSystemFeedPage(List<Path> files, boolean reversed, boolean limited) {
        this.files = files;
        this.reversed = reversed;
        this.limited = limited;
    }

    @Override
    public FileSystemLocation getLocation() {
        return new FileSystemLocation(
            files.get(reversed ? 0 : (files.size() - 1)).toString(),
            FileSystemLocation.Direction.BACKWARD_INCLUSIVE
        );
    }

    @Override
    public Optional<FileSystemLocation> getPreviousLocation() {
        if (reversed && limited) {
            return Optional.empty();
        }
        return Optional.of(new FileSystemLocation(
            files.get(reversed ? (files.size() - 1) : 0).toString(),
            FileSystemLocation.Direction.BACKWARD_EXCLUSIVE
        ));
    }

    @Override
    public Optional<FileSystemLocation> getNextLocation() {
        if (!reversed && limited) {
            return Optional.empty();
        }
        return Optional.of(new FileSystemLocation(
            files.get(reversed ? 0 : (files.size() - 1)).toString(),
            FileSystemLocation.Direction.FORWARD
        ));
    }

    @Override
    public List<FileSystemFeedEntry> getEntries() {
        return (reversed
            ? IntStream.range(0, files.size()).mapToObj(index -> files.get(files.size() - 1 - index))
            : files.stream()).map(FileSystemFeedEntry::new).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return "page@" + getLocation();
    }
}
