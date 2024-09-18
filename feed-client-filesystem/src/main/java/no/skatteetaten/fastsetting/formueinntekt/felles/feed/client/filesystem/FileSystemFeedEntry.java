package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.filesystem;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedEntry;

import java.nio.file.Path;

public class FileSystemFeedEntry implements FeedEntry<FileSystemLocation> {

    private final Path file;

    FileSystemFeedEntry(Path file) {
        this.file = file;
    }

    public Path getFile() {
        return file;
    }

    @Override
    public FileSystemLocation getLocation() {
        return new FileSystemLocation(file.toString(), FileSystemLocation.Direction.BACKWARD_INCLUSIVE);
    }

    @Override
    public String toString() {
        return "entry@" + file;
    }
}
