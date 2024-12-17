package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.filesystem;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedEndpoint;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileSystemFeedEndpoint implements FeedEndpoint<FileSystemLocation,
    FileSystemFeedEntry,
    FileSystemFeedPage> {

    private final FileSystemResolver resolver;
    private final int size;

    public FileSystemFeedEndpoint(FileSystemResolver resolver, int size) {
        this.resolver = resolver;
        this.size = size;
    }

    public static FileSystemFeedEndpoint ofFilesInDefaultOrder(Path folder) {
        return ofFilesInDefaultOrder(folder, 100);
    }

    public static FileSystemFeedEndpoint ofFilesInDefaultOrder(Path folder, int size) {
        return of(folder, Comparator.naturalOrder(), file -> !Files.isDirectory(file), 1, size);
    }

    public static FileSystemFeedEndpoint of(
        Path folder,
        Comparator<Path> comparator,
        Predicate<Path> filter,
        int depth,
        int size
    ) {
        return new FileSystemFeedEndpoint((location, direction, limit) -> {
            try (Stream<Path> stream = Files.walk(folder, depth)) {
                return stream
                    .filter(filter.and(file -> location == null || direction.includes(file.compareTo(folder.resolve(location)))))
                    .map(file -> folder.relativize(file))
                    .sorted(direction.isReversed() ? comparator.reversed() : comparator)
                    .limit(limit)
                    .collect(Collectors.toList());
            }
        }, size);
    }

    @Override
    public Optional<FileSystemFeedPage> getFirstPage() {
        try {
            List<Path> files = resolver.find(null, FileSystemLocation.Direction.FORWARD, size);
            return files.isEmpty() ? Optional.empty() : Optional.of(new FileSystemFeedPage(files, false, files.size() < size));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Optional<FileSystemFeedPage> getLastPage() {
        try {
            List<Path> files = resolver.find(null, FileSystemLocation.Direction.BACKWARD_EXCLUSIVE, size);
            return files.isEmpty() ? Optional.empty() : Optional.of(new FileSystemFeedPage(files, true, files.size() < size));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Optional<FileSystemFeedPage> getPage(FileSystemLocation location) {
        try {
            List<Path> files = resolver.find(location.getFile(), location.getDirection(), size);
            return files.isEmpty() ? Optional.empty() : Optional.of(new FileSystemFeedPage(files, location.getDirection().isReversed(), files.size() < size));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @FunctionalInterface
    public interface FileSystemResolver {

        List<Path> find(String location, FileSystemLocation.Direction direction, int limit) throws IOException;
    }
}
