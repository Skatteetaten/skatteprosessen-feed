package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.filesystem.serialization;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.filesystem.FileSystemLocation;

import java.util.Map;
import java.util.function.Function;

public class FileSystemSerializer implements Function<FileSystemLocation, Map<String, String>> {

    @Override
    public Map<String, String> apply(FileSystemLocation location) {
        return Map.of(
            "FILE", location.getFile(),
            "DIRECTION", location.getDirection().name()
        );
    }
}
