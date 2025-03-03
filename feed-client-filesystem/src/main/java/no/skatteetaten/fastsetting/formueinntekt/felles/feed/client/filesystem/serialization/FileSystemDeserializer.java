package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.filesystem.serialization;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.filesystem.FileSystemLocation;

import java.util.Map;
import java.util.function.Function;

public class FileSystemDeserializer implements Function<Map<String, String>, FileSystemLocation> {

    @Override
    public FileSystemLocation apply(Map<String, String> serialization) {
        return new FileSystemLocation(
            serialization.get("FILE"),
            FileSystemLocation.Direction.valueOf(serialization.get("DIRECTION"))
        );
    }
}
