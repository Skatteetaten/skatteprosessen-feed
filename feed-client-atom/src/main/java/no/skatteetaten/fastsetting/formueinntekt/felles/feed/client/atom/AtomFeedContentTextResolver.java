package no.skatteetaten.fastsetting.formueinntekt.felles.feed.client.atom;

import java.util.Optional;
import java.util.function.BiFunction;

import com.rometools.rome.feed.synd.SyndContent;
import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndFeed;

public class AtomFeedContentTextResolver implements BiFunction<SyndFeed, SyndEntry, Optional<String>> {

    @Override
    public Optional<String> apply(SyndFeed feed, SyndEntry entry) {
        return entry.getContents().stream().findFirst().map(SyndContent::getValue);
    }
}
