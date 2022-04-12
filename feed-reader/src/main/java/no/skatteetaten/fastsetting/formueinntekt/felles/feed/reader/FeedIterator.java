package no.skatteetaten.fastsetting.formueinntekt.felles.feed.reader;

import java.util.Optional;
import java.util.function.BooleanSupplier;

import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedEndpoint;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedEntry;
import no.skatteetaten.fastsetting.formueinntekt.felles.feed.api.FeedPage;

enum FeedIterator {

    FORWARD(
        FeedEndpoint::getFirstPage, FeedEndpoint::getFirstPage,
        FeedPage::getNextLocation, PageFinalizer::forward
    ),
    BACKWARD(
        FeedEndpoint::getLastPage, FeedEndpoint::getLastPage,
        FeedPage::getPreviousLocation, PageFinalizer::backward
    );

    private final PageBoundInitializer boundInitializer;
    private final PageOpenInitializer openInitializer;

    private final PageForwarder forwarder;
    private final PageFinalizer finalizer;

    FeedIterator(
        PageBoundInitializer boundInitializer, PageOpenInitializer openInitializer,
        PageForwarder forwarder, PageFinalizer finalizer
    ) {
        this.boundInitializer = boundInitializer;
        this.openInitializer = openInitializer;
        this.forwarder = forwarder;
        this.finalizer = finalizer;
    }

    <LOCATION,
        ENTRY extends FeedEntry<LOCATION>,
        PAGE extends FeedPage<LOCATION, ENTRY>>
    Optional<LOCATION> read(
        FeedEndpoint<LOCATION, ENTRY, PAGE> endpoint,
        LOCATION from, LOCATION to,
        PageCallback<? super LOCATION, ? super ENTRY, ? super PAGE> callback,
        BooleanSupplier isAlive
    ) throws InterruptedException {
        Optional<PAGE> current;
        if (from != null) {
            current = to == null ? endpoint.getPage(from) : endpoint.getPage(from, to);
            if (current.isEmpty()) {
                return Optional.of(from);
            }
        } else if (to != null) {
            current = boundInitializer.apply(endpoint, to);
            if (current.isEmpty()) {
                return Optional.of(to);
            }
        } else {
            current = openInitializer.apply(endpoint);
        }
        Optional<PAGE> next = current;
        Optional<LOCATION> origin = current.map(FeedPage::getLocation);
        while (next.isPresent()) {
            if (!isAlive.getAsBoolean()) {
                throw new InterruptedException();
            }
            current = next;
            next = current
                .filter(page -> callback.accept(page, origin.orElse(null)))
                .flatMap(forwarder::apply)
                .filter(location -> !location.equals(to))
                .flatMap(endpoint::getPage);
        }
        return finalizer.apply(
            origin.orElse(null),
            current.map(page -> forwarder.apply(page).orElse(page.getLocation())).orElse(null)
        );
    }

    @FunctionalInterface
    interface PageCallback<LOCATION,
        ENTRY extends FeedEntry<LOCATION>,
        PAGE extends FeedPage<LOCATION, ENTRY>> {

        boolean accept(PAGE page, LOCATION origin);
    }

    @FunctionalInterface
    private interface PageOpenInitializer {

        <LOCATION,
            ENTRY extends FeedEntry<LOCATION>,
            PAGE extends FeedPage<LOCATION, ENTRY>>
        Optional<PAGE> apply(FeedEndpoint<LOCATION, ENTRY, PAGE> endpoint);
    }

    @FunctionalInterface
    private interface PageBoundInitializer {

        <LOCATION,
            ENTRY extends FeedEntry<LOCATION>,
            PAGE extends FeedPage<LOCATION, ENTRY>>
        Optional<PAGE> apply(FeedEndpoint<LOCATION, ENTRY, PAGE> endpoint, LOCATION location);
    }

    @FunctionalInterface
    private interface PageForwarder {

        <LOCATION, PAGE extends FeedPage<LOCATION, ?>> Optional<LOCATION> apply(PAGE page);
    }

    @FunctionalInterface
    private interface PageFinalizer {

        <LOCATION> Optional<LOCATION> apply(LOCATION initial, LOCATION page);

        @SuppressWarnings("unused")
        static <LOCATION> Optional<LOCATION> forward(LOCATION origin, LOCATION last) {
            return Optional.ofNullable(last);
        }

        @SuppressWarnings("unused")
        static <LOCATION> Optional<LOCATION> backward(LOCATION origin, LOCATION last) {
            return Optional.ofNullable(origin);
        }
    }
}
