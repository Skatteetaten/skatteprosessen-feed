package no.skatteetaten.fastsetting.formueinntekt.felles.feed.api;

import java.util.function.Supplier;

public interface FeedTransactor {

    void start(Type type);

    void end();

    default void end(Throwable t) {
        end();
    }

    default <VALUE> VALUE apply(Type type, Supplier<VALUE> supplier) {
        start(type);
        try {
            VALUE value = supplier.get();
            end();
            return value;
        } catch (Throwable t) {
            end(t);
            throw t;
        }
    }

    enum Type {
        INITIALIZE,
        FIRST_PAGE,
        NEXT_PAGE
    }

    class NoOp implements FeedTransactor {

        @Override
        public void start(Type type) { }

        @Override
        public void end() { }

        @Override
        public void end(Throwable t) { }

        @Override
        public <VALUE> VALUE apply(Type type, Supplier<VALUE> supplier) {
            return supplier.get();
        }
    }
}
