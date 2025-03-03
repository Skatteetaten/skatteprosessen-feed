package no.skatteetaten.fastsetting.formueinntekt.felles.feed.api;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

public interface FeedProcessor<POINTER> {

    boolean start(long timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException;

    boolean stop(long timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException;

    void reset(long timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException;

    void complete(long timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException;

    FeedState<POINTER> getCurrentState();

    default Optional<Activation> findActivation() {
        return Optional.empty();
    }

    default boolean initialize(long timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException {
        start(timeout, timeUnit);
        return true;
    }

    default boolean shutdown(long timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException {
        stop(timeout, timeUnit);
        return true;
    }

    enum Activation {
        ACTIVE, INACTIVE
    }

    abstract class Simple<POINTER> implements FeedProcessor<POINTER> {

        protected final POINTER pointer;

        private final Executor executor;

        private final AtomicBoolean alive = new AtomicBoolean();
        private volatile CountDownLatch completionLatch = new CountDownLatch(0);

        private final AtomicReference<Thread> current = new AtomicReference<>();

        protected volatile Throwable lastError;

        protected Simple(POINTER pointer, Executor executor) {
            this.pointer = pointer;
            this.executor = executor;
        }

        @Override
        public synchronized boolean start(long timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException {
            if (alive.get()) {
                return false;
            }
            lastError = null;
            CountDownLatch startLatch = new CountDownLatch(1);
            completionLatch = new CountDownLatch(1);
            executor.execute(() -> {
                if (alive.compareAndSet(false, true)) {
                    current.set(Thread.currentThread());
                    startLatch.countDown();
                    try {
                        doStart(() -> current.get() != null && !Thread.interrupted());
                    } catch (Throwable t) {
                        lastError = t;
                    } finally {
                        current.set(null);
                        alive.set(false);
                        completionLatch.countDown();
                    }
                } else {
                    throw new IllegalStateException("Feed for " + pointer + " is already running");
                }
            });
            try {
                if (!startLatch.await(timeout, timeUnit)) {
                    throw new TimeoutException();
                }
            } catch (Throwable t) {
                Thread thread = current.getAndSet(null);
                if (thread != null) {
                    thread.interrupt();
                }
                throw t;
            }
            return true;
        }

        protected abstract void doStart(BooleanSupplier isAlive);

        @Override
        public synchronized boolean stop(long timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException {
            if (!alive.get()) {
                return false;
            }
            Thread thread = current.getAndSet(null);
            if (thread != null) {
                thread.interrupt();
            }
            if (!completionLatch.await(timeout, timeUnit)) {
                throw new TimeoutException();
            }
            return true;
        }

        @Override
        public synchronized void reset(long timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException {
            if (alive.get()) {
                Thread thread = current.getAndSet(null);
                if (thread != null) {
                    thread.interrupt();
                }
            }
            if (!completionLatch.await(timeout, timeUnit)) {
                throw new TimeoutException();
            }
            onReset();
            lastError = null;
        }

        protected abstract void onReset();

        @Override
        public synchronized void complete(long timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException {
            if (alive.get()) {
                Thread thread = current.getAndSet(null);
                if (thread != null) {
                    thread.interrupt();
                }
            }
            if (!completionLatch.await(timeout, timeUnit)) {
                throw new TimeoutException();
            }
            onComplete();
            lastError = null;
        }

        protected abstract void onComplete();

        @Override
        public FeedState<POINTER> getCurrentState() {
            return new FeedState<>(pointer, alive.get(), lastError);
        }
    }
}
