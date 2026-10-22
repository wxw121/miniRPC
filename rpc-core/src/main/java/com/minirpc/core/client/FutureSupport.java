package com.minirpc.core.client;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/** Future transformations that retain cancellation of the underlying RPC attempt. */
public final class FutureSupport {
    private FutureSupport() { }

    public static Throwable unwrap(Throwable error) {
        while ((error instanceof CompletionException || error instanceof ExecutionException)
                && error.getCause() != null) {
            error = error.getCause();
        }
        return error;
    }

    public static RuntimeException propagate(Throwable error) {
        Throwable cause = unwrap(error);
        return cause instanceof RuntimeException runtime ? runtime : new CompletionException(cause);
    }

    public static <T, R> CompletableFuture<R> handle(CompletableFuture<T> source,
                                                   BiFunction<T, Throwable, R> handler) {
        CompletableFuture<R> result = new CompletableFuture<>();
        result.whenComplete((value, error) -> {
            if (result.isCancelled()) {
                source.cancel(false);
            }
        });
        source.whenComplete((value, error) -> {
            try {
                result.complete(handler.apply(value, error == null ? null : unwrap(error)));
            } catch (Throwable failure) {
                result.completeExceptionally(unwrap(failure));
            }
        });
        return result;
    }

    /** Legacy synchronous extensions run on a worker; native async extensions avoid this bridge. */
    public static <T> CompletableFuture<T> adapt(
            Function<Supplier<T>, T> synchronous, Supplier<CompletableFuture<T>> invoker) {
        return adaptFunction(sync -> synchronous.apply(() -> sync.apply(null)), ignored -> invoker.get());
    }

    public static <A, T> CompletableFuture<T> adaptFunction(
            Function<Function<A, T>, T> synchronous, Function<A, CompletableFuture<T>> invoker) {
        CompletableFuture<T> result = new CompletableFuture<>();
        AtomicReference<CompletableFuture<T>> active = new AtomicReference<>();
        result.whenComplete((value, error) -> {
            if (result.isCancelled()) {
                CompletableFuture<T> attempt = active.get();
                if (attempt != null) attempt.cancel(false);
            }
        });
        CompletableFuture.runAsync(() -> {
            try {
                if (result.isDone()) return;
                result.complete(synchronous.apply(argument -> {
                    if (result.isDone()) throw new CancellationException();
                    CompletableFuture<T> attempt = invoker.apply(argument);
                    active.set(attempt);
                    if (result.isCancelled()) attempt.cancel(false);
                    try {
                        return attempt.join();
                    } catch (CompletionException failure) {
                        throw propagate(failure);
                    }
                }));
            } catch (Throwable failure) {
                result.completeExceptionally(unwrap(failure));
            }
        });
        return result;
    }
}
