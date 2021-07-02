package com.codecubic.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Slf4j
public class ConcurrUtil {
    private static BlockingQueue<Runnable> queue = new SynchronousQueue<>();
    private static ExecutorService exeServ = new ThreadPoolExecutor(10, 10,
            60L, TimeUnit.SECONDS,
            queue);
    private static final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(
                    1,
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("failAfter-%d")
                            .build());


    public static <T> CompletableFuture<T> failAfter(Duration duration) {
        final CompletableFuture<T> promise = new CompletableFuture<>();
        scheduler.schedule(promise::isDone, duration.toMillis(), MILLISECONDS);
        return promise;
    }

    public static <T> CompletableFuture<T> within(CompletableFuture<T> future, Duration duration) {
        final CompletableFuture<T> timeout = failAfter(duration);
        return future.applyToEither(timeout, Function.identity());
    }

    public static CompletableFuture submit(Supplier supplier, int sleepMills) {
        final CompletableFuture<String> responseFuture = within(
                CompletableFuture.supplyAsync(supplier, exeServ)
                , Duration.ofMillis(sleepMills));

        responseFuture
//                .thenAccept(this::send)
                .exceptionally(throwable -> {
                    log.error("Unrecoverable error", throwable);
                    return null;
                });
        return responseFuture;
    }

}
