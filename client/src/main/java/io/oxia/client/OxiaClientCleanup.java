/*
 * Copyright © 2022-2025 The Oxia Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.oxia.client;

import io.oxia.client.batch.BatchManager;
import io.oxia.client.grpc.RpcProvider;
import io.oxia.client.notify.NotificationManager;
import io.oxia.client.session.SessionManager;
import io.oxia.client.shard.ShardManager;
import io.oxia.client.util.NativeCallbackScope;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Cleanup-only capability over the original allocations, including an unfinished opening. */
public final class OxiaClientCleanup {
    volatile ScheduledExecutorService executor;
    volatile RpcProvider rpc;
    volatile ShardManager shards;
    volatile NotificationManager notifications;
    volatile BatchManager reads;
    volatile BatchManager writes;
    volatile SessionManager sessions;
    volatile boolean openingFinished;
    private volatile boolean stopped;
    private final AtomicBoolean cleaning = new AtomicBoolean();
    private final Map<String, Boolean> requested = new LinkedHashMap<>();
    private volatile Throwable firstFailure;

    OxiaClientCleanup() {}

    boolean stopped() { return stopped; }

    synchronized void retainFailure(Throwable failure) {
        if (failure == null || failure == firstFailure) return;
        if (firstFailure == null) firstFailure = failure;
        else {
            for (Throwable prior : firstFailure.getSuppressed()) if (prior == failure) return;
            firstFailure.addSuppressed(failure);
        }
    }

    /** One original cleanup at a time; deadline bounds native observation, never fabricates completion. */
    public Status close(Duration budget) {
        if (budget == null || budget.isNegative() || budget.isZero()
                || budget.compareTo(Duration.ofMinutes(1)) > 0) {
            throw new IllegalArgumentException("cleanup budget must be within (0, 1 minute]");
        }
        stopped = true;
        if (!cleaning.compareAndSet(false, true)) return status();
        long start = System.nanoTime();
        long nanos = budget.toNanos();
        try {
            attempt("read-batches", reads == null ? null : reads::close);
            attempt("write-batches", writes == null ? null : writes::close);
            if (!NativeCallbackScope.active()) {
                observe(() -> { if (reads != null) reads.awaitTermination(remaining(start, nanos)); });
                observe(() -> { if (writes != null) writes.awaitTermination(remaining(start, nanos)); });
            }
            attempt("sessions", sessions == null ? null : sessions::requestClose);
            if (!NativeCallbackScope.active()) {
                observe(() -> { if (sessions != null) sessions.awaitClose(remaining(start, nanos)); });
            }
            attempt("notifications", notifications == null ? null : notifications::close);
            attempt("shards", shards == null ? null : shards::close);
            attempt("rpc", rpc == null ? null : rpc::close);
            attempt("executor", executor == null ? null : () -> executor.shutdownNow());
            // Callbacks and batching threads may themselves be executing close. They never await themselves.
            if (!NativeCallbackScope.active()) {
                if (rpc != null) rpc.awaitTermination(remaining(start, nanos));
                if (executor != null) executor.awaitTermination(remaining(start, nanos), TimeUnit.NANOSECONDS);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            retainFailure(interrupted);
        } catch (Throwable failure) {
            retainFailure(failure);
        } finally {
            cleaning.set(false);
        }
        return status();
    }

    private static long remaining(long start, long budget) {
        long elapsed = System.nanoTime() - start;
        return elapsed < 0 || elapsed >= budget ? 0 : budget - elapsed;
    }

    private void observe(AutoCloseable observation) {
        try { observation.close(); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); retainFailure(interrupted); }
        catch (Throwable failure) { retainFailure(failure); }
    }

    private void attempt(String name, AutoCloseable close) {
        synchronized (this) {
            if (Boolean.TRUE.equals(requested.get(name))) return;
        }
        try {
            if (close != null) close.close();
            synchronized (this) { requested.put(name, true); }
        } catch (Throwable failure) {
            synchronized (this) { requested.put(name, false); }
            retainFailure(failure);
        }
    }

    public Status status() {
        if (reads != null) retainFailure(reads.closeFailure());
        if (writes != null) retainFailure(writes.closeFailure());
        if (sessions != null) retainFailure(sessions.closeFailure());
        if (rpc != null) retainFailure(rpc.closeFailure());
        final Map<String, Boolean> requests;
        synchronized (this) { requests = Map.copyOf(requested); }
        boolean allRequested = requests.size() == 7 && requests.values().stream().allMatch(Boolean::booleanValue);
        boolean batchExit = (reads == null || reads.isTerminated()) && (writes == null || writes.isTerminated());
        boolean transportExit = rpc == null || rpc.isTerminated();
        boolean executorExit = executor == null || executor.isTerminated();
        return new Status(stopped, openingFinished, requests, batchExit, transportExit,
                executorExit, stopped && openingFinished && !cleaning.get() && allRequested && batchExit && transportExit && executorExit,
                firstFailure);
    }

    /** SDK termination does not prove any remote write, session deletion, Owner release or checkpoint outcome. */
    public record Status(boolean closeRequested, boolean openingFinished, Map<String, Boolean> closeRequests,
            boolean batchThreadsTerminated, boolean transportTerminated, boolean executorTerminated,
            boolean terminated, Throwable firstFailure) {}
}
