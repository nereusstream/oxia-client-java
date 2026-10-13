/*
 * Copyright © 2022-2026 The Oxia Authors
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
package io.oxia.client.batch;

import io.oxia.client.ClientConfig;
import io.oxia.client.grpc.RpcProvider;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.session.SessionManager;
import lombok.NonNull;

/**
 * Routes operations to a fixed set of {@link Batcher} threads. A shard is always served by the same
 * batcher, so that the per-shard ordering of operations is preserved.
 */
public class BatchManager implements AutoCloseable {

    private final Batcher[] batchers;
    private volatile boolean closed;

    BatchManager(@NonNull ClientConfig config, String name, @NonNull BatchFactory batchFactory) {
        this(config, name, batchFactory, ignored -> {});
    }

    private BatchManager(ClientConfig config, String name, BatchFactory batchFactory,
            java.util.function.Consumer<BatchManager> allocated) {
        this.batchers = new Batcher[config.batchingThreads()];
        allocated.accept(this);
        for (int i = 0; i < batchers.length; i++) {
            batchers[i] = new Batcher(config, name + "-" + i, batchFactory);
        }
    }

    public void add(@NonNull Operation<?> operation) {
        if (closed) {
            throw new IllegalStateException("Batch manager is closed");
        }
        batchers[(int) Math.floorMod(operation.shardId(), batchers.length)].add(operation);
    }

    @Override
    public void close() throws Exception {
        closed = true;
        Throwable first = null;
        for (Batcher batcher : batchers) {
            if (batcher == null) continue;
            try { batcher.close(); }
            catch (Throwable failure) { if (first == null) first = failure; else first.addSuppressed(failure); }
        }
        if (first != null) throw new IllegalStateException("batch cleanup request failed", first);
    }

    public Throwable closeFailure() {
        Throwable first = null;
        for (Batcher batcher : batchers) {
            if (batcher != null && batcher.closeFailure() != null) {
                if (first == null) first = batcher.closeFailure();
                else if (first != batcher.closeFailure()) {
                    boolean held = false;
                    for (Throwable prior : first.getSuppressed()) if (prior == batcher.closeFailure()) held = true;
                    if (!held) first.addSuppressed(batcher.closeFailure());
                }
            }
        }
        return first;
    }

    public boolean isTerminated() {
        if (!closed) return false;
        for (Batcher batcher : batchers) if (batcher != null && !batcher.isTerminated()) return false;
        return true;
    }

    public void awaitTermination(long nanos) throws InterruptedException {
        long start = System.nanoTime();
        for (Batcher batcher : batchers) {
            long elapsed = System.nanoTime() - start;
            if (batcher != null) batcher.awaitTermination(elapsed < 0 || elapsed >= nanos ? 0 : nanos - elapsed);
        }
    }

    public static BatchManager newReadBatchManager(ClientConfig config, RpcProvider rpc,
            InstrumentProvider instruments, java.util.function.Consumer<BatchManager> allocated) {
        return new BatchManager(config, "oxia-read-batcher", new ReadBatchFactory(rpc, config, instruments), allocated);
    }

    public static BatchManager newWriteBatchManager(ClientConfig config, RpcProvider rpc, SessionManager sessions,
            InstrumentProvider instruments, java.util.function.Consumer<BatchManager> allocated) {
        return new BatchManager(config, "oxia-write-batcher", new WriteBatchFactory(rpc, sessions, config, instruments), allocated);
    }

    public static @NonNull BatchManager newReadBatchManager(
            @NonNull ClientConfig config,
            @NonNull RpcProvider rpcProvider,
            @NonNull InstrumentProvider instrumentProvider) {
        return new BatchManager(
                config, "oxia-read-batcher", new ReadBatchFactory(rpcProvider, config, instrumentProvider));
    }

    public static @NonNull BatchManager newWriteBatchManager(
            @NonNull ClientConfig config,
            @NonNull RpcProvider rpcProvider,
            @NonNull SessionManager sessionManager,
            @NonNull InstrumentProvider instrumentProvider) {
        return new BatchManager(
                config,
                "oxia-write-batcher",
                new WriteBatchFactory(rpcProvider, sessionManager, config, instrumentProvider));
    }
}
