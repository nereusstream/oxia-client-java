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
package io.oxia.client.grpc;

import com.google.common.annotations.VisibleForTesting;
import io.github.merlimat.slog.Logger;
import io.oxia.client.ClientConfig;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;

class ConnectionManager implements AutoCloseable {
    private static final Logger log = Logger.get(ConnectionManager.class);

    @VisibleForTesting final Map<Key, Connection> connections = new ConcurrentHashMap<>();

    private volatile boolean closed;
    private final java.util.Set<Connection> retiring = ConcurrentHashMap.newKeySet();
    private final java.util.Set<io.grpc.ManagedChannel> partial = ConcurrentHashMap.newKeySet();
    private volatile Throwable closeFailure;

    private final int maxConnectionPerNode;
    private final ClientConfig clientConfig;
    private final ScheduledExecutorService executor;

    ConnectionManager(ClientConfig clientConfig, ScheduledExecutorService executor) {
        this.clientConfig = clientConfig;
        this.maxConnectionPerNode = clientConfig.maxConnectionPerNode();
        this.executor = executor;
    }

    synchronized Connection getConnection(String address) {
        if (closed) throw new IllegalStateException("connection manager closed");
        retiring.removeIf(Connection::isTerminated);
        partial.removeIf(io.grpc.ManagedChannel::isTerminated);
        final var random = ThreadLocalRandom.current().nextInt();
        var modKey = random % maxConnectionPerNode;
        if (modKey < 0) {
            modKey += maxConnectionPerNode;
        }
        return connections.computeIfAbsent(
                new Key(address, modKey),
                key -> {
                    final Connection connection;
                    try {
                    connection = new Connection(
                                    key.address,
                                    clientConfig,
                                    executor,
                                    connectionToRemove -> removeConnection(key, connectionToRemove));
                    } catch (Connection.AllocationFailure failure) {
                        partial.add(failure.channel);
                        try { failure.channel.shutdownNow(); } catch (Throwable cleanup) { retainFailure(cleanup); }
                        retainFailure(failure);
                        throw failure;
                    }
                    log.info()
                            .attr("address", key.address)
                            .attr("connectionKey", key.random)
                            .attr("connectionId", connection.getConnectionId())
                            .log("Creating managed GRPC connection");
                    return connection;
                });
    }

    @Override
    public void close() {
        synchronized (this) {
            closed = true;
            retiring.addAll(connections.values());
            connections.clear();
        }
        Throwable failedAttempt = null;
        for (Connection connection : retiring) {
            try { connection.close(); }
            catch (Throwable failure) { retainFailure(failure); if (failedAttempt == null) failedAttempt = failure; }
        }
        for (io.grpc.ManagedChannel channel : partial) {
            try { channel.shutdownNow(); }
            catch (Throwable failure) { retainFailure(failure); if (failedAttempt == null) failedAttempt = failure; }
        }
        if (failedAttempt != null) throw new IllegalStateException("original transport shutdown request failed", failedAttempt);
    }

    boolean isTerminated() {
        return closed && connections.isEmpty() && retiring.stream().allMatch(Connection::isTerminated)
                && partial.stream().allMatch(io.grpc.ManagedChannel::isTerminated);
    }

    void awaitTermination(long nanos) throws InterruptedException {
        long start = System.nanoTime();
        for (Connection connection : retiring) connection.awaitTermination(remaining(start, nanos));
        for (io.grpc.ManagedChannel channel : partial) {
            long remaining = remaining(start, nanos);
            if (remaining > 0) channel.awaitTermination(remaining, java.util.concurrent.TimeUnit.NANOSECONDS);
        }
    }

    private static long remaining(long start, long budget) {
        long elapsed = System.nanoTime() - start;
        return elapsed < 0 || elapsed >= budget ? 0 : budget - elapsed;
    }

    Throwable closeFailure() { return closeFailure; }

    private synchronized void retainFailure(Throwable failure) {
        if (closeFailure == null) closeFailure = failure;
        else if (failure != closeFailure) closeFailure.addSuppressed(failure);
    }

    private void removeConnection(Key key, Connection connection) {
        synchronized (this) {
        if (!connections.remove(key, connection)) {
            return;
        }
        retiring.add(connection);
        }
        log.info()
                .attr("address", key.address)
                .attr("connectionKey", key.random)
                .attr("connectionId", connection.getConnectionId())
                .log("Removing unhealthy GRPC connection");
        try {
            connection.close();
        } catch (Exception e) {
            retainFailure(e);
            log.warn().exception(e).log("Failed to close unhealthy GRPC connection");
        }
    }

    record Key(String address, int random) {}
}
