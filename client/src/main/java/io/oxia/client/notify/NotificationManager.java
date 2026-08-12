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
package io.oxia.client.notify;

import static io.oxia.client.api.NotificationContinuityState.ARMING;
import static io.oxia.client.api.NotificationContinuityState.CLOSED;
import static io.oxia.client.api.NotificationContinuityState.READY;

import io.github.merlimat.slog.Logger;
import io.opentelemetry.api.common.Attributes;
import io.oxia.client.CompositeConsumer;
import io.oxia.client.api.Notification;
import io.oxia.client.api.NotificationContinuityRegistration;
import io.oxia.client.api.NotificationContinuitySnapshot;
import io.oxia.client.grpc.RpcProvider;
import io.oxia.client.metrics.Counter;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.metrics.Unit;
import io.oxia.client.shard.ShardManager;
import io.oxia.client.shard.ShardManager.ShardAssignmentChanges;
import io.oxia.proto.NotificationBatch;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.NonNull;

public class NotificationManager implements AutoCloseable, Consumer<ShardAssignmentChanges> {
    private final Logger log = Logger.get(NotificationManager.class);
    private final ConcurrentMap<Long, ShardNotificationReceiver> shardReceivers =
            new ConcurrentHashMap<>();
    private final @NonNull ShardNotificationReceiver.Factory receiverFactory;
    private final @NonNull ShardManager shardManager;
    private final CompositeConsumer<Notification> compositeCallback;
    private final SerialContinuityDispatcher continuityDispatcher;
    private final Set<ContinuityRegistration> continuityRegistrations = new HashSet<>();

    @Getter private final ScheduledExecutorService executor;

    private volatile boolean started;
    private volatile boolean closed;
    private volatile NotificationContinuitySnapshot currentSnapshot;

    private long generation;
    private Set<Long> currentAssignment = Set.of();
    private Set<Long> readyShards = Set.of();
    private CompletableFuture<Void> readyFuture;

    @Getter private final Counter counterNotificationsReceived;
    @Getter private final Counter counterNotificationsBatchesReceived;

    public NotificationManager(
            @NonNull ScheduledExecutorService executor,
            @NonNull RpcProvider rpcProvider,
            @NonNull ShardManager shardManager,
            @NonNull InstrumentProvider instrumentProvider) {
        this(
                executor,
                new ShardNotificationReceiver.Factory(rpcProvider),
                shardManager,
                instrumentProvider);
    }

    public NotificationManager(
            @NonNull ScheduledExecutorService executor,
            @NonNull ShardNotificationReceiver.Factory receiverFactory,
            @NonNull ShardManager shardManager,
            @NonNull InstrumentProvider instrumentProvider) {
        this.receiverFactory = receiverFactory;
        this.compositeCallback = receiverFactory.getCallback();
        this.shardManager = shardManager;
        this.executor = executor;
        this.continuityDispatcher = new SerialContinuityDispatcher(executor);

        this.counterNotificationsReceived =
                instrumentProvider.newCounter(
                        "oxia.client.notifications.received",
                        Unit.Events,
                        "The total number of notification events",
                        Attributes.empty());
        this.counterNotificationsBatchesReceived =
                instrumentProvider.newCounter(
                        "oxia.client.notifications.batches.received",
                        Unit.Events,
                        "The total number of notification batches received",
                        Attributes.empty());
    }

    @Override
    public void accept(@NonNull ShardAssignmentChanges changes) {
        final GenerationTransition transition;
        synchronized (this) {
            if (!started || closed || changesAreEmpty(changes)) {
                return;
            }

            var nextAssignment = new HashSet<>(currentAssignment);
            changes.removed().forEach(shard -> nextAssignment.remove(shard.id()));
            changes.added().forEach(shard -> nextAssignment.add(shard.id()));
            transition = beginGenerationLocked(Set.copyOf(nextAssignment));
        }
        applyTransition(transition);
    }

    public void registerCallback(@NonNull Consumer<Notification> callback) {
        final GenerationTransition transition;
        synchronized (this) {
            ensureOpen();
            compositeCallback.add(callback);
            transition = startIfNeededLocked();
        }
        applyTransition(transition);
    }

    public NotificationContinuityRegistration registerContinuityListener(
            @NonNull Consumer<NotificationContinuitySnapshot> listener) {
        final ContinuityRegistration registration;
        final GenerationTransition transition;
        final NotificationContinuitySnapshot initialSnapshot;
        synchronized (this) {
            ensureOpen();
            registration = new ContinuityRegistration(listener);
            continuityRegistrations.add(registration);
            transition = startIfNeededLocked();
            initialSnapshot = currentSnapshot;
        }

        if (transition != null) {
            applyTransition(transition);
        } else {
            dispatchSnapshot(initialSnapshot, List.of(registration), null);
        }
        return registration;
    }

    boolean onBatch(@NonNull ShardNotificationReceiver receiver, @NonNull NotificationBatch batch) {
        final NotificationContinuitySnapshot readySnapshot;
        final List<ContinuityRegistration> listeners;
        final CompletableFuture<Void> stageToComplete;
        synchronized (this) {
            if (closed
                    || shardReceivers.get(receiver.getShardId()) != receiver
                    || !currentAssignment.contains(receiver.getShardId())) {
                return false;
            }

            if (!readyShards.add(receiver.getShardId())
                    || readyShards.size() != currentAssignment.size()
                    || currentAssignment.isEmpty()) {
                return true;
            }

            readySnapshot = newSnapshotLocked(READY);
            currentSnapshot = readySnapshot;
            listeners = List.copyOf(continuityRegistrations);
            stageToComplete = readyFuture;
        }
        dispatchSnapshot(readySnapshot, listeners, () -> stageToComplete.complete(null));
        return true;
    }

    private synchronized GenerationTransition startIfNeededLocked() {
        if (started) {
            return null;
        }
        started = true;
        var assignment =
                shardManager.allShards().stream()
                        .map(shard -> shard.id())
                        .collect(java.util.stream.Collectors.toUnmodifiableSet());
        return beginGenerationLocked(assignment);
    }

    private GenerationTransition beginGenerationLocked(@NonNull Set<Long> assignment) {
        if (generation == Long.MAX_VALUE) {
            throw new IllegalStateException("notification continuity generation overflow");
        }

        var oldReadyFuture = readyFuture;
        var oldReceivers = List.copyOf(shardReceivers.values());
        shardReceivers.clear();
        generation += 1;
        currentAssignment = assignment;
        readyShards = new HashSet<>();
        readyFuture = new CompletableFuture<>();
        currentSnapshot = newSnapshotLocked(ARMING);
        return new GenerationTransition(
                generation,
                assignment,
                currentSnapshot,
                List.copyOf(continuityRegistrations),
                oldReadyFuture,
                oldReceivers);
    }

    private NotificationContinuitySnapshot newSnapshotLocked(
            io.oxia.client.api.NotificationContinuityState state) {
        return new NotificationContinuitySnapshot(
                generation, state, readyFuture.minimalCompletionStage());
    }

    private void applyTransition(GenerationTransition transition) {
        if (transition == null) {
            return;
        }

        dispatchSnapshot(
                transition.snapshot(),
                transition.listeners(),
                () -> {
                    if (transition.oldReadyFuture() != null) {
                        transition
                                .oldReadyFuture()
                                .completeExceptionally(
                                        new IllegalStateException("notification continuity generation was superseded"));
                    }
                });
        transition.oldReceivers().forEach(ShardNotificationReceiver::close);
        openReceivers(transition.generation(), transition.assignment());
    }

    private void openReceivers(long expectedGeneration, Set<Long> assignment) {
        for (long shardId : assignment) {
            final ShardNotificationReceiver receiver;
            synchronized (this) {
                if (closed
                        || generation != expectedGeneration
                        || !currentAssignment.contains(shardId)
                        || shardReceivers.containsKey(shardId)) {
                    continue;
                }
                receiver = receiverFactory.newReceiver(shardId, this, OptionalLong.empty());
                shardReceivers.put(shardId, receiver);
            }
            receiver.start();
        }
    }

    private void dispatchSnapshot(
            NotificationContinuitySnapshot snapshot,
            List<ContinuityRegistration> listeners,
            Runnable beforeListeners) {
        continuityDispatcher.execute(
                () -> {
                    if (beforeListeners != null) {
                        beforeListeners.run();
                    }
                    listeners.forEach(registration -> registration.publish(snapshot));
                });
    }

    private static boolean changesAreEmpty(ShardAssignmentChanges changes) {
        return changes.added().isEmpty()
                && changes.removed().isEmpty()
                && changes.reassigned().isEmpty();
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Notification manager has been closed");
        }
    }

    @Override
    public void close() {
        final NotificationContinuitySnapshot closedSnapshot;
        final List<ContinuityRegistration> listeners;
        final List<ShardNotificationReceiver> receivers;
        final CompletableFuture<Void> oldReadyFuture;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            if (generation == Long.MAX_VALUE) {
                throw new IllegalStateException("notification continuity generation overflow");
            }
            oldReadyFuture = readyFuture;
            generation += 1;
            readyFuture = new CompletableFuture<>();
            currentSnapshot = newSnapshotLocked(CLOSED);
            closedSnapshot = currentSnapshot;
            listeners = List.copyOf(continuityRegistrations);
            receivers = new ArrayList<>(shardReceivers.values());
            shardReceivers.clear();
        }

        dispatchSnapshot(
                closedSnapshot,
                listeners,
                () -> {
                    if (oldReadyFuture != null) {
                        oldReadyFuture.completeExceptionally(
                                new CancellationException("notification manager was closed"));
                    }
                    readyFuture.completeExceptionally(
                            new CancellationException("notification manager was closed"));
                });
        receivers.forEach(ShardNotificationReceiver::close);
    }

    private record GenerationTransition(
            long generation,
            Set<Long> assignment,
            NotificationContinuitySnapshot snapshot,
            List<ContinuityRegistration> listeners,
            CompletableFuture<Void> oldReadyFuture,
            List<ShardNotificationReceiver> oldReceivers) {}

    private final class ContinuityRegistration implements NotificationContinuityRegistration {
        private final Consumer<NotificationContinuitySnapshot> listener;
        private volatile boolean registrationClosed;

        private ContinuityRegistration(Consumer<NotificationContinuitySnapshot> listener) {
            this.listener = listener;
        }

        @Override
        public NotificationContinuitySnapshot current() {
            return currentSnapshot;
        }

        @Override
        public void close() {
            synchronized (NotificationManager.this) {
                if (registrationClosed) {
                    return;
                }
                registrationClosed = true;
                continuityRegistrations.remove(this);
            }
        }

        private void publish(NotificationContinuitySnapshot snapshot) {
            if (registrationClosed) {
                return;
            }
            try {
                listener.accept(snapshot);
            } catch (Throwable error) {
                log.warn().exceptionMessage(error).log("Notification continuity listener failed");
            }
        }
    }
}
