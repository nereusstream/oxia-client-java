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

import io.github.merlimat.slog.Logger;
import io.opentelemetry.api.common.Attributes;
import io.oxia.client.CompositeConsumer;
import io.oxia.client.api.Notification;
import io.oxia.client.api.NotificationContinuityRegistration;
import io.oxia.client.api.NotificationContinuitySnapshot;
import io.oxia.client.api.NotificationContinuityState;
import io.oxia.client.grpc.RpcProvider;
import io.oxia.client.metrics.Counter;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.metrics.Unit;
import io.oxia.client.shard.Shard;
import io.oxia.client.shard.ShardManager;
import io.oxia.client.shard.ShardManager.ShardAssignmentContinuityListener;
import io.oxia.client.shard.ShardManager.ShardAssignmentContinuitySnapshot;
import io.oxia.client.util.Backoff;
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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.NonNull;

public class NotificationManager implements AutoCloseable, ShardAssignmentContinuityListener {
    private final Logger log = Logger.get(NotificationManager.class);
    private final ConcurrentMap<Long, ShardNotificationReceiver> shardReceivers =
            new ConcurrentHashMap<>();
    private final @NonNull ShardNotificationReceiver.Factory receiverFactory;
    private final @NonNull ShardManager shardManager;
    private final @NonNull Backoff backoff;
    private final CompositeConsumer<Notification> compositeCallback;
    private final SerialContinuityDispatcher continuityDispatcher;
    private final Set<ContinuityRegistration> continuityRegistrations = new HashSet<>();

    @Getter private final ScheduledExecutorService executor;

    private volatile boolean started;
    private volatile boolean closed;
    private volatile NotificationContinuitySnapshot currentSnapshot;

    private long generation;
    private long assignmentGeneration;
    private ShardManager.ShardAssignmentContinuityState assignmentContinuityState;
    private boolean assignmentContinuityReady;
    private Set<Shard> currentAssignment = Set.of();
    private Set<Long> readyShards = Set.of();
    private CompletableFuture<Void> readyFuture;
    private ScheduledFuture<?> retryTask;

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
                instrumentProvider,
                new Backoff());
    }

    NotificationManager(
            @NonNull ScheduledExecutorService executor,
            @NonNull ShardNotificationReceiver.Factory receiverFactory,
            @NonNull ShardManager shardManager,
            @NonNull InstrumentProvider instrumentProvider) {
        this(executor, receiverFactory, shardManager, instrumentProvider, new Backoff());
    }

    NotificationManager(
            @NonNull ScheduledExecutorService executor,
            @NonNull ShardNotificationReceiver.Factory receiverFactory,
            @NonNull ShardManager shardManager,
            @NonNull InstrumentProvider instrumentProvider,
            @NonNull Backoff backoff) {
        this.receiverFactory = receiverFactory;
        this.compositeCallback = receiverFactory.getCallback();
        this.shardManager = shardManager;
        this.executor = executor;
        this.backoff = backoff;
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
        synchronized (this) {
            ensureOpen();
            registration = new ContinuityRegistration(listener);
            continuityRegistrations.add(registration);
            transition = startIfNeededLocked();
            if (transition == null) {
                // Enqueue while holding the state lock so a concurrent loss cannot overtake this
                // registration's initial snapshot.
                dispatchSnapshot(currentSnapshot, List.of(registration), null);
            }
        }
        applyTransition(transition);
        return registration;
    }

    @Override
    public void onContinuityChange(@NonNull ShardAssignmentContinuitySnapshot snapshot) {
        if (snapshot.state() == ShardManager.ShardAssignmentContinuityState.CLOSED) {
            close();
            return;
        }

        GenerationTransition transition = null;
        ReceiverPlan receiverPlan = null;
        synchronized (this) {
            if (!started || closed) {
                return;
            }

            if (snapshot.generation() < assignmentGeneration) {
                return;
            }

            var nextAssignment = Set.copyOf(snapshot.assignment());
            if (snapshot.generation() > assignmentGeneration) {
                // A later READY can arrive after its ARMING callback was delayed. Treat the
                // generation change itself as the continuity cut so no stale receiver survives.
                assignmentGeneration = snapshot.generation();
                assignmentContinuityState = snapshot.state();
                assignmentContinuityReady =
                        snapshot.state() == ShardManager.ShardAssignmentContinuityState.READY;
                transition =
                        beginGenerationLocked(
                                nextAssignment, assignmentContinuityReady && !nextAssignment.isEmpty());
            } else if (snapshot.state() == ShardManager.ShardAssignmentContinuityState.ARMING) {
                if (assignmentContinuityState == ShardManager.ShardAssignmentContinuityState.READY) {
                    // READY is terminal within one assignment generation. A same-generation
                    // ARMING callback can only be a delayed publication and must not regress it.
                    return;
                }
                assignmentContinuityState = snapshot.state();
                if (assignmentContinuityReady) {
                    assignmentContinuityReady = false;
                    transition = beginGenerationLocked(nextAssignment, false);
                }
            } else {
                assignmentContinuityState = snapshot.state();
                if (!assignmentContinuityReady) {
                    assignmentContinuityReady = true;
                    receiverPlan = resumeGenerationLocked(nextAssignment);
                } else if (!currentAssignment.equals(nextAssignment)) {
                    transition = beginGenerationLocked(nextAssignment, !nextAssignment.isEmpty());
                }
            }
        }

        applyTransition(transition);
        applyReceiverPlan(receiverPlan);
    }

    boolean onBatch(@NonNull ShardNotificationReceiver receiver, @NonNull NotificationBatch batch) {
        final NotificationContinuitySnapshot readySnapshot;
        final List<ContinuityRegistration> listeners;
        final CompletableFuture<Void> stageToComplete;
        synchronized (this) {
            if (closed
                    || shardReceivers.get(receiver.getShardId()) != receiver
                    || currentAssignment.stream().noneMatch(shard -> shard.id() == receiver.getShardId())) {
                return false;
            }

            if (!readyShards.add(receiver.getShardId())
                    || readyShards.size() != currentAssignment.size()
                    || currentAssignment.isEmpty()) {
                return true;
            }

            backoff.reset();
            readySnapshot = newSnapshotLocked(NotificationContinuityState.READY);
            currentSnapshot = readySnapshot;
            listeners = List.copyOf(continuityRegistrations);
            stageToComplete = readyFuture;
            // Enqueue under the state lock so a concurrent loss cannot publish ARMING(g+1)
            // before READY(g). The stage completes before listeners observe READY.
            dispatchSnapshot(readySnapshot, listeners, () -> stageToComplete.complete(null));
        }
        return true;
    }

    void onReceiverError(@NonNull ShardNotificationReceiver receiver, @NonNull Throwable error) {
        handleReceiverLoss(receiver, error);
    }

    void onReceiverCompleted(@NonNull ShardNotificationReceiver receiver) {
        handleReceiverLoss(
                receiver, new IllegalStateException("notification stream completed unexpectedly"));
    }

    private void handleReceiverLoss(ShardNotificationReceiver receiver, Throwable error) {
        final GenerationTransition transition;
        final long retryDelayMillis;
        synchronized (this) {
            if (closed || shardReceivers.get(receiver.getShardId()) != receiver) {
                return;
            }
            log.warn()
                    .attr("shard", receiver.getShardId())
                    .exceptionMessage(error)
                    .log("Notification continuity was lost");
            transition = beginGenerationLocked(currentAssignment, false);
            retryDelayMillis = closed ? -1 : backoff.nextDelayMillis();
        }

        applyTransition(transition);
        if (retryDelayMillis >= 0) {
            scheduleRetry(transition.generation(), retryDelayMillis);
        }
    }

    private GenerationTransition startIfNeededLocked() {
        if (started) {
            return null;
        }

        var assignmentSnapshot = shardManager.assignmentContinuity();
        if (assignmentSnapshot.state() == ShardManager.ShardAssignmentContinuityState.CLOSED) {
            throw new IllegalStateException("Shard assignment manager has been closed");
        }

        started = true;
        assignmentGeneration = assignmentSnapshot.generation();
        assignmentContinuityState = assignmentSnapshot.state();
        assignmentContinuityReady =
                assignmentSnapshot.state() == ShardManager.ShardAssignmentContinuityState.READY;
        currentAssignment = Set.copyOf(assignmentSnapshot.assignment());
        return beginGenerationLocked(
                currentAssignment, assignmentContinuityReady && !currentAssignment.isEmpty());
    }

    private GenerationTransition beginGenerationLocked(
            @NonNull Set<Shard> assignment, boolean openReceivers) {
        cancelRetryLocked();
        if (generation == Long.MAX_VALUE) {
            return terminalTransitionLocked();
        }

        var oldReadyFuture = readyFuture;
        var oldReceivers = List.copyOf(shardReceivers.values());
        shardReceivers.clear();
        generation += 1;
        currentAssignment = Set.copyOf(assignment);
        readyShards = new HashSet<>();
        readyFuture = new CompletableFuture<>();
        currentSnapshot = newSnapshotLocked(NotificationContinuityState.ARMING);
        var transition =
                new GenerationTransition(
                        generation,
                        currentAssignment,
                        currentSnapshot,
                        List.copyOf(continuityRegistrations),
                        oldReadyFuture,
                        oldReceivers,
                        openReceivers);
        enqueueTransitionLocked(transition);
        return transition;
    }

    private GenerationTransition terminalTransitionLocked() {
        closed = true;
        assignmentContinuityReady = false;
        var oldReadyFuture = readyFuture;
        var oldReceivers = List.copyOf(shardReceivers.values());
        shardReceivers.clear();
        readyFuture = new CompletableFuture<>();
        currentSnapshot = newSnapshotLocked(NotificationContinuityState.CLOSED);
        var transition =
                new GenerationTransition(
                        generation,
                        Set.of(),
                        currentSnapshot,
                        List.copyOf(continuityRegistrations),
                        oldReadyFuture,
                        oldReceivers,
                        false);
        enqueueTransitionLocked(transition);
        return transition;
    }

    private ReceiverPlan resumeGenerationLocked(Set<Shard> assignment) {
        cancelRetryLocked();
        var oldReceivers = List.copyOf(shardReceivers.values());
        shardReceivers.clear();
        currentAssignment = Set.copyOf(assignment);
        readyShards = new HashSet<>();
        return new ReceiverPlan(generation, currentAssignment, oldReceivers);
    }

    private NotificationContinuitySnapshot newSnapshotLocked(NotificationContinuityState state) {
        return new NotificationContinuitySnapshot(
                generation, state, readyFuture.minimalCompletionStage());
    }

    private void applyTransition(GenerationTransition transition) {
        if (transition == null) {
            return;
        }

        transition.oldReceivers().forEach(ShardNotificationReceiver::close);
        if (transition.openReceivers() && !transition.assignment().isEmpty()) {
            openReceivers(transition.generation(), transition.assignment());
        }
    }

    private void enqueueTransitionLocked(GenerationTransition transition) {
        dispatchSnapshot(
                transition.snapshot(),
                transition.listeners(),
                () -> failPendingStage(transition.oldReadyFuture()));
    }

    private void applyReceiverPlan(ReceiverPlan plan) {
        if (plan == null) {
            return;
        }
        plan.oldReceivers().forEach(ShardNotificationReceiver::close);
        if (!plan.assignment().isEmpty()) {
            openReceivers(plan.generation(), plan.assignment());
        }
    }

    private void scheduleRetry(long expectedGeneration, long retryDelayMillis) {
        synchronized (this) {
            if (closed || generation != expectedGeneration || !assignmentContinuityReady) {
                return;
            }
            retryTask =
                    executor.schedule(
                            () -> retry(expectedGeneration), retryDelayMillis, TimeUnit.MILLISECONDS);
        }
    }

    private void retry(long expectedGeneration) {
        final Set<Shard> assignment;
        synchronized (this) {
            if (closed || generation != expectedGeneration || !assignmentContinuityReady) {
                return;
            }
            retryTask = null;
            assignment = currentAssignment;
        }
        openReceivers(expectedGeneration, assignment);
    }

    private void openReceivers(long expectedGeneration, Set<Shard> assignment) {
        assignment.stream()
                .sorted(java.util.Comparator.comparingLong(Shard::id))
                .forEach(shard -> openReceiver(expectedGeneration, shard.id()));
    }

    private void openReceiver(long expectedGeneration, long shardId) {
        final ShardNotificationReceiver receiver;
        synchronized (this) {
            if (closed
                    || generation != expectedGeneration
                    || !assignmentContinuityReady
                    || currentAssignment.stream().noneMatch(shard -> shard.id() == shardId)
                    || shardReceivers.containsKey(shardId)) {
                return;
            }
            receiver = receiverFactory.newReceiver(shardId, this, OptionalLong.empty());
            shardReceivers.put(shardId, receiver);
        }
        receiver.start();
    }

    private CompletableFuture<Void> dispatchSnapshot(
            NotificationContinuitySnapshot snapshot,
            List<ContinuityRegistration> listeners,
            Runnable beforeListeners) {
        return continuityDispatcher.execute(
                () -> {
                    if (beforeListeners != null) {
                        beforeListeners.run();
                    }
                    listeners.forEach(registration -> registration.publish(snapshot));
                });
    }

    private void failPendingStage(CompletableFuture<Void> stage) {
        if (stage != null) {
            stage.completeExceptionally(
                    new IllegalStateException("notification continuity generation was superseded"));
        }
    }

    private void cancelRetryLocked() {
        if (retryTask != null) {
            retryTask.cancel(false);
            retryTask = null;
        }
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
        final CompletableFuture<Void> terminalReadyFuture;
        final CompletableFuture<Void> publication;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            assignmentContinuityReady = false;
            cancelRetryLocked();
            oldReadyFuture = readyFuture;
            if (generation < Long.MAX_VALUE) {
                generation += 1;
            }
            readyFuture = new CompletableFuture<>();
            terminalReadyFuture = readyFuture;
            currentSnapshot = newSnapshotLocked(NotificationContinuityState.CLOSED);
            closedSnapshot = currentSnapshot;
            listeners = List.copyOf(continuityRegistrations);
            receivers = new ArrayList<>(shardReceivers.values());
            shardReceivers.clear();
            publication =
                    dispatchSnapshot(
                            closedSnapshot,
                            listeners,
                            () -> {
                                if (oldReadyFuture != null) {
                                    oldReadyFuture.completeExceptionally(
                                            new CancellationException("notification manager was closed"));
                                }
                                terminalReadyFuture.completeExceptionally(
                                        new CancellationException("notification manager was closed"));
                            });
        }

        receivers.forEach(ShardNotificationReceiver::close);
        if (!continuityDispatcher.isDispatchThread()) {
            publication.join();
        }
    }

    private record GenerationTransition(
            long generation,
            Set<Shard> assignment,
            NotificationContinuitySnapshot snapshot,
            List<ContinuityRegistration> listeners,
            CompletableFuture<Void> oldReadyFuture,
            List<ShardNotificationReceiver> oldReceivers,
            boolean openReceivers) {}

    private record ReceiverPlan(
            long generation, Set<Shard> assignment, List<ShardNotificationReceiver> oldReceivers) {}

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
