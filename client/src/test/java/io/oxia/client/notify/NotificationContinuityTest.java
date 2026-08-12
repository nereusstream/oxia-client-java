/*
 * Copyright © 2026 The Oxia Authors
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
import static io.oxia.proto.NotificationType.KEY_CREATED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.oxia.client.CompositeConsumer;
import io.oxia.client.api.Notification;
import io.oxia.client.api.Notification.KeyCreated;
import io.oxia.client.api.NotificationContinuitySnapshot;
import io.oxia.client.grpc.RpcProvider;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.shard.HashRange;
import io.oxia.client.shard.Shard;
import io.oxia.client.shard.ShardManager;
import io.oxia.client.shard.ShardManager.ShardAssignmentContinuitySnapshot;
import io.oxia.client.shard.ShardManager.ShardAssignmentContinuityState;
import io.oxia.client.util.Backoff;
import io.oxia.proto.NotificationBatch;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class NotificationContinuityTest {
    @Mock ShardManager shardManager;
    @Mock ShardNotificationReceiver.Factory receiverFactory;
    @Mock ShardNotificationReceiver receiver1;
    @Mock ShardNotificationReceiver receiver2;
    @Mock ShardNotificationReceiver receiver1Replacement;
    @Mock ShardNotificationReceiver receiver2Replacement;
    @Mock Backoff backoff;

    private ScheduledExecutorService executor;
    private NotificationManager manager;

    @BeforeEach
    void setup() {
        executor = Executors.newSingleThreadScheduledExecutor();
        when(receiverFactory.getCallback()).thenReturn(new CompositeConsumer<Notification>());
        manager =
                new NotificationManager(
                        executor, receiverFactory, shardManager, InstrumentProvider.NOOP, backoff);
        lenient().when(backoff.nextDelayMillis()).thenReturn(0L);
        lenient()
                .when(receiverFactory.newReceiver(anyLong(), eq(manager), any(OptionalLong.class)))
                .thenAnswer(invocation -> invocation.<Long>getArgument(0) == 1L ? receiver1 : receiver2);
        lenient().when(receiver1.getShardId()).thenReturn(1L);
        lenient().when(receiver2.getShardId()).thenReturn(2L);
        lenient().when(receiver1Replacement.getShardId()).thenReturn(1L);
        lenient().when(receiver2Replacement.getShardId()).thenReturn(2L);
    }

    @AfterEach
    void teardown() {
        manager.close();
        executor.shutdownNow();
    }

    @Test
    void emptyFirstBatchMakesSingleShardReady() {
        when(shardManager.assignmentContinuity())
                .thenReturn(ready(Set.of(new Shard(1L, "leader1", new HashRange(0, 1)))));
        List<NotificationContinuitySnapshot> snapshots = new CopyOnWriteArrayList<>();

        var registration = manager.registerContinuityListener(snapshots::add);
        await()
                .untilAsserted(
                        () ->
                                assertThat(snapshots)
                                        .extracting(NotificationContinuitySnapshot::state)
                                        .containsExactly(ARMING));

        assertThat(manager.onBatch(receiver1, new NotificationBatch())).isTrue();

        await()
                .untilAsserted(
                        () -> {
                            assertThat(snapshots)
                                    .extracting(NotificationContinuitySnapshot::state)
                                    .containsExactly(ARMING, READY);
                            assertThat(snapshots)
                                    .extracting(NotificationContinuitySnapshot::generation)
                                    .containsOnly(1L);
                            assertThat(registration.current().state()).isEqualTo(READY);
                            assertThat(registration.current().readyStage().toCompletableFuture()).isCompleted();
                        });
    }

    @Test
    void nonEmptyFirstBatchMakesReadyAndDeliversPayloadExactlyOnce() {
        manager.close();
        var callback = new CompositeConsumer<Notification>();
        var realReceiverFactory = mock(ShardNotificationReceiver.Factory.class);
        var rpcProvider = mock(RpcProvider.class);
        when(realReceiverFactory.getCallback()).thenReturn(callback);
        when(shardManager.assignmentContinuity())
                .thenReturn(ready(Set.of(new Shard(1L, "leader1", new HashRange(0, 1)))));
        manager =
                new NotificationManager(
                        executor, realReceiverFactory, shardManager, InstrumentProvider.NOOP, backoff);
        when(realReceiverFactory.newReceiver(1L, manager, OptionalLong.empty()))
                .thenAnswer(
                        invocation ->
                                new ShardNotificationReceiver(
                                        rpcProvider, 1L, callback, manager, OptionalLong.empty()));
        var batch = new NotificationBatch().setOffset(7L);
        batch.putNotifications("key").setType(KEY_CREATED).setVersionId(3L);
        org.mockito.Mockito.doAnswer(
                        invocation -> {
                            invocation
                                    .<io.oxia.client.grpc.observer.CancelableStreamObserver<NotificationBatch>>
                                            getArgument(1)
                                    .onNext(batch);
                            return null;
                        })
                .when(rpcProvider)
                .getNotifications(any(), any());
        List<Notification> payloads = new CopyOnWriteArrayList<>();
        callback.add(payloads::add);

        var registration = manager.registerContinuityListener(snapshot -> {});

        await()
                .untilAsserted(
                        () -> {
                            assertThat(registration.current().state()).isEqualTo(READY);
                            assertThat(registration.current().readyStage().toCompletableFuture()).isCompleted();
                            assertThat(payloads).containsExactly(new KeyCreated("key", 3L));
                        });
    }

    @Test
    void allCurrentShardsMustReachTheirFirstBatch() {
        when(shardManager.assignmentContinuity())
                .thenReturn(
                        ready(
                                Set.of(
                                        new Shard(1L, "leader1", new HashRange(0, 1)),
                                        new Shard(2L, "leader2", new HashRange(2, 3)))));
        List<NotificationContinuitySnapshot> snapshots = new CopyOnWriteArrayList<>();

        var registration = manager.registerContinuityListener(snapshots::add);
        await().untilAsserted(() -> assertThat(snapshots).hasSize(1));

        assertThat(manager.onBatch(receiver1, new NotificationBatch())).isTrue();
        assertThat(registration.current().state()).isEqualTo(ARMING);
        assertThat(registration.current().readyStage().toCompletableFuture()).isNotCompleted();

        assertThat(manager.onBatch(receiver2, new NotificationBatch())).isTrue();
        await().untilAsserted(() -> assertThat(registration.current().state()).isEqualTo(READY));
    }

    @Test
    void emptyAssignmentIsNotVacuouslyReady() {
        when(shardManager.assignmentContinuity()).thenReturn(ready(Set.of()));

        var registration = manager.registerContinuityListener(snapshot -> {});

        assertThat(registration.current().state()).isEqualTo(ARMING);
        assertThat(registration.current().readyStage().toCompletableFuture()).isNotCompleted();
    }

    @Test
    void deregistrationIsIdempotentAndSuppressesQueuedCallbacks() throws Exception {
        when(shardManager.assignmentContinuity())
                .thenReturn(ready(Set.of(new Shard(1L, "leader1", new HashRange(0, 1)))));
        List<NotificationContinuitySnapshot> snapshots = new CopyOnWriteArrayList<>();
        var dispatcherBlocked = new CountDownLatch(1);
        var releaseDispatcher = new CountDownLatch(1);
        executor.execute(
                () -> {
                    dispatcherBlocked.countDown();
                    try {
                        releaseDispatcher.await();
                    } catch (InterruptedException error) {
                        Thread.currentThread().interrupt();
                    }
                });
        dispatcherBlocked.await();

        var registration = manager.registerContinuityListener(snapshots::add);
        registration.close();
        registration.close();
        manager.onBatch(receiver1, new NotificationBatch());

        releaseDispatcher.countDown();
        executor.submit(() -> {}).get();
        assertThat(snapshots).isEmpty();
        assertThat(registration.current().state()).isEqualTo(READY);
    }

    @Test
    void errorBeforeReadyFailsOldStageAndPublishesLossBeforeRetryReady() {
        when(shardManager.assignmentContinuity())
                .thenReturn(ready(Set.of(new Shard(1L, "leader1", new HashRange(0, 1)))));
        when(receiverFactory.newReceiver(1L, manager, OptionalLong.empty()))
                .thenReturn(receiver1, receiver1Replacement);
        List<NotificationContinuitySnapshot> snapshots = new CopyOnWriteArrayList<>();

        var registration = manager.registerContinuityListener(snapshots::add);
        var generationOneStage = registration.current().readyStage().toCompletableFuture();
        manager.onReceiverError(receiver1, new IllegalStateException("lost before ready"));

        await()
                .untilAsserted(
                        () -> {
                            assertStates(snapshots, ARMING, ARMING);
                            assertThat(snapshots)
                                    .extracting(NotificationContinuitySnapshot::generation)
                                    .containsExactly(1L, 2L);
                            assertThat(generationOneStage).isCompletedExceptionally();
                            verify(receiver1).close();
                            verify(receiver1Replacement).start();
                        });

        assertThat(manager.onBatch(receiver1Replacement, new NotificationBatch())).isTrue();
        await().untilAsserted(() -> assertStates(snapshots, ARMING, ARMING, READY));
        assertThat(registration.current().generation()).isEqualTo(2L);
    }

    @Test
    void errorAfterReadyCreatesFreshStageAndOldSuccessCannotAuthorizeIt() {
        when(shardManager.assignmentContinuity())
                .thenReturn(ready(Set.of(new Shard(1L, "leader1", new HashRange(0, 1)))));
        when(receiverFactory.newReceiver(1L, manager, OptionalLong.empty()))
                .thenReturn(receiver1, receiver1Replacement);
        List<NotificationContinuitySnapshot> snapshots = new CopyOnWriteArrayList<>();

        var registration = manager.registerContinuityListener(snapshots::add);
        manager.onBatch(receiver1, new NotificationBatch());
        await().untilAsserted(() -> assertThat(registration.current().state()).isEqualTo(READY));
        var generationOneStage = registration.current().readyStage().toCompletableFuture();
        assertThat(generationOneStage).isCompleted();

        manager.onReceiverError(receiver1, new IllegalStateException("lost after ready"));
        await()
                .untilAsserted(
                        () -> {
                            assertStates(snapshots, ARMING, READY, ARMING);
                            assertThat(registration.current().generation()).isEqualTo(2L);
                            assertThat(registration.current().readyStage().toCompletableFuture())
                                    .isNotCompleted();
                            assertThat(generationOneStage).isCompleted();
                        });

        manager.onBatch(receiver1Replacement, new NotificationBatch());
        await().untilAsserted(() -> assertStates(snapshots, ARMING, READY, ARMING, READY));
    }

    @Test
    void assignmentGapClosesReceiversAndWaitsForFreshFullSnapshot() {
        var assignment = Set.of(new Shard(1L, "leader1", new HashRange(0, 1)));
        when(shardManager.assignmentContinuity()).thenReturn(ready(assignment));
        when(receiverFactory.newReceiver(1L, manager, OptionalLong.empty()))
                .thenReturn(receiver1, receiver1Replacement);
        List<NotificationContinuitySnapshot> snapshots = new CopyOnWriteArrayList<>();

        var registration = manager.registerContinuityListener(snapshots::add);
        manager.onBatch(receiver1, new NotificationBatch());
        await().untilAsserted(() -> assertThat(registration.current().state()).isEqualTo(READY));

        manager.onContinuityChange(
                new ShardAssignmentContinuitySnapshot(
                        2, ShardAssignmentContinuityState.ARMING, assignment));
        await()
                .untilAsserted(
                        () -> {
                            assertThat(registration.current().state()).isEqualTo(ARMING);
                            assertThat(registration.current().generation()).isEqualTo(2L);
                            verify(receiver1).close();
                            verify(receiver1Replacement, never()).start();
                        });

        manager.onContinuityChange(
                new ShardAssignmentContinuitySnapshot(2, ShardAssignmentContinuityState.READY, assignment));
        await().untilAsserted(() -> verify(receiver1Replacement).start());
        assertThat(registration.current().generation()).isEqualTo(2L);
        assertThat(registration.current().state()).isEqualTo(ARMING);

        manager.onBatch(receiver1Replacement, new NotificationBatch());
        await().untilAsserted(() -> assertThat(registration.current().state()).isEqualTo(READY));
    }

    @Test
    void laterAssignmentReadyCutsGenerationEvenIfArmingCallbackWasDelayed() {
        var assignment = Set.of(new Shard(1L, "leader1", new HashRange(0, 1)));
        when(shardManager.assignmentContinuity()).thenReturn(ready(assignment));
        when(receiverFactory.newReceiver(1L, manager, OptionalLong.empty()))
                .thenReturn(receiver1, receiver1Replacement);
        var registration = manager.registerContinuityListener(snapshot -> {});
        manager.onBatch(receiver1, new NotificationBatch());
        await().untilAsserted(() -> assertThat(registration.current().state()).isEqualTo(READY));

        manager.onContinuityChange(
                new ShardAssignmentContinuitySnapshot(2, ShardAssignmentContinuityState.READY, assignment));
        manager.onContinuityChange(
                new ShardAssignmentContinuitySnapshot(
                        2, ShardAssignmentContinuityState.ARMING, assignment));

        assertThat(registration.current().generation()).isEqualTo(2L);
        assertThat(registration.current().state()).isEqualTo(ARMING);
        verify(receiver1).close();
        verify(receiver1Replacement).start();
        manager.onBatch(receiver1Replacement, new NotificationBatch());
        await().untilAsserted(() -> assertThat(registration.current().state()).isEqualTo(READY));
    }

    @Test
    void assignmentChangeRebuildsAllShardsAndRejectsOldGenerationBatch() {
        var firstAssignment = Set.of(new Shard(1L, "leader1", new HashRange(0, 3)));
        var secondAssignment =
                Set.of(
                        new Shard(1L, "leader1", new HashRange(0, 1)),
                        new Shard(2L, "leader2", new HashRange(2, 3)));
        when(shardManager.assignmentContinuity()).thenReturn(ready(firstAssignment));
        when(receiverFactory.newReceiver(1L, manager, OptionalLong.empty()))
                .thenReturn(receiver1, receiver1Replacement);
        when(receiverFactory.newReceiver(2L, manager, OptionalLong.empty()))
                .thenReturn(receiver2Replacement);
        var registration = manager.registerContinuityListener(snapshot -> {});
        manager.onBatch(receiver1, new NotificationBatch());
        await().untilAsserted(() -> assertThat(registration.current().state()).isEqualTo(READY));

        manager.onContinuityChange(
                new ShardAssignmentContinuitySnapshot(
                        1, ShardAssignmentContinuityState.READY, secondAssignment));

        assertThat(registration.current().generation()).isEqualTo(2L);
        assertThat(registration.current().state()).isEqualTo(ARMING);
        assertThat(manager.onBatch(receiver1, new NotificationBatch())).isFalse();
        verify(receiver1).close();
        verify(receiver1Replacement).start();
        verify(receiver2Replacement).start();

        manager.onBatch(receiver1Replacement, new NotificationBatch());
        assertThat(registration.current().state()).isEqualTo(ARMING);
        manager.onBatch(receiver2Replacement, new NotificationBatch());
        await().untilAsserted(() -> assertThat(registration.current().state()).isEqualTo(READY));
    }

    @Test
    void registrationDuringAssignmentOutageDoesNotOpenStaleReceivers() {
        var assignment = Set.of(new Shard(1L, "leader1", new HashRange(0, 1)));
        when(shardManager.assignmentContinuity())
                .thenReturn(
                        new ShardAssignmentContinuitySnapshot(
                                2, ShardAssignmentContinuityState.ARMING, assignment));
        when(receiverFactory.newReceiver(1L, manager, OptionalLong.empty())).thenReturn(receiver1);

        var registration = manager.registerContinuityListener(snapshot -> {});

        assertThat(registration.current().state()).isEqualTo(ARMING);
        verify(receiver1, never()).start();
        manager.onContinuityChange(
                new ShardAssignmentContinuitySnapshot(2, ShardAssignmentContinuityState.READY, assignment));
        verify(receiver1).start();
    }

    @Test
    void listenerFailureIsIsolatedFromOtherListenersAndReady() {
        when(shardManager.assignmentContinuity())
                .thenReturn(ready(Set.of(new Shard(1L, "leader1", new HashRange(0, 1)))));
        List<NotificationContinuitySnapshot> observed = new CopyOnWriteArrayList<>();
        manager.registerContinuityListener(
                snapshot -> {
                    throw new IllegalStateException("listener failure");
                });
        var registration = manager.registerContinuityListener(observed::add);

        manager.onBatch(receiver1, new NotificationBatch());

        await()
                .untilAsserted(
                        () -> {
                            assertStates(observed, ARMING, READY);
                            assertThat(registration.current().state()).isEqualTo(READY);
                        });
    }

    @Test
    void lateRegistrationAfterLossObservesCurrentGenerationBeforeItsReady() {
        when(shardManager.assignmentContinuity())
                .thenReturn(ready(Set.of(new Shard(1L, "leader1", new HashRange(0, 1)))));
        when(receiverFactory.newReceiver(1L, manager, OptionalLong.empty()))
                .thenReturn(receiver1, receiver1Replacement);
        manager.registerContinuityListener(snapshot -> {});
        manager.onBatch(receiver1, new NotificationBatch());
        manager.onReceiverCompleted(receiver1);
        List<NotificationContinuitySnapshot> lateSnapshots = new CopyOnWriteArrayList<>();

        var lateRegistration = manager.registerContinuityListener(lateSnapshots::add);

        await()
                .untilAsserted(
                        () -> {
                            assertThat(lateSnapshots).isNotEmpty();
                            assertThat(lateSnapshots.get(0).generation()).isEqualTo(2L);
                            assertThat(lateSnapshots.get(0).state()).isEqualTo(ARMING);
                        });
        manager.onBatch(receiver1Replacement, new NotificationBatch());
        await().untilAsserted(() -> assertThat(lateRegistration.current().state()).isEqualTo(READY));
    }

    @Test
    void closePublishesOneTerminalGenerationAndRejectsNewRegistration() {
        when(shardManager.assignmentContinuity())
                .thenReturn(ready(Set.of(new Shard(1L, "leader1", new HashRange(0, 1)))));
        List<NotificationContinuitySnapshot> snapshots = new CopyOnWriteArrayList<>();
        var registration = manager.registerContinuityListener(snapshots::add);

        manager.close();
        manager.close();

        assertThat(registration.current().state()).isEqualTo(CLOSED);
        assertThat(registration.current().generation()).isEqualTo(2L);
        assertThat(registration.current().readyStage().toCompletableFuture())
                .isCompletedExceptionally();
        assertThatThrownBy(() -> manager.registerContinuityListener(snapshot -> {}))
                .isInstanceOf(IllegalStateException.class);
        verify(receiver1).close();
        await().untilAsserted(() -> assertStates(snapshots, ARMING, CLOSED));
    }

    @Test
    void repeatedLossAndReadyUsesStrictlyIncreasingGenerations() {
        when(shardManager.assignmentContinuity())
                .thenReturn(ready(Set.of(new Shard(1L, "leader1", new HashRange(0, 1)))));
        when(receiverFactory.newReceiver(1L, manager, OptionalLong.empty()))
                .thenReturn(receiver1, receiver1Replacement, receiver2Replacement);
        when(receiver2Replacement.getShardId()).thenReturn(1L);
        List<NotificationContinuitySnapshot> snapshots = new CopyOnWriteArrayList<>();
        manager.registerContinuityListener(snapshots::add);

        manager.onBatch(receiver1, new NotificationBatch());
        manager.onReceiverError(receiver1, new IllegalStateException("first loss"));
        await().untilAsserted(() -> verify(receiver1Replacement).start());
        manager.onBatch(receiver1Replacement, new NotificationBatch());
        manager.onReceiverCompleted(receiver1Replacement);
        await().untilAsserted(() -> verify(receiver2Replacement).start());
        manager.onBatch(receiver2Replacement, new NotificationBatch());

        await()
                .untilAsserted(
                        () -> {
                            assertStates(snapshots, ARMING, READY, ARMING, READY, ARMING, READY);
                            assertThat(snapshots)
                                    .extracting(NotificationContinuitySnapshot::generation)
                                    .containsExactly(1L, 1L, 2L, 2L, 3L, 3L);
                            verify(backoff, times(3)).reset();
                        });
    }

    private static void assertStates(
            List<NotificationContinuitySnapshot> snapshots,
            io.oxia.client.api.NotificationContinuityState... expected) {
        assertThat(snapshots)
                .extracting(NotificationContinuitySnapshot::state)
                .containsExactly(expected);
    }

    private static ShardAssignmentContinuitySnapshot ready(Set<Shard> assignment) {
        return new ShardAssignmentContinuitySnapshot(
                1, ShardAssignmentContinuityState.READY, assignment);
    }
}
