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
import static io.oxia.client.api.NotificationContinuityState.READY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import io.oxia.client.CompositeConsumer;
import io.oxia.client.api.Notification;
import io.oxia.client.api.NotificationContinuitySnapshot;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.shard.HashRange;
import io.oxia.client.shard.Shard;
import io.oxia.client.shard.ShardManager;
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

    private ScheduledExecutorService executor;
    private NotificationManager manager;

    @BeforeEach
    void setup() {
        executor = Executors.newSingleThreadScheduledExecutor();
        when(receiverFactory.getCallback()).thenReturn(new CompositeConsumer<Notification>());
        manager =
                new NotificationManager(executor, receiverFactory, shardManager, InstrumentProvider.NOOP);
        lenient()
                .when(receiverFactory.newReceiver(anyLong(), eq(manager), any(OptionalLong.class)))
                .thenAnswer(invocation -> invocation.<Long>getArgument(0) == 1L ? receiver1 : receiver2);
        lenient().when(receiver1.getShardId()).thenReturn(1L);
        lenient().when(receiver2.getShardId()).thenReturn(2L);
    }

    @AfterEach
    void teardown() {
        manager.close();
        executor.shutdownNow();
    }

    @Test
    void emptyFirstBatchMakesSingleShardReady() {
        when(shardManager.allShards())
                .thenReturn(Set.of(new Shard(1L, "leader1", new HashRange(0, 1))));
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
    void allCurrentShardsMustReachTheirFirstBatch() {
        when(shardManager.allShards())
                .thenReturn(
                        Set.of(
                                new Shard(1L, "leader1", new HashRange(0, 1)),
                                new Shard(2L, "leader2", new HashRange(2, 3))));
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
        when(shardManager.allShards()).thenReturn(Set.of());

        var registration = manager.registerContinuityListener(snapshot -> {});

        assertThat(registration.current().state()).isEqualTo(ARMING);
        assertThat(registration.current().readyStage().toCompletableFuture()).isNotCompleted();
    }

    @Test
    void deregistrationIsIdempotentAndSuppressesQueuedCallbacks() throws Exception {
        when(shardManager.allShards())
                .thenReturn(Set.of(new Shard(1L, "leader1", new HashRange(0, 1))));
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
}
