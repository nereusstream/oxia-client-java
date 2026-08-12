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
package io.oxia.client.notify;

import static io.oxia.proto.NotificationType.KEY_CREATED;
import static io.oxia.proto.NotificationType.KEY_DELETED;
import static io.oxia.proto.NotificationType.KEY_MODIFIED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.oxia.client.api.Notification;
import io.oxia.client.api.Notification.KeyCreated;
import io.oxia.client.api.Notification.KeyDeleted;
import io.oxia.client.api.Notification.KeyModified;
import io.oxia.client.grpc.RpcProvider;
import io.oxia.client.metrics.Counter;
import io.oxia.proto.NotificationBatch;
import io.oxia.proto.NotificationsRequest;
import io.oxia.proto.OxiaClientGrpc;
import java.util.OptionalLong;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ShardNotificationReceiverTest {

    record NotificationWrapper(NotificationBatch notifications, Exception ex, boolean endOfStream) {}

    BlockingQueue<NotificationWrapper> responses = new ArrayBlockingQueue<>(10);

    OxiaClientGrpc.OxiaClientImplBase serviceImpl =
            new OxiaClientGrpc.OxiaClientImplBase() {
                @Override
                public void getNotifications(
                        NotificationsRequest request, StreamObserver<NotificationBatch> responseObserver) {
                    requests.incrementAndGet();
                    NotificationWrapper nw = responses.poll();
                    if (nw != null) {
                        if (nw.ex != null) {
                            responseObserver.onError(nw.ex);
                        } else {
                            responseObserver.onNext(nw.notifications);

                            if (nw.endOfStream) {
                                responseObserver.onCompleted();
                            }
                        }
                    }
                }
            };
    AtomicInteger requests = new AtomicInteger();

    String serverName = InProcessServerBuilder.generateName();
    Server server;
    ManagedChannel channel;

    long shardId = 1L;
    @Mock RpcProvider rpcProvider;
    @Mock Consumer<Notification> notificationCallback;
    @Mock NotificationManager notificationManager;

    @BeforeEach
    void beforeEach() throws Exception {
        requests.set(0);
        responses.clear();
        server =
                InProcessServerBuilder.forName(serverName)
                        .directExecutor()
                        .addService(serviceImpl)
                        .build()
                        .start();
        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        doAnswer(
                        invocation -> {
                            OxiaClientGrpc.newStub(channel)
                                    .getNotifications(invocation.getArgument(0), invocation.getArgument(1));
                            return null;
                        })
                .when(rpcProvider)
                .getNotifications(any(), any());
        lenient().when(notificationManager.onBatch(any(), any())).thenReturn(true);
    }

    @AfterEach
    void afterEach() throws Exception {
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    void start() throws Exception {
        when(notificationManager.getCounterNotificationsReceived()).thenReturn(mock(Counter.class));
        when(notificationManager.getCounterNotificationsBatchesReceived())
                .thenReturn(mock(Counter.class));

        var notifications = new NotificationBatch();
        notifications.putNotifications("key1").copyFrom(created(1L));
        notifications.putNotifications("key2").copyFrom(deleted(2L));
        notifications.putNotifications("key3").copyFrom(modified(3L));
        responses.put(new NotificationWrapper(notifications, null, false));
        try (var notificationReceiver =
                new ShardNotificationReceiver(
                        rpcProvider,
                        shardId,
                        notificationCallback,
                        notificationManager,
                        OptionalLong.empty())) {
            notificationReceiver.start();
            await()
                    .untilAsserted(
                            () -> {
                                verify(notificationCallback).accept(new KeyCreated("key1", 1L));
                                verify(notificationCallback).accept(new KeyDeleted("key2"));
                                verify(notificationCallback).accept(new KeyModified("key3", 3L));
                            });
        }
    }

    @Test
    void neverStarts() {
        //        responses.offer(Flux.never());
        try (var notificationReceiver =
                new ShardNotificationReceiver(
                        rpcProvider,
                        shardId,
                        notificationCallback,
                        notificationManager,
                        OptionalLong.empty())) {
            notificationReceiver.start();
            await()
                    .untilAsserted(
                            () -> {
                                verify(notificationCallback, never()).accept(any());
                            });
        }
    }

    @Test
    void staleFirstBatchIsDroppedWithoutPayloadDelivery() throws Exception {
        when(notificationManager.onBatch(any(), any())).thenReturn(false);
        var notifications = newNotificationBatch("key1", created(1L));
        responses.put(new NotificationWrapper(notifications, null, false));
        try (var notificationReceiver =
                new ShardNotificationReceiver(
                        rpcProvider,
                        shardId,
                        notificationCallback,
                        notificationManager,
                        OptionalLong.empty())) {
            notificationReceiver.start();
            await()
                    .untilAsserted(
                            () -> verify(notificationManager).onBatch(notificationReceiver, notifications));
            verify(notificationCallback, never()).accept(any());
            verify(notificationManager, never()).getCounterNotificationsReceived();
            verify(notificationManager, never()).getCounterNotificationsBatchesReceived();
        }
    }

    @Test
    public void reportsErrorToManagerWithoutReceiverLocalRetry() {
        responses.offer(new NotificationWrapper(null, Status.UNAVAILABLE.asException(), false));
        try (var notificationReceiver =
                new ShardNotificationReceiver(
                        rpcProvider,
                        shardId,
                        notificationCallback,
                        notificationManager,
                        OptionalLong.empty())) {
            notificationReceiver.start();
            await()
                    .untilAsserted(
                            () -> verify(notificationManager).onReceiverError(eq(notificationReceiver), any()));
        }
        assertThat(requests).hasValue(1);
    }

    @Test
    public void reportsEndOfStreamToManagerWithoutReceiverLocalRetry() throws Exception {
        when(notificationManager.getCounterNotificationsReceived()).thenReturn(mock(Counter.class));
        when(notificationManager.getCounterNotificationsBatchesReceived())
                .thenReturn(mock(Counter.class));
        var notifications = newNotificationBatch("key1", created(1L));
        responses.put(new NotificationWrapper(notifications, null, true));
        try (var notificationReceiver =
                new ShardNotificationReceiver(
                        rpcProvider,
                        shardId,
                        notificationCallback,
                        notificationManager,
                        OptionalLong.empty())) {
            notificationReceiver.start();
            await()
                    .untilAsserted(
                            () -> {
                                verify(notificationCallback).accept(new KeyCreated("key1", 1L));
                                verify(notificationManager).onReceiverCompleted(notificationReceiver);
                            });
        }
        assertThat(requests).hasValue(1);
    }

    static NotificationBatch newNotificationBatch(
            String key, io.oxia.proto.Notification notification) {
        var batch = new NotificationBatch();
        batch.putNotifications(key).copyFrom(notification);
        return batch;
    }

    static io.oxia.proto.Notification created(long version) {
        var notification = new io.oxia.proto.Notification();
        notification.setType(KEY_CREATED).setVersionId(version);
        return notification;
    }

    static io.oxia.proto.Notification deleted(long version) {
        var notification = new io.oxia.proto.Notification();
        notification.setType(KEY_DELETED).setVersionId(version);
        return notification;
    }

    static io.oxia.proto.Notification modified(long version) {
        var notification = new io.oxia.proto.Notification();
        notification.setType(KEY_MODIFIED).setVersionId(version);
        return notification;
    }
}
