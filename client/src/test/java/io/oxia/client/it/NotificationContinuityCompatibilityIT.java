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
package io.oxia.client.it;

import static io.oxia.client.api.NotificationContinuityState.ARMING;
import static io.oxia.client.api.NotificationContinuityState.READY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.Notification;
import io.oxia.client.api.Notification.KeyCreated;
import io.oxia.client.api.NotificationContinuityRegistration;
import io.oxia.client.api.NotificationContinuitySnapshot;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.testcontainers.OxiaContainer;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class NotificationContinuityCompatibilityIT {
    @Container
    private static final OxiaContainer oxia =
            new OxiaContainer(OxiaImages.OXIA)
                    .withShards(3)
                    .withLogConsumer(
                            new Slf4jLogConsumer(
                                    LoggerFactory.getLogger(NotificationContinuityCompatibilityIT.class)));

    private static final List<NotificationContinuitySnapshot> continuity =
            new CopyOnWriteArrayList<>();
    private static final List<Notification> notifications = new CopyOnWriteArrayList<>();

    private static AsyncOxiaClient client;
    private static NotificationContinuityRegistration registration;

    @BeforeAll
    static void beforeAll() {
        client = OxiaClientBuilder.create(oxia.getServiceAddress()).asyncClient().join();
        registration = client.notificationContinuity(continuity::add);
        client.notifications(notifications::add);
    }

    @AfterAll
    static void afterAll() throws Exception {
        if (registration != null) {
            registration.close();
        }
        if (client != null) {
            client.close();
        }
    }

    @Test
    void exactServerDummyArmsContinuityAndNonEmptyPayloadRemainsCompatible() {
        await()
                .atMost(Duration.ofSeconds(30))
                .untilAsserted(
                        () -> {
                            assertThat(registration.current().state()).isEqualTo(READY);
                            assertThat(registration.current().readyStage().toCompletableFuture()).isCompleted();
                            assertThat(continuity)
                                    .extracting(NotificationContinuitySnapshot::state)
                                    .containsSequence(ARMING, READY);
                        });

        var key = "o1-exact-compatibility-" + UUID.randomUUID();
        var result = client.put(key, "value".getBytes(UTF_8)).join();

        await()
                .atMost(Duration.ofSeconds(30))
                .untilAsserted(
                        () ->
                                assertThat(notifications)
                                        .containsExactly(new KeyCreated(key, result.version().versionId())));
        assertThat(registration.current().state()).isEqualTo(READY);
    }
}
