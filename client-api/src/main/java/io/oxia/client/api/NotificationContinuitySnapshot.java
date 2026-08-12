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
package io.oxia.client.api;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * An immutable view of one process-local notification continuity generation.
 *
 * <p>{@link #readyStage()} completes successfully only when this exact generation reaches {@link
 * NotificationContinuityState#READY}. It completes exceptionally when the generation is lost before
 * becoming ready. A successfully completed stage says nothing about a later generation; consumers
 * must continue observing snapshots.
 *
 * @param generation the positive, monotonically increasing process-local generation
 * @param state the current state of this generation
 * @param readyStage a client-owned, non-completable view of this generation's ready stage
 */
public record NotificationContinuitySnapshot(
        long generation, NotificationContinuityState state, CompletionStage<Void> readyStage) {

    public NotificationContinuitySnapshot {
        if (generation <= 0) {
            throw new IllegalArgumentException("generation must be positive");
        }
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(readyStage, "readyStage");
    }
}
