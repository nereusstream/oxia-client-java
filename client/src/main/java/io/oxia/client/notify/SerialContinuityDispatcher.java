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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.NonNull;

/** Serializes continuity callbacks without creating a thread per client. */
final class SerialContinuityDispatcher {
    private final Executor executor;
    private final Queue<Runnable> tasks = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean draining = new AtomicBoolean();

    SerialContinuityDispatcher(@NonNull Executor executor) {
        this.executor = executor;
    }

    void execute(@NonNull Runnable task) {
        tasks.add(task);
        scheduleDrain();
    }

    private void scheduleDrain() {
        if (draining.compareAndSet(false, true)) {
            executor.execute(this::drain);
        }
    }

    private void drain() {
        try {
            Runnable task;
            while ((task = tasks.poll()) != null) {
                task.run();
            }
        } finally {
            draining.set(false);
            if (!tasks.isEmpty()) {
                scheduleDrain();
            }
        }
    }
}
