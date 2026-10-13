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
package io.oxia.client;

import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.SyncOxiaClient;
import java.util.concurrent.CompletableFuture;

/** Original SDK allocations and opening ownership; never exposes a half-constructed read/write client. */
public final class OxiaClientLifecycle {
    private OxiaClientLifecycle() {}

    public static Opening openSync(OxiaClientBuilder builder) {
        if (!(builder instanceof OxiaClientBuilderImpl original)) {
            throw new IllegalArgumentException("requires the original Oxia client builder");
        }
        var cleanup = new OxiaClientCleanup();
        try {
            var configuration = original.getClientConfig();
            if (configuration.requestTimeout().toMillis() < 1 || configuration.requestTimeout().toMillis() > 60_000) {
                throw new IllegalArgumentException("owned Oxia opening requires a request timeout within 1..60000ms");
            }
            var future = AsyncOxiaClientImpl.newInstance(configuration, cleanup)
                    .thenApply(client -> (SyncOxiaClient) new SyncOxiaClientImpl(client));
            future.whenComplete((value, error) -> {
                if (future.isCancelled()) cleanup.close(java.time.Duration.ofNanos(1));
            });
            return new Opening(cleanup, future, configuration);
        } catch (Throwable failure) {
            cleanup.openingFinished = true;
            cleanup.retainFailure(failure);
            throw new OpeningFailure(failure, cleanup);
        }
    }

    public static OxiaClientCleanup cleanupOf(SyncOxiaClient client) {
        if (!(client instanceof SyncOxiaClientImpl original)) {
            throw new IllegalArgumentException("requires the original Oxia sync client");
        }
        return original.cleanup();
    }

    public record Opening(OxiaClientCleanup cleanup, CompletableFuture<SyncOxiaClient> client, ClientConfig configuration) {}

    public static final class OpeningFailure extends IllegalStateException {
        private static final long serialVersionUID = 1L;
        private final transient OxiaClientCleanup cleanup;
        OpeningFailure(Throwable cause, OxiaClientCleanup cleanup) {
            super("Oxia opening failed; original cleanup remains owned", cause);
            this.cleanup = cleanup;
        }
        public OxiaClientCleanup cleanup() { return cleanup; }
    }
}
