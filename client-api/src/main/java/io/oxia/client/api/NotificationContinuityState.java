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

/** The process-local lifecycle of the notification streams for one Oxia client. */
public enum NotificationContinuityState {
    /** The current generation has not observed a first notification batch from every shard. */
    ARMING,

    /** Every shard in the current assignment has observed its first notification batch. */
    READY,

    /** The client is closed and cannot establish another notification generation. */
    CLOSED
}
