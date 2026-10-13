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
package io.oxia.client.util;

/** Marks actual native callback/executor execution, so client cleanup cannot wait for itself. */
public final class NativeCallbackScope {
    private static final ThreadLocal<Boolean> ACTIVE = ThreadLocal.withInitial(() -> false);
    private NativeCallbackScope() {}
    public static boolean active() { return ACTIVE.get(); }
    public static void run(Runnable action) {
        boolean prior = ACTIVE.get();
        ACTIVE.set(true);
        try { action.run(); }
        finally { if (prior) ACTIVE.set(true); else ACTIVE.remove(); }
    }
}
