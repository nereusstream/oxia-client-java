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

import org.testcontainers.utility.DockerImageName;

public final class OxiaImages {
    private static final String DEFAULT_IMAGE = "oxia/oxia:0.16.3";

    public static final DockerImageName OXIA = configuredImage();

    private OxiaImages() {}

    private static DockerImageName configuredImage() {
        var configured = System.getenv("OXIA_TEST_IMAGE");
        if (configured == null || configured.isBlank()) {
            configured = System.getProperty("oxia.test.image", DEFAULT_IMAGE);
        }
        var image = DockerImageName.parse(configured);
        return DEFAULT_IMAGE.equals(configured) ? image : image.asCompatibleSubstituteFor("oxia/oxia");
    }
}
