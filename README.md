# Oxia Java client SDK

[![Build](https://github.com/oxia-db/oxia-client-java/actions/workflows/ci-build-test.yaml/badge.svg)](https://github.com/oxia-db/oxia-client-java/actions/workflows/ci-build-test.yaml)

## Overview

This project comprises JDK language compatible modules for the [Oxia][oxia] service. It provides
the following capabilities:

- Client for the Oxia service — see the [user guide](https://oxia-db.github.io/docs/clients/java)
- [Oxia Testcontainers](https://github.com/oxia-db/testcontainers) for integration testing with a local Oxia service
- [Performance Test Tool](perf/) for performance testing with an Oxia service

## Build

Requirements:

* JDK 17
* Gradle 9.4+ (wrapper included)

Common build actions:

|             Action              |                 Command                   |
|---------------------------------|-------------------------------------------|
| Full build and test             | `./gradlew build`                         |
| Skip tests                      | `./gradlew build -x test`                |
| Skip Jacoco test coverage check | `./gradlew build -x jacocoTestReport`    |
| Skip Spotless formatting check  | `./gradlew build -x spotlessCheck`       |
| Format code                     | `./gradlew spotlessApply`                |

### Contributing to Oxia

Please 🌟 star the project if you like it.

Feel free to open an [issue](https://github.com/oxia-db/oxia/issues/new) or start a [discussion](https://github.com/oxia-db/oxia/discussions/new/choose). You can also follow the development [guide]() to contribute and build on it.

### License

Copyright 2022-2026 The Oxia Authors

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

### Owned client cleanup for Nereus Delay

The `nereus/delay-client-cleanup-v1` branch is based on `v0.9.0` and adds
`OxiaClientLifecycle.openSync(builder)`. Its opening retains a cleanup-only
capability before the client is exposed. Construction/startup failure and a
cancelled or late opening keep the same capability; no half-constructed client
is returned. The owned opening requires an actual request timeout of 1..60000ms.

`cleanup.close(Duration)` stops admission and uses one observation deadline.
`cleanup.status()` reports original batch-thread exit, owned gRPC channel/call
termination and executor termination, independently of the first retained
failure. Run cleanup on the application's existing bounded I/O executor; retain
that original task if a callback blocks, and observe it to exit before another
attempt. Native callbacks never wait for themselves. Pending shutdowns retain
original objects; unhealthy/replaced and partially constructed channels remain
owned until termination. Global Netty/OpenTelemetry facilities are not closed.

Native termination is not evidence of a remote Put/Delete/CAS outcome, ephemeral
marker deletion, Owner release or checkpoint completion. Those obligations stay
with the application. This branch has development compilation evidence only;
fault, Broker and recovery validation is deferred with NDIP-3.
