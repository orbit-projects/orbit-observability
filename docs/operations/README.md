# Orbit Observability: operations and security

This guide organizes runtime behavior documented by the package. It does not certify production readiness. Verify provider/client versions, permissions, transport security, limits, and failure behavior in the target environment before release.

## Configuration surface

Environment names found in the package README:

The package README does not name `ORBIT_*` variables. Use its typed constructors and application configuration, and confirm exact runtime inputs in the implementation before deployment.

Use the package README's constructor and deployment examples. Store credentials in a secret manager and avoid logging credentials, raw provider errors, request data, or opaque cursors.

## Lifecycle, failure behavior, and limits

`orbit-observability` coordinates bounded flush and shutdown for explicitly selected logs,
metrics, and tracing providers. Provider-neutral logging, metrics, and tracing APIs and their vendor
adapters remain in their owning packages; this package composes their lifecycle without importing
those packages or any vendor SDK.

**Status:** pre-alpha lifecycle capability. It does not install instrumentation, collect telemetry,
select exporters, or configure a backend. Core and applications still decide which provider adapters
to install and wire.

## Install

```bash
pip install orbit-observability
```

## Provider lifecycle contract

Providers implement `TelemetryProvider`, declare one or more signals, and own their exporter
resources. The application transfers ownership explicitly to `ObservabilityLifecycle`:

```python
from orbit_observability import ObservabilityLifecycle, TelemetrySignal


class AppTelemetryProvider:
    name = "otlp"
    signals = frozenset({TelemetrySignal.METRICS, TelemetrySignal.TRACES})

    async def force_flush(self, *, timeout_seconds: float) -> None:
        await self.exporter.force_flush(timeout_seconds=timeout_seconds)

    async def shutdown(self, *, timeout_seconds: float) -> None:
        await self.exporter.shutdown(timeout_seconds=timeout_seconds)


lifecycle = ObservabilityLifecycle([AppTelemetryProvider()], max_concurrency=4)
```

Call `force_flush()` at an application-defined checkpoint and `shutdown()` during application
shutdown. Both operations are async, attempt every configured provider, limit concurrent operations,
and enforce a per-provider timeout greater than 0 and at most 300 seconds. Cancellation
propagates to active flushes. Shutdown is one-shot, continues in a retained task if its caller is
cancelled, and can be awaited again. Providers must make their own shutdown idempotent and release
all owned workers, queues, clients, and transports.

Failures are aggregated as stable provider slugs plus `timeout` or `provider_failure`; raw vendor
exceptions are deliberately omitted because they may contain credentials, telemetry values, or
request details. Providers must still apply their own data-minimization and sensitive-attribute
policy before export. This package does not make arbitrary telemetry safe to collect or transmit.

The provider lifecycle is independent of Core's plugin lifecycle. Application integration code
must arrange ownership and call shutdown in the correct order relative to its own event loop and
other resources. No global SDK provider is installed or mutated.

## Production validation

Validate startup/shutdown cleanup, timeout and cancellation behavior, concurrency and payload bounds where applicable, secret rotation and least-privilege access, data durability, backup/restore, and failover against the selected provider. Do not infer distributed or durable guarantees from an in-process API or fake-client tests.
