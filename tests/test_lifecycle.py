# Copyright 2026-present Orbit Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for bounded multi-provider flush, failure, cancellation, and shutdown behavior."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import pytest

from orbit_observability import (
    LifecycleOperation,
    ObservabilityLifecycle,
    ObservabilityLifecycleError,
    TelemetrySignal,
)


class Provider:
    """Small deterministic provider fake with optional lifecycle hooks."""

    def __init__(
        self,
        name: str,
        *,
        signals: frozenset[TelemetrySignal] = frozenset({TelemetrySignal.METRICS}),
        flush: Callable[[], Awaitable[None]] | None = None,
        stop: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self.name = name
        self.signals = signals
        self.flush_hook = flush
        self.stop_hook = stop
        self.flush_count = 0
        self.stop_count = 0

    async def force_flush(self, *, timeout_seconds: float) -> None:
        self.flush_count += 1
        if self.flush_hook is not None:
            await self.flush_hook()

    async def shutdown(self, *, timeout_seconds: float) -> None:
        self.stop_count += 1
        if self.stop_hook is not None:
            await self.stop_hook()


@pytest.mark.asyncio
async def test_flush_attempts_all_and_omits_vendor_exception_details() -> None:
    async def fail() -> None:
        raise RuntimeError("secret token=private-value")

    failed = Provider("metrics", flush=fail)
    healthy = Provider("traces")
    lifecycle = ObservabilityLifecycle((failed, healthy))
    with pytest.raises(ObservabilityLifecycleError) as captured:
        await lifecycle.force_flush()
    assert captured.value.failures[0].provider == "metrics"
    assert captured.value.failures[0].operation is LifecycleOperation.FLUSH
    assert "private-value" not in str(captured.value)
    assert failed.flush_count == healthy.flush_count == 1


@pytest.mark.asyncio
async def test_provider_identity_is_snapshotted_before_callbacks_run() -> None:
    async def fail() -> None:
        raise RuntimeError("failure")

    provider = Provider("metrics", flush=fail)
    lifecycle = ObservabilityLifecycle((provider,))
    provider.name = "token=private-value"
    with pytest.raises(ObservabilityLifecycleError) as captured:
        await lifecycle.force_flush()
    assert captured.value.failures[0].provider == "metrics"
    assert "private-value" not in str(captured.value)


@pytest.mark.asyncio
async def test_timeout_cancels_slow_provider_and_reports_safe_failure() -> None:
    cancelled = asyncio.Event()

    async def hang() -> None:
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    lifecycle = ObservabilityLifecycle((Provider("slow", flush=hang),))
    with pytest.raises(ObservabilityLifecycleError) as captured:
        await lifecycle.force_flush(timeout_seconds=0.01)
    assert cancelled.is_set()
    assert captured.value.failures[0].code == "timeout"


@pytest.mark.asyncio
async def test_shutdown_is_one_shot_and_flush_is_rejected_after_close() -> None:
    provider = Provider("metrics")
    lifecycle = ObservabilityLifecycle((provider,))
    await lifecycle.shutdown()
    await lifecycle.shutdown()
    assert provider.stop_count == 1
    assert lifecycle.closing
    with pytest.raises(RuntimeError, match="shutting down"):
        await lifecycle.force_flush()


@pytest.mark.asyncio
async def test_shutdown_attempts_all_providers_before_raising() -> None:
    async def fail() -> None:
        raise RuntimeError("vendor detail")

    first = Provider("logs", stop=fail)
    second = Provider("traces")
    lifecycle = ObservabilityLifecycle((first, second), max_concurrency=1)
    with pytest.raises(ObservabilityLifecycleError) as captured:
        await lifecycle.shutdown()
    assert first.stop_count == second.stop_count == 1
    assert captured.value.failures[0].operation is LifecycleOperation.SHUTDOWN
    assert "vendor detail" not in str(captured.value)


@pytest.mark.asyncio
async def test_shutdown_continues_in_background_when_caller_is_cancelled() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    async def slow_shutdown() -> None:
        entered.set()
        await release.wait()

    lifecycle = ObservabilityLifecycle((Provider("traces", stop=slow_shutdown),))
    caller = asyncio.create_task(lifecycle.shutdown())
    await entered.wait()
    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller
    release.set()
    await lifecycle.shutdown()
    assert lifecycle.closing


def test_constructor_rejects_duplicate_names_and_invalid_signals() -> None:
    with pytest.raises(ValueError, match="unique"):
        ObservabilityLifecycle((Provider("metrics"), Provider("metrics")))
    with pytest.raises(ValueError, match="known telemetry signals"):
        ObservabilityLifecycle((Provider("bad", signals=frozenset()),))


@pytest.mark.asyncio
async def test_provider_concurrency_is_bounded() -> None:
    active = 0
    peak = 0

    async def work() -> None:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.005)
        active -= 1

    providers = tuple(Provider(f"metrics-{index}", flush=work) for index in range(5))
    await ObservabilityLifecycle(providers, max_concurrency=2).force_flush()
    assert peak == 2
