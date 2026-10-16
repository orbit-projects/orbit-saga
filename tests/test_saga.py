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
"""Saga tests verify durable sequencing, idempotency, compensation, and bounded state."""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from orbit_saga import (
    InMemorySagaStore,
    SagaConcurrencyError,
    SagaCoordinator,
    SagaDefinition,
    SagaDefinitionConflict,
    SagaExecutionError,
    SagaIdempotencyConflict,
    SagaRecoveryWorker,
    SagaSnapshot,
    SagaStatus,
    SagaStep,
    SagaStoreError,
)


@pytest.mark.asyncio
async def test_completed_saga_is_idempotent_and_runs_steps_in_order() -> None:
    calls: list[str] = []

    async def reserve(data, context):
        calls.append(f"reserve:{context.idempotency_key}")
        return {"reservation": data["order"]}

    async def release(data, output, context):
        del data, output, context
        calls.append("release")

    async def charge(data, context):
        calls.append(f"charge:{data['order']}:{context.idempotency_key}")
        return {"receipt": "r-1"}

    async def refund(data, output, context):
        del data, output, context
        calls.append("refund")

    definition = SagaDefinition(
        "checkout",
        (
            SagaStep("reserve", reserve, release),
            SagaStep("charge", charge, refund),
        ),
    )
    store = InMemorySagaStore()
    coordinator = SagaCoordinator(store)

    first = await coordinator.start(definition, {"order": "o-42"}, idempotency_key="checkout-42")
    second = await coordinator.start(definition, {"order": "o-42"}, idempotency_key="checkout-42")

    assert first.status is SagaStatus.COMPLETED
    assert second == first
    assert len(calls) == 2
    assert calls[0].startswith(f"reserve:{first.saga_id}:reserve")
    assert calls[1].startswith(f"charge:o-42:{first.saga_id}:charge")
    with pytest.raises(SagaIdempotencyConflict):
        await coordinator.start(definition, {"order": "different"}, idempotency_key="checkout-42")
    changed = SagaDefinition("checkout", definition.steps, revision="2")
    with pytest.raises(SagaDefinitionConflict):
        await coordinator.start(changed, {"order": "o-42"}, idempotency_key="checkout-42")


@pytest.mark.asyncio
async def test_failure_compensates_completed_steps_in_reverse_order() -> None:
    calls: list[str] = []

    async def execute(name, result):
        async def action(_data, _context):
            calls.append(name)
            if isinstance(result, Exception):
                raise result
            return result

        return action

    async def compensate(name):
        async def action(_data, _output, _context):
            calls.append(name)

        return action

    definition = SagaDefinition(
        "order_flow",
        (
            SagaStep(
                "reserve", await execute("reserve", {"id": "r1"}), await compensate("undo-reserve")
            ),
            SagaStep("charge", await execute("charge", {"id": "c1"}), await compensate("refund")),
            SagaStep(
                "notify",
                await execute("notify", RuntimeError("sensitive provider detail")),
                await compensate("undo-notify"),
            ),
        ),
    )
    coordinator = SagaCoordinator(InMemorySagaStore())

    with pytest.raises(SagaExecutionError) as error:
        await coordinator.start(definition, {"order": "o-42"}, idempotency_key="order-42")

    assert error.value.status is SagaStatus.FAILED
    assert calls == ["reserve", "charge", "notify", "refund", "undo-reserve"]
    assert "sensitive provider detail" not in str(error.value)


@pytest.mark.asyncio
async def test_failed_compensation_is_durable_and_resume_retries_with_stable_key() -> None:
    calls: list[str] = []
    compensation_attempts = 0

    async def done(_data, context):
        calls.append(context.idempotency_key)
        return {"token": "t-1"}

    async def undo(_data, _output, context):
        nonlocal compensation_attempts
        compensation_attempts += 1
        calls.append(context.idempotency_key)
        if compensation_attempts == 1:
            raise RuntimeError("private response")

    async def fail(_data, _context):
        raise RuntimeError("failure")

    async def noop(_data, _output, _context):
        return None

    definition = SagaDefinition(
        "recoverable",
        (SagaStep("done", done, undo), SagaStep("fail", fail, noop)),
    )
    coordinator = SagaCoordinator(InMemorySagaStore())
    with pytest.raises(SagaExecutionError) as error:
        await coordinator.start(definition, {"x": 1}, idempotency_key="key-1")
    assert error.value.status is SagaStatus.COMPENSATION_REQUIRED

    final = await coordinator.resume(definition, error.value.saga_id)

    assert final.status is SagaStatus.FAILED
    assert calls[1] == calls[2]


@pytest.mark.asyncio
async def test_cancellation_leaves_recoverable_intent_and_reuses_action_key() -> None:
    attempts: list[str] = []
    fail_once = True

    async def action(_data, context):
        nonlocal fail_once
        attempts.append(context.idempotency_key)
        if fail_once:
            fail_once = False
            raise asyncio.CancelledError
        return {"done": True}

    async def compensate(_data, _output, _context):
        return None

    definition = SagaDefinition("cancel", (SagaStep("work", action, compensate),))
    store = InMemorySagaStore()
    coordinator = SagaCoordinator(store)
    with pytest.raises(asyncio.CancelledError):
        await coordinator.start(definition, {"x": 1}, idempotency_key="cancel-key")
    pending = await store.get_by_key("cancel", "cancel-key")
    assert pending is not None and pending.status is SagaStatus.RUNNING

    completed = await coordinator.resume(definition, pending.saga_id)

    assert completed.status is SagaStatus.COMPLETED
    assert attempts[0] == attempts[1]


@pytest.mark.asyncio
async def test_store_compare_and_swap_rejects_stale_versions() -> None:
    store = InMemorySagaStore()
    snapshot = SagaSnapshot(
        saga_id=uuid4(),
        definition="cas",
        revision="1",
        idempotency_key="key",
        status=SagaStatus.PENDING,
        input={},
    )
    await store.create(snapshot)
    updated = SagaSnapshot(
        saga_id=snapshot.saga_id,
        definition=snapshot.definition,
        revision=snapshot.revision,
        idempotency_key=snapshot.idempotency_key,
        status=SagaStatus.PENDING,
        input=snapshot.input,
        version=1,
    )
    await store.save(updated, expected_version=0)
    with pytest.raises(SagaConcurrencyError):
        await store.save(updated, expected_version=0)


@pytest.mark.asyncio
async def test_state_bounds_and_store_capacity_are_enforced() -> None:
    with pytest.raises(ValueError, match="byte limit"):
        SagaSnapshot(
            saga_id=uuid4(),
            definition="bounds",
            revision="1",
            idempotency_key="key",
            status=SagaStatus.PENDING,
            input={"value": "x" * 1_100_000},
        )
    store = InMemorySagaStore(max_instances=1)
    coordinator = SagaCoordinator(store)

    async def action(_data, _context):
        return {}

    async def compensate(_data, _output, _context):
        return None

    definition = SagaDefinition("capacity", (SagaStep("step", action, compensate),))
    await coordinator.start(definition, {}, idempotency_key="one")
    with pytest.raises(SagaStoreError, match="capacity"):
        await coordinator.start(definition, {}, idempotency_key="two")


def test_snapshot_representation_does_not_reveal_input_or_idempotency_key() -> None:
    snapshot = SagaSnapshot(
        saga_id=uuid4(),
        definition="safe_repr",
        revision="1",
        idempotency_key="customer-identifier-42",
        status=SagaStatus.PENDING,
        input={"access_token": "never-render-this"},
    )

    assert "customer-identifier-42" not in repr(snapshot)
    assert "never-render-this" not in repr(snapshot)


@pytest.mark.asyncio
async def test_recovery_worker_resumes_bounded_incomplete_page() -> None:
    calls: list[str] = []

    async def action(_data, context):
        calls.append(context.idempotency_key)
        return {"done": True}

    async def compensate(_data, _output, _context):
        return None

    definition = SagaDefinition("recovery", (SagaStep("work", action, compensate),), revision="3")
    store = InMemorySagaStore()
    pending = SagaSnapshot(
        saga_id=uuid4(),
        definition=definition.name,
        revision=definition.revision,
        idempotency_key="recovery-key",
        status=SagaStatus.PENDING,
        input={"order": "o-7"},
    )
    await store.create(pending)
    worker = SagaRecoveryWorker(
        SagaCoordinator(store), store, {(definition.name, definition.revision): definition}
    )

    report = await worker.run_once()

    assert report.resumed == 1
    assert report.failed == report.conflicts == report.unknown_definitions == 0
    assert len(calls) == 1
    final = await store.get(pending.saga_id)
    assert final is not None and final.status is SagaStatus.COMPLETED


@pytest.mark.asyncio
async def test_recovery_worker_ignores_unknown_revisions_without_mutating_state() -> None:
    store = InMemorySagaStore()
    pending = SagaSnapshot(
        saga_id=uuid4(),
        definition="missing_definition",
        revision="2",
        idempotency_key="unknown-revision",
        status=SagaStatus.PENDING,
        input={},
    )
    await store.create(pending)
    worker = SagaRecoveryWorker(
        SagaCoordinator(store),
        store,
        {
            ("known", "1"): SagaDefinition(
                "known", (SagaStep("noop", _return_empty, _compensate_empty),)
            )
        },
    )

    report = await worker.run_once()

    assert report.unknown_definitions == 1
    assert await store.get(pending.saga_id) == pending


@pytest.mark.asyncio
async def test_recovery_worker_timeout_preserves_intent_for_a_later_scan() -> None:
    attempts: list[str] = []
    finish = False

    async def action(_data, context):
        nonlocal finish
        attempts.append(context.idempotency_key)
        if not finish:
            await asyncio.Event().wait()
        return {"done": True}

    async def compensate(_data, _output, _context):
        return None

    definition = SagaDefinition("timeout_recovery", (SagaStep("work", action, compensate),))
    store = InMemorySagaStore()
    pending = SagaSnapshot(
        saga_id=uuid4(),
        definition=definition.name,
        revision=definition.revision,
        idempotency_key="timeout-key",
        status=SagaStatus.PENDING,
        input={},
    )
    await store.create(pending)
    worker = SagaRecoveryWorker(
        SagaCoordinator(store),
        store,
        {(definition.name, definition.revision): definition},
        operation_timeout_seconds=0.05,
    )

    timed_out = await worker.run_once()
    after_timeout = await store.get(pending.saga_id)
    assert timed_out.failed == 1
    assert after_timeout is not None and after_timeout.status is SagaStatus.RUNNING

    finish = True
    recovered = await worker.run_once()
    final = await store.get(pending.saga_id)
    assert recovered.resumed == 1
    assert final is not None and final.status is SagaStatus.COMPLETED
    assert attempts[0] == attempts[1]


async def _return_empty(_data, _context):
    return {}


async def _compensate_empty(_data, _output, _context):
    return None
