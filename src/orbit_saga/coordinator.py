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
"""Cancellation-aware saga coordinator with durable intent and reverse compensation."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import replace
from uuid import UUID, uuid4

from orbit_saga.models import (
    JSONValue,
    SagaDefinition,
    SagaDefinitionConflict,
    SagaExecutionError,
    SagaIdempotencyConflict,
    SagaSnapshot,
    SagaStatus,
    SagaStepContext,
    _freeze_json_mapping,
    _validate_idempotency_key,
)
from orbit_saga.store import SagaConcurrencyError, SagaStore, SagaStoreError


class SagaCoordinator:
    """Drive one durable saga definition over an explicitly supplied ``SagaStore``.

    The store persists intent before each side effect and optimistic versions prevent concurrent
    writers from silently overwriting each other. Actions use stable idempotency keys so callers
    can safely recover from a crash between a remote side effect and the following state commit.
    """

    def __init__(self, store: SagaStore) -> None:
        """Bind a provider-backed store; the coordinator does not own its lifecycle."""
        if not isinstance(store, SagaStore):
            raise TypeError("store must implement the Orbit SagaStore contract.")
        self._store = store

    async def start(
        self,
        definition: SagaDefinition,
        input: Mapping[str, JSONValue],
        *,
        idempotency_key: str,
    ) -> SagaSnapshot:
        """Start or recover the unique instance for one definition and idempotency key."""
        if not isinstance(definition, SagaDefinition):
            raise TypeError("definition must be a SagaDefinition.")
        _validate_idempotency_key(idempotency_key)
        frozen_input = _freeze_json_mapping(input)
        existing = await self._store.get_by_key(definition.name, idempotency_key)
        if existing is not None:
            if existing.revision != definition.revision:
                raise SagaDefinitionConflict(
                    "Idempotency key belongs to a different saga definition revision."
                )
            if existing.input != frozen_input:
                raise SagaIdempotencyConflict(
                    "Idempotency key was already used with different input."
                )
            return await self._drive(definition, existing)
        snapshot = SagaSnapshot(
            saga_id=uuid4(),
            definition=definition.name,
            revision=definition.revision,
            idempotency_key=idempotency_key,
            status=SagaStatus.PENDING,
            input=frozen_input,
        )
        try:
            await self._store.create(snapshot)
        except SagaStoreError:
            existing = await self._store.get_by_key(definition.name, idempotency_key)
            if existing is None:
                raise
            if existing.revision != definition.revision:
                raise SagaDefinitionConflict(
                    "Idempotency key belongs to a different saga definition revision."
                ) from None
            if existing.input != frozen_input:
                raise SagaIdempotencyConflict(
                    "Idempotency key was already used with different input."
                ) from None
            snapshot = existing
        return await self._drive(definition, snapshot)

    async def resume(self, definition: SagaDefinition, saga_id: UUID) -> SagaSnapshot:
        """Resume a durable incomplete saga or its pending compensation work."""
        if not isinstance(definition, SagaDefinition):
            raise TypeError("definition must be a SagaDefinition.")
        if not isinstance(saga_id, UUID):
            raise TypeError("saga_id must be a UUID.")
        snapshot = await self._store.get(saga_id)
        if snapshot is None:
            raise SagaStoreError("Saga instance does not exist.")
        if snapshot.definition != definition.name or snapshot.revision != definition.revision:
            raise SagaStoreError("Saga definition does not match the persisted instance.")
        return await self._drive(definition, snapshot)

    async def _drive(self, definition: SagaDefinition, snapshot: SagaSnapshot) -> SagaSnapshot:
        if snapshot.status in {SagaStatus.COMPLETED, SagaStatus.FAILED}:
            return snapshot
        if snapshot.status is SagaStatus.COMPENSATION_REQUIRED:
            snapshot = await self._save(snapshot, status=SagaStatus.COMPENSATING)
        if snapshot.status is SagaStatus.COMPENSATING:
            return await self._compensate(definition, snapshot)

        while snapshot.next_step < len(definition.steps):
            step = definition.steps[snapshot.next_step]
            snapshot = await self._save(snapshot, status=SagaStatus.RUNNING)
            context = self._context(snapshot, step.name)
            try:
                output = await step.execute(snapshot.input, context)
                frozen_output = None if output is None else _freeze_json_mapping(output)
            except asyncio.CancelledError:
                raise
            except Exception:
                snapshot = await self._save(snapshot, status=SagaStatus.COMPENSATING)
                snapshot = await self._compensate(definition, snapshot)
                raise SagaExecutionError(snapshot.saga_id, snapshot.status) from None
            outputs = snapshot.outputs + (frozen_output,)
            status = (
                SagaStatus.COMPLETED
                if len(outputs) == len(definition.steps)
                else SagaStatus.RUNNING
            )
            snapshot = await self._save(snapshot, status=status, outputs=outputs)
        return snapshot

    async def _compensate(self, definition: SagaDefinition, snapshot: SagaSnapshot) -> SagaSnapshot:
        while snapshot.compensated_steps < len(snapshot.outputs):
            output_index = len(snapshot.outputs) - snapshot.compensated_steps - 1
            step = definition.steps[output_index]
            context = self._context(snapshot, step.name, compensation=True)
            try:
                await step.compensate(snapshot.input, snapshot.outputs[output_index], context)
            except asyncio.CancelledError:
                raise
            except Exception:
                return await self._save(snapshot, status=SagaStatus.COMPENSATION_REQUIRED)
            snapshot = await self._save(
                snapshot,
                status=SagaStatus.COMPENSATING,
                compensated_steps=snapshot.compensated_steps + 1,
            )
        return await self._save(snapshot, status=SagaStatus.FAILED)

    async def _save(
        self,
        snapshot: SagaSnapshot,
        *,
        status: SagaStatus,
        outputs: tuple[Mapping[str, JSONValue] | None, ...] | None = None,
        compensated_steps: int | None = None,
    ) -> SagaSnapshot:
        updated = replace(
            snapshot,
            version=snapshot.version + 1,
            status=status,
            outputs=snapshot.outputs if outputs is None else outputs,
            compensated_steps=(
                snapshot.compensated_steps if compensated_steps is None else compensated_steps
            ),
        )
        try:
            return await self._store.save(updated, expected_version=snapshot.version)
        except asyncio.CancelledError:
            raise
        except (SagaStoreError, SagaConcurrencyError):
            raise
        except Exception:
            raise SagaStoreError("Saga state could not be committed.") from None

    @staticmethod
    def _context(
        snapshot: SagaSnapshot, step_name: str, *, compensation: bool = False
    ) -> SagaStepContext:
        suffix = ":compensate" if compensation else ""
        return SagaStepContext(
            saga_id=snapshot.saga_id,
            saga_name=snapshot.definition,
            step_name=step_name,
            idempotency_key=f"{snapshot.saga_id}:{step_name}{suffix}",
            compensation=compensation,
        )


__all__ = ["SagaCoordinator"]
