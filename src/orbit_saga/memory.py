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
"""Explicit process-local saga store for deterministic development and tests."""

from __future__ import annotations

import asyncio
from uuid import UUID

from orbit_saga.models import SagaSnapshot, SagaStatus
from orbit_saga.store import SagaConcurrencyError, SagaStoreError


class InMemorySagaStore:
    """Process-local store; never use it for production or multi-worker execution."""

    def __init__(self, *, max_instances: int = 10_000) -> None:
        """Create a bounded empty store with one lock protecting atomic state transitions."""
        if isinstance(max_instances, bool) or not isinstance(max_instances, int):
            raise TypeError("max_instances must be an integer.")
        if not 1 <= max_instances <= 1_000_000:
            raise ValueError("max_instances must be between 1 and 1,000,000.")
        self._max_instances = max_instances
        self._items: dict[UUID, SagaSnapshot] = {}
        self._keys: dict[tuple[str, str], UUID] = {}
        self._lock = asyncio.Lock()

    async def create(self, snapshot: SagaSnapshot) -> None:
        """Insert one unique instance and unique definition-scoped key atomically."""
        if snapshot.version != 0 or snapshot.status is not SagaStatus.PENDING:
            raise SagaStoreError("New saga instances must start pending at version zero.")
        async with self._lock:
            key = (snapshot.definition, snapshot.idempotency_key)
            if snapshot.saga_id in self._items or key in self._keys:
                raise SagaStoreError("Saga instance or idempotency key already exists.")
            if len(self._items) >= self._max_instances:
                raise SagaStoreError("In-memory saga store capacity is exhausted.")
            self._items[snapshot.saga_id] = snapshot
            self._keys[key] = snapshot.saga_id

    async def get(self, saga_id: UUID) -> SagaSnapshot | None:
        """Return the immutable snapshot for an instance, when present."""
        async with self._lock:
            return self._items.get(saga_id)

    async def get_by_key(self, definition: str, idempotency_key: str) -> SagaSnapshot | None:
        """Return an instance by its unique definition-scoped key, when present."""
        async with self._lock:
            saga_id = self._keys.get((definition, idempotency_key))
            return None if saga_id is None else self._items.get(saga_id)

    async def list_incomplete(self, *, limit: int = 100) -> tuple[SagaSnapshot, ...]:
        """Return a bounded stable page of resumable snapshots for development and tests."""
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1_000:
            raise ValueError("limit must be between 1 and 1,000.")
        async with self._lock:
            candidates = sorted(
                (
                    snapshot
                    for snapshot in self._items.values()
                    if snapshot.status
                    in {
                        SagaStatus.PENDING,
                        SagaStatus.RUNNING,
                        SagaStatus.COMPENSATING,
                        SagaStatus.COMPENSATION_REQUIRED,
                    }
                ),
                key=lambda snapshot: snapshot.saga_id.hex,
            )
            return tuple(candidates[:limit])

    async def save(self, snapshot: SagaSnapshot, *, expected_version: int) -> SagaSnapshot:
        """Compare and swap one instance version while preserving its unique key index."""
        async with self._lock:
            current = self._items.get(snapshot.saga_id)
            if current is None:
                raise SagaStoreError("Saga instance does not exist.")
            if current.version != expected_version or snapshot.version != expected_version + 1:
                raise SagaConcurrencyError("Saga instance version changed concurrently.")
            if (snapshot.definition, snapshot.revision, snapshot.idempotency_key) != (
                current.definition,
                current.revision,
                current.idempotency_key,
            ):
                raise SagaStoreError("Saga identity fields cannot change after creation.")
            self._items[snapshot.saga_id] = snapshot
            return snapshot


__all__ = ["InMemorySagaStore"]
