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
"""Provider-neutral persistence contract for durable saga execution snapshots."""

from __future__ import annotations

from typing import Protocol, runtime_checkable
from uuid import UUID

from orbit_saga.models import SagaSnapshot, SagaStatus

_INCOMPLETE_STATUSES = (
    SagaStatus.PENDING,
    SagaStatus.RUNNING,
    SagaStatus.COMPENSATING,
    SagaStatus.COMPENSATION_REQUIRED,
)


class SagaStoreError(RuntimeError):
    """Sanitized failure to load or persist saga state."""


class SagaConcurrencyError(SagaStoreError):
    """Raised when optimistic version checking detects a concurrent state transition."""


@runtime_checkable
class SagaStore(Protocol):
    """Durable store for snapshots and idempotency-key ownership.

    Implementations must make ``create`` unique for ``(definition, idempotency_key)`` and
    ``save`` compare-and-swap on ``expected_version``. Stores must not expose payloads in errors.
    """

    async def create(self, snapshot: SagaSnapshot) -> None:
        """Persist a new version-zero instance or raise ``SagaStoreError`` on conflict."""

    async def get(self, saga_id: UUID) -> SagaSnapshot | None:
        """Load one snapshot by stable instance ID."""

    async def get_by_key(self, definition: str, idempotency_key: str) -> SagaSnapshot | None:
        """Load the instance that owns a definition-scoped idempotency key."""

    async def list_incomplete(self, *, limit: int = 100) -> tuple[SagaSnapshot, ...]:
        """Return a bounded, deterministic page of snapshots that may be resumed.

        Results are candidates rather than exclusive claims. Implementations may return the same
        snapshot to multiple workers; callers must resume through the coordinator and handle
        ``SagaConcurrencyError``. Completed and failed instances must never be returned.
        """

    async def save(self, snapshot: SagaSnapshot, *, expected_version: int) -> SagaSnapshot:
        """Atomically replace a snapshot only when its persisted version still matches."""


__all__ = ["SagaConcurrencyError", "SagaStore", "SagaStoreError"]
