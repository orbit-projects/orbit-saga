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
"""Bounded polling recovery worker for persisted incomplete saga instances."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from dataclasses import dataclass

from orbit_saga.coordinator import SagaCoordinator
from orbit_saga.models import SagaDefinition, SagaExecutionError
from orbit_saga.store import SagaConcurrencyError, SagaStore, SagaStoreError

_LOG = logging.getLogger(__name__)
_MAX_DEFINITIONS = 1_000


@dataclass(frozen=True, slots=True)
class SagaRecoveryReport:
    """Counts for one bounded scan; contains no saga input or provider diagnostics."""

    resumed: int
    failed: int
    conflicts: int
    unknown_definitions: int

    def __post_init__(self) -> None:
        """Keep report counters non-negative integers, excluding booleans."""
        for value in (self.resumed, self.failed, self.conflicts, self.unknown_definitions):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError("Saga recovery counts must be non-negative integers.")


class SagaRecoveryWorker:
    """Poll a store page and resume only instances matching registered definition revisions.

    The worker deliberately does not claim rows. Stores may return duplicate candidates to
    different processes, and optimistic compare-and-swap determines which worker advances a
    snapshot. A conflict is an expected race, not permission to run a side effect. Forward and
    compensation operations still require stable idempotency handling for crash windows.
    """

    def __init__(
        self,
        coordinator: SagaCoordinator,
        store: SagaStore,
        definitions: Mapping[tuple[str, str], SagaDefinition],
        *,
        poll_interval_seconds: float = 5.0,
        operation_timeout_seconds: float = 60.0,
        batch_size: int = 100,
    ) -> None:
        """Validate the bounded scan policy and freeze the exact recoverable revisions."""
        if not isinstance(coordinator, SagaCoordinator):
            raise TypeError("coordinator must be a SagaCoordinator.")
        if not isinstance(store, SagaStore):
            raise TypeError("store must implement the Orbit SagaStore contract.")
        if not isinstance(definitions, Mapping) or not 1 <= len(definitions) <= _MAX_DEFINITIONS:
            raise ValueError("definitions must contain between 1 and 1,000 revisions.")
        frozen: dict[tuple[str, str], SagaDefinition] = {}
        for key, definition in definitions.items():
            if (
                not isinstance(key, tuple)
                or len(key) != 2
                or not isinstance(definition, SagaDefinition)
                or key != (definition.name, definition.revision)
            ):
                raise ValueError("Each definition key must match its name and revision.")
            frozen[key] = definition
        if (
            isinstance(poll_interval_seconds, bool)
            or not isinstance(poll_interval_seconds, (int, float))
            or not 0.05 <= poll_interval_seconds <= 3_600
        ):
            raise ValueError("poll_interval_seconds must be between 0.05 and 3,600.")
        if (
            isinstance(operation_timeout_seconds, bool)
            or not isinstance(operation_timeout_seconds, (int, float))
            or not 0.05 <= operation_timeout_seconds <= 3_600
        ):
            raise ValueError("operation_timeout_seconds must be between 0.05 and 3,600.")
        if (
            isinstance(batch_size, bool)
            or not isinstance(batch_size, int)
            or not 1 <= batch_size <= 1_000
        ):
            raise ValueError("batch_size must be between 1 and 1,000.")
        self._coordinator = coordinator
        self._store = store
        self._definitions = frozen
        self._interval = float(poll_interval_seconds)
        self._operation_timeout = float(operation_timeout_seconds)
        self._batch_size = batch_size

    async def run_once(self) -> SagaRecoveryReport:
        """Process one bounded page, continuing after individual action failures or races."""
        snapshots = await self._store.list_incomplete(limit=self._batch_size)
        resumed = failed = conflicts = unknown = 0
        for snapshot in snapshots:
            definition = self._definitions.get((snapshot.definition, snapshot.revision))
            if definition is None:
                unknown += 1
                continue
            try:
                async with asyncio.timeout(self._operation_timeout):
                    await self._coordinator.resume(definition, snapshot.saga_id)
            except SagaConcurrencyError:
                conflicts += 1
            except (SagaExecutionError, TimeoutError):
                # Failed compensation or cooperative timeout leaves resumable durable state.
                failed += 1
            else:
                resumed += 1
        return SagaRecoveryReport(resumed, failed, conflicts, unknown)

    async def run(self, stop: asyncio.Event | None = None) -> None:
        """Poll until stopped; cancellation and store outages preserve bounded shutdown behavior.

        Pass an event when an application needs an explicit stop signal. Otherwise the owning task
        supervisor can stop this coroutine through cancellation during application shutdown.
        """
        if stop is None:
            stop = asyncio.Event()
        if not isinstance(stop, asyncio.Event):
            raise TypeError("stop must be an asyncio.Event.")
        while not stop.is_set():
            try:
                report = await self.run_once()
                if report.failed or report.unknown_definitions:
                    _LOG.warning(
                        "Saga recovery scan needs attention (failed=%d, unknown_revision=%d).",
                        report.failed,
                        report.unknown_definitions,
                    )
            except asyncio.CancelledError:
                raise
            except SagaStoreError:
                # Store errors are sanitized; never log SQL, persisted state, or driver messages.
                _LOG.error("Saga recovery scan failed because the state store is unavailable.")
            try:
                async with asyncio.timeout(self._interval):
                    await stop.wait()
            except TimeoutError:
                continue


__all__ = ["SagaRecoveryReport", "SagaRecoveryWorker"]
