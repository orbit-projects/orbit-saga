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
"""Validated immutable values for saga definitions and persisted execution state."""

from __future__ import annotations

import json
import re
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Any, TypeAlias
from uuid import UUID

_IDENTIFIER = re.compile(r"[a-z][a-z0-9_-]{0,62}\Z")
MAX_STATE_BYTES = 1_048_576
MAX_STEPS = 128
JSONValue: TypeAlias = None | bool | int | float | str | list[Any] | dict[str, Any]


class SagaStatus(StrEnum):
    """Durable lifecycle states for one saga instance."""

    PENDING = "pending"
    RUNNING = "running"
    COMPENSATING = "compensating"
    COMPENSATION_REQUIRED = "compensation_required"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class SagaStepContext:
    """Stable idempotency identity supplied to a forward or compensation operation."""

    saga_id: UUID
    saga_name: str
    step_name: str
    idempotency_key: str
    compensation: bool = False


SagaAction = Callable[
    [Mapping[str, JSONValue], SagaStepContext], Awaitable[Mapping[str, JSONValue] | None]
]
SagaCompensation = Callable[
    [Mapping[str, JSONValue], Mapping[str, JSONValue] | None, SagaStepContext], Awaitable[None]
]


@dataclass(frozen=True, slots=True)
class SagaStep:
    """One asynchronous side effect and its reverse operation.

    Both handlers receive the immutable original input. The forward result is persisted and passed
    to compensation. Handlers must be idempotent for the provided context key because a crash can
    occur after the side effect and before its result is committed to the store.
    """

    name: str
    execute: SagaAction
    compensate: SagaCompensation

    def __post_init__(self) -> None:
        """Reject ambiguous step names and non-callable operations at definition time."""
        if not isinstance(self.name, str) or _IDENTIFIER.fullmatch(self.name) is None:
            raise ValueError("Saga step names must be lowercase portable identifiers.")
        if not callable(self.execute) or not callable(self.compensate):
            raise TypeError("Saga steps require callable execute and compensate handlers.")


@dataclass(frozen=True, slots=True)
class SagaDefinition:
    """Immutable ordered saga definition used to start or resume instances."""

    name: str
    steps: tuple[SagaStep, ...]
    revision: str = "1"

    def __post_init__(self) -> None:
        """Validate bounded step count and unique step identifiers."""
        if not isinstance(self.name, str) or _IDENTIFIER.fullmatch(self.name) is None:
            raise ValueError("Saga names must be lowercase portable identifiers.")
        if (
            not isinstance(self.revision, str)
            or not 1 <= len(self.revision) <= 64
            or any(ord(char) < 33 or ord(char) == 127 for char in self.revision)
        ):
            raise ValueError("Saga revision must be 1 to 64 printable non-space characters.")
        steps = tuple(self.steps)
        if not 1 <= len(steps) <= MAX_STEPS:
            raise ValueError(f"Saga definitions must contain 1 to {MAX_STEPS} steps.")
        if any(not isinstance(step, SagaStep) for step in steps):
            raise TypeError("Saga steps must be SagaStep instances.")
        if len({step.name for step in steps}) != len(steps):
            raise ValueError("Saga step names must be unique within a definition.")
        object.__setattr__(self, "steps", steps)


@dataclass(frozen=True, slots=True)
class SagaSnapshot:
    """Immutable persisted state for one saga execution and its optimistic version."""

    saga_id: UUID
    definition: str
    revision: str
    idempotency_key: str = field(repr=False)
    status: SagaStatus
    input: Mapping[str, JSONValue] = field(repr=False)
    outputs: tuple[Mapping[str, JSONValue] | None, ...] = field(default_factory=tuple, repr=False)
    compensated_steps: int = 0
    version: int = 0

    def __post_init__(self) -> None:
        """Freeze bounded JSON input/output and validate indexes before store persistence."""
        if not isinstance(self.saga_id, UUID):
            raise TypeError("saga_id must be a UUID.")
        if _IDENTIFIER.fullmatch(self.definition) is None:
            raise ValueError("definition must be a lowercase portable identifier.")
        if (
            not isinstance(self.revision, str)
            or not 1 <= len(self.revision) <= 64
            or any(ord(char) < 33 or ord(char) == 127 for char in self.revision)
        ):
            raise ValueError("revision must be 1 to 64 printable non-space characters.")
        _validate_idempotency_key(self.idempotency_key)
        if not isinstance(self.status, SagaStatus):
            raise TypeError("status must be a SagaStatus.")
        outputs = tuple(
            None if output is None else _freeze_json_mapping(output) for output in self.outputs
        )
        if len(outputs) > MAX_STEPS:
            raise ValueError(f"Persisted saga output cannot exceed {MAX_STEPS} steps.")
        if (
            isinstance(self.version, bool)
            or not isinstance(self.version, int)
            or not 0 <= self.version <= 9_223_372_036_854_775_807
        ):
            raise ValueError("version must fit a non-negative signed 64-bit integer.")
        if (
            isinstance(self.compensated_steps, bool)
            or not isinstance(self.compensated_steps, int)
            or not 0 <= self.compensated_steps <= len(outputs)
        ):
            raise ValueError("compensated_steps is outside the completed step range.")
        frozen_input = _freeze_json_mapping(self.input)
        _encode_state(frozen_input, outputs)
        object.__setattr__(self, "input", frozen_input)
        object.__setattr__(self, "outputs", outputs)

    @property
    def next_step(self) -> int:
        """Return the next forward step index, based on durably stored outputs."""
        return len(self.outputs)

    def to_state_json(self) -> str:
        """Serialize payload state without including the store version or secret values in logs."""
        value = {
            "input": _thaw_json(self.input),
            "outputs": [None if output is None else _thaw_json(output) for output in self.outputs],
            "compensated_steps": self.compensated_steps,
        }
        return _encode_state(value["input"], tuple(value["outputs"]), self.compensated_steps)

    @classmethod
    def from_state_json(
        cls,
        *,
        saga_id: UUID,
        definition: str,
        revision: str,
        idempotency_key: str,
        status: SagaStatus,
        state_json: str,
        version: int,
    ) -> SagaSnapshot:
        """Parse and validate bounded durable JSON returned by a trusted SagaStore adapter."""
        if not isinstance(state_json, str) or len(state_json.encode("utf-8")) > MAX_STATE_BYTES:
            raise ValueError("Persisted saga state exceeds its configured byte limit.")
        try:
            value = json.loads(
                state_json,
                object_pairs_hook=_unique_object,
                parse_constant=_reject_constant,
            )
            if not isinstance(value, dict) or set(value) != {
                "input",
                "outputs",
                "compensated_steps",
            }:
                raise ValueError
            if not isinstance(value["input"], dict) or not isinstance(value["outputs"], list):
                raise ValueError
            return cls(
                saga_id=saga_id,
                definition=definition,
                revision=revision,
                idempotency_key=idempotency_key,
                status=status,
                input=value["input"],
                outputs=tuple(value["outputs"]),
                compensated_steps=value["compensated_steps"],
                version=version,
            )
        except Exception:
            raise ValueError("Persisted saga state is invalid.") from None


class SagaError(RuntimeError):
    """Base class for safe saga orchestration failures."""


class SagaExecutionError(SagaError):
    """A saga step failed or compensation needs operator attention."""

    def __init__(self, saga_id: UUID, status: SagaStatus) -> None:
        """Expose the saga identity and durable status without copying handler exceptions."""
        self.saga_id = saga_id
        self.status = status
        super().__init__(f"Saga {saga_id} execution ended in state {status.value}.")


class SagaIdempotencyConflict(SagaError):
    """Raised when one idempotency key is reused with different input."""


class SagaDefinitionConflict(SagaError):
    """Raised when an existing idempotent instance uses a different definition revision."""


def _validate_idempotency_key(value: object) -> None:
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= 255
        or any(ord(char) < 33 or ord(char) == 127 for char in value)
    ):
        raise ValueError("idempotency_key must be 1 to 255 printable non-space characters.")


def _freeze_json_mapping(value: object) -> Mapping[str, JSONValue]:
    if not isinstance(value, Mapping):
        raise TypeError("Saga input and step outputs must be JSON objects.")
    frozen = _freeze_json(value, depth=0, budget=[0])
    if not isinstance(frozen, Mapping):
        raise TypeError("Saga input and step outputs must be JSON objects.")
    _encode_state(frozen, ())
    return frozen


def _freeze_json(value: object, *, depth: int, budget: list[int]) -> Any:
    if depth > 64:
        raise ValueError("Saga state is nested too deeply.")
    budget[0] += 1
    if budget[0] > 100_000:
        raise ValueError("Saga state contains too many values.")
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if (
                not isinstance(key, str)
                or not 1 <= len(key) <= 255
                or any(ord(char) < 32 or ord(char) == 127 for char in key)
            ):
                raise ValueError("Saga state contains an invalid object key.")
            result[key] = _freeze_json(item, depth=depth + 1, budget=budget)
        return MappingProxyType(result)
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json(item, depth=depth + 1, budget=budget) for item in value)
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    raise TypeError("Saga state must contain JSON-compatible values only.")


def _thaw_json(value: object) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


def _encode_state(
    input_value: object,
    outputs: tuple[object, ...],
    compensated_steps: int = 0,
) -> str:
    try:
        serialized = json.dumps(
            {
                "input": _thaw_json(input_value),
                "outputs": [None if output is None else _thaw_json(output) for output in outputs],
                "compensated_steps": compensated_steps,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError, UnicodeError):
        raise ValueError("Saga state cannot be serialized as finite JSON.") from None
    if len(serialized.encode("utf-8")) > MAX_STATE_BYTES:
        raise ValueError("Saga state exceeds the configured byte limit.")
    return serialized


def _reject_constant(_value: str) -> None:
    raise ValueError


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


__all__ = [
    "JSONValue",
    "SagaDefinition",
    "SagaError",
    "SagaExecutionError",
    "SagaIdempotencyConflict",
    "SagaSnapshot",
    "SagaStatus",
    "SagaStep",
    "SagaStepContext",
]
