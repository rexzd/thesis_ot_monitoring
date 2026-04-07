"""Shared contracts for OT signal access and delivery.

These types define the stable boundary between the experiment clients and any
transport-specific implementation, such as the current OPC UA simulator or a
future ABB 800xA connection.

Timestamp semantics:
- source_ts: when the value was generated or last became valid at the source.
- publish_ts: when the transport or server exposed the value to clients.
- recv_ts: when the experiment client observed the value locally.

If a transport cannot provide source_ts or publish_ts reliably, the field may
be left as None and the adapter should document its fallback rule.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Literal, Protocol, Sequence


SignalDataType = Literal["string", "bool", "float", "int", "enum", "json"]
SignalQuality = Literal["GOOD", "UNCERTAIN", "BAD"]


@dataclass(frozen=True, slots=True)
class SignalDefinition:
    """Describe one logical signal independent of transport details.

    data_type should use a small, transport-agnostic vocabulary such as
    "string", "bool", "float", "int", "enum", or "json".
    """

    logical_name: str
    transport_address: str
    data_type: SignalDataType
    description: str = ""


@dataclass(frozen=True, slots=True)
class SignalObservation:
    """Canonical observation format shared across polling and subscriptions.

    The value field is intentionally flexible so transports can carry strings,
    booleans, numbers, None, and structured payloads without forcing early
    flattening.

    seq is a per-signal monotonic counter that starts at 1 for the first known
    observation in a stream and increases by 1 for each new observation of that
    same logical signal. It is not a global event id.

    quality should use a small vocabulary such as "GOOD", "UNCERTAIN", or
    "BAD".
    """

    signal: str
    value: Any
    seq: int
    source_ts: float | None = None
    publish_ts: float | None = None
    recv_ts: float | None = None
    quality: SignalQuality = "GOOD"


SignalUpdateHandler = Callable[[SignalObservation], Awaitable[None] | None]


async def dispatch_signal_update(
    handler: SignalUpdateHandler,
    observation: SignalObservation,
) -> None:
    """Call a sync or async handler and await it only when needed."""

    result = handler(observation)
    if inspect.isawaitable(result):
        await result


class SignalReader(Protocol):
    """Read signal values from a transport-specific source."""

    async def read(self, signal: SignalDefinition) -> SignalObservation:
        """Return the current observation for one signal."""
        ...


class SignalSubscription(Protocol):
    """Represent an active subscription that can be closed cleanly."""

    async def close(self) -> None:
        """Release any transport resources associated with the subscription."""
        ...


class SignalSubscriber(Protocol):
    """Subscribe to updates for one or more logical signals."""

    async def subscribe(
        self,
        signals: Sequence[SignalDefinition],
        on_update: SignalUpdateHandler,
    ) -> SignalSubscription:
        """Start streaming updates for the requested signals."""
        ...