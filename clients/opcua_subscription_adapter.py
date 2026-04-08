"""OPC UA subscription adapter.

This adapter is the transport boundary for subscription reads. It should convert
transport-specific values into the shared SignalObservation contract.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Sequence, cast

from .contracts import (
    SignalUpdateHandler,
    SignalDefinition,
    SignalObservation,
    SignalQuality,
    SignalSubscription,
    SignalSubscriber,
    dispatch_signal_update,
)


class ObservationParseError(ValueError):
    """Raised when a transport payload cannot be mapped to SignalObservation."""


class _OpcUaSubscriptionHandle(SignalSubscription):
    """Concrete handle for an active asyncua subscription."""

    def __init__(self, subscription: Any, handles: list[int]) -> None:
        self._subscription = subscription
        self._handles = handles
        self._closed = False

    async def close(self) -> None:
        """Unsubscribe all nodes and delete the underlying subscription."""
        if self._closed:
            return
        self._closed = True
        await self._subscription.unsubscribe(self._handles)
        await self._subscription.delete()


class _DataChangeHandler:
    """asyncua callback adapter that forwards normalized observations."""

    def __init__(
        self,
        signal_by_node_id: dict[str, SignalDefinition],
        on_update: SignalUpdateHandler,
        parse_payload: Any,
        to_observation: Any,
    ) -> None:
        self._signal_by_node_id = signal_by_node_id
        self._on_update = on_update
        self._parse_payload = parse_payload
        self._to_observation = to_observation
        self._seen_first_notification: set[str] = set()
        self._pending_tasks: set[asyncio.Task[None]] = set()

    def datachange_notification(self, node: Any, val: Any, data: Any) -> None:
        """Handle OPC UA value-change callback.

        asyncua emits an initial snapshot on subscribe; skip the first callback
        per node to align with the previous experiment behavior.
        """
        node_id = node.nodeid.to_string()
        signal = self._signal_by_node_id.get(node_id)
        if signal is None:
            return

        if node_id not in self._seen_first_notification:
            self._seen_first_notification.add(node_id)
            return

        recv_ts = time.time()

        try:
            payload = self._parse_payload(val)
            observation = self._to_observation(signal, payload, recv_ts)
        except Exception as exc:
            print(f"subscription_parse_error ({signal.logical_name}): {exc}")
            return

        task = asyncio.create_task(dispatch_signal_update(self._on_update, observation))
        self._pending_tasks.add(task)

        def _cleanup(completed: asyncio.Task[None]) -> None:
            self._pending_tasks.discard(completed)
            exception = completed.exception()
            if exception is not None:
                print(f"subscription_handler_error ({signal.logical_name}): {exception}")

        task.add_done_callback(_cleanup)

    def event_notification(self, event: Any) -> None:
        return None

    def status_change_notification(self, status: Any) -> None:
        return None


class OpcUaSubscriptionAdapter(SignalSubscriber):
    def __init__(
        self,
        opcua_client: Any,
        root_path: list[str],
        publishing_interval_ms: int,
    ) -> None:
        """Store dependencies only.

        Keep constructor lightweight for testability: no network calls here.
        """
        self._client = opcua_client
        self._root_path = root_path
        self._publishing_interval_ms = publishing_interval_ms
        self._nodes_by_address: dict[str, Any] = {}

    async def subscribe(
        self,
        signals: Sequence[SignalDefinition],
        on_update: SignalUpdateHandler,
    ) -> SignalSubscription:
        """Subscribe to one or more signals and forward normalized observations.

        Returns a subscription handle that the caller must close.
        """
        signal_nodes: dict[SignalDefinition, Any] = {}
        for signal in signals:
            node = await self._get_or_resolve_node(signal.transport_address)
            signal_nodes[signal] = node

        signal_by_node_id = {
            node.nodeid.to_string(): signal for signal, node in signal_nodes.items()
        }
        
        handler = _DataChangeHandler(
            signal_by_node_id=signal_by_node_id,
            on_update=on_update,
            parse_payload=self._parse_payload,
            to_observation=self._to_observation,
        )

        subscription = await self._client.create_subscription(
            self._publishing_interval_ms,
            handler,
        )

        handles: list[int] = []
        for node in signal_nodes.values():
            handle = await subscription.subscribe_data_change(node)
            handles.append(handle)

        return _OpcUaSubscriptionHandle(subscription=subscription, handles=handles)

    async def _get_or_resolve_node(self, transport_address: str) -> Any:
        """Resolve and cache OPC UA node by address.

        Keep addressing rules in one place so swapping transports later does
        not affect client orchestration code.
        """
        if transport_address in self._nodes_by_address:
            return self._nodes_by_address[transport_address]

        root = await self._client.nodes.root.get_child(self._root_path)
        node = await root.get_child(transport_address)
        self._nodes_by_address[transport_address] = node
        return node

    def _parse_payload(self, raw_value: Any) -> dict[str, Any]:
        """Parse raw OPC UA value into a dictionary."""
        if isinstance(raw_value, dict):
            return raw_value

        if isinstance(raw_value, str):
            try:
                parsed = json.loads(raw_value)
            except json.JSONDecodeError as exc:
                raise ObservationParseError("Invalid JSON payload") from exc
            if not isinstance(parsed, dict):
                raise ObservationParseError("Expected JSON object payload")
            return parsed

        raise ObservationParseError(f"Unsupported payload type: {type(raw_value).__name__}")

    def _to_observation(
        self,
        signal: SignalDefinition,
        payload: dict[str, Any],
        recv_ts: float,
    ) -> SignalObservation:
        """Validate expected keys and map payload to the shared contract."""
        if "value" not in payload:
            raise ObservationParseError("Missing required field: value")
        if "seq" not in payload:
            raise ObservationParseError("Missing required field: seq")

        try:
            seq = int(payload["seq"])
        except (ValueError, TypeError) as exc:
            raise ObservationParseError(
                f"Invalid seq field: {payload.get('seq')}"
            ) from exc

        return SignalObservation(
            signal=signal.logical_name,
            value=payload["value"],
            seq=seq,
            source_ts=self._as_optional_float(payload.get("source_ts")),
            publish_ts=self._as_optional_float(payload.get("publish_ts")),
            recv_ts=recv_ts,
            quality=self._parse_quality(payload.get("quality", "GOOD")),
        )

    @staticmethod
    def _as_optional_float(value: Any) -> float | None:
        """Convert optional numeric timestamp fields safely."""
        if value is None or value == "":
            return None
        return float(value)

    @staticmethod
    def _parse_quality(raw_quality: Any) -> SignalQuality:
        """Validate and normalize quality into the shared vocabulary."""
        value = str(raw_quality).upper()
        if value not in {"GOOD", "UNCERTAIN", "BAD"}:
            raise ObservationParseError(f"Unsupported quality value: {raw_quality}")
        return cast(SignalQuality, value)