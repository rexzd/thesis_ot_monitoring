"""OPC UA polling adapter.

This adapter is the transport boundary for polling reads. It should convert
transport-specific values into the shared SignalObservation contract.
"""

from __future__ import annotations

import json
import time
from typing import Any, cast

from .contracts import (
    SignalDefinition,
    SignalObservation,
    SignalQuality,
    SignalReader,
)


class ObservationParseError(ValueError):
    """Raised when a transport payload cannot be mapped to SignalObservation."""


class OpcUaPollingAdapter(SignalReader):
    def __init__(self, opcua_client: Any, root_path: list[str]) -> None:
        """Store dependencies only.

        Keep constructor lightweight for testability: no network calls here.
        root_path must point to an object like ["0:Objects", "0:simulator"].
        Note: Cached nodes are assumed to remain valid for the adapter's lifetime.
        If transport-level invalidation occurs, this cache should be cleared.
        """
        self._client = opcua_client
        self._root_path = root_path
        self._nodes_by_address: dict[str, Any] = {}

    async def read(self, signal: SignalDefinition) -> SignalObservation:
        """Read one signal from OPC UA and return a normalized observation.

        1) Resolve node from signal.transport_address (cache it).
        2) Read raw value from node.
        3) Parse and validate payload.
        4) Set recv_ts locally and return SignalObservation.
        """
        node = await self._get_or_resolve_node(signal.transport_address)
        raw_value = await node.read_value()
        recv_ts = time.time()
        payload = self._parse_payload(raw_value)
        return self._to_observation(signal, payload, recv_ts)

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
        """Parse raw OPC UA value into a dictionary.

        For the simulator, values are JSON strings. If future systems return
        native structures, normalize those here.
        """
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
        """Validate expected keys and map payload to the shared contract.

        source_ts and publish_ts are nullable by contract, but seq and value
        should fail loud if missing because sequence quality depends on them.
        """
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

    