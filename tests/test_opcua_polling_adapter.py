from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

import clients.opcua_polling_adapter as adapter_module
from clients.contracts import SignalDefinition, SignalObservation
from clients.opcua_polling_adapter import ObservationParseError, OpcUaPollingAdapter


def make_signal(logical_name: str = "controller_status", transport_address: str = "0:controller_status") -> SignalDefinition:
    return SignalDefinition(
        logical_name=logical_name,
        transport_address=transport_address,
        data_type="string",
        description="Controller operational status",
    )


class FakeNodeId:
    def __init__(self, value: str) -> None:
        self._value = value

    def to_string(self) -> str:
        return self._value


class FakeNode:
    def __init__(self, node_id: str, read_value_result=None) -> None:
        self.nodeid = FakeNodeId(node_id)
        self.read_value = AsyncMock(return_value=read_value_result)


class FakeRoot:
    def __init__(self, nodes_by_address: dict[str, FakeNode]) -> None:
        self._nodes_by_address = nodes_by_address
        self.get_child_calls: list[list[str] | str] = []

    async def get_child(self, path):
        self.get_child_calls.append(path)
        if isinstance(path, list):
            return self
        return self._nodes_by_address[path]


class FakeClient:
    def __init__(self, root: FakeRoot) -> None:
        self.nodes = SimpleNamespace(root=root)


@pytest.fixture
def adapter_factory():
    def _factory(*, nodes_by_address: dict[str, FakeNode] | None = None):
        nodes_by_address = nodes_by_address or {
            "0:controller_status": FakeNode(
                "ns=1;s=controller_status",
                read_value_result='{"value": "RUNNING", "seq": 1}',
            ),
            "0:communication_status": FakeNode(
                "ns=1;s=communication_status",
                read_value_result='{"value": "ONLINE", "seq": 2}',
            ),
        }
        root = FakeRoot(nodes_by_address)
        client = FakeClient(root=root)
        adapter = OpcUaPollingAdapter(
            opcua_client=client,
            root_path=["0:Objects", "0:simulator"],
        )
        return adapter, client, root, nodes_by_address

    return _factory


def test_parse_payload_accepts_dict(adapter_factory):
    adapter, *_ = adapter_factory()

    payload = {"value": "RUNNING", "seq": 1}

    assert adapter._parse_payload(payload) is payload


def test_parse_payload_accepts_json_object_string(adapter_factory):
    adapter, *_ = adapter_factory()

    payload = adapter._parse_payload('{"value": "RUNNING", "seq": 1}')

    assert payload == {"value": "RUNNING", "seq": 1}


def test_parse_payload_rejects_invalid_json(adapter_factory):
    adapter, *_ = adapter_factory()

    with pytest.raises(ObservationParseError, match="Invalid JSON payload"):
        adapter._parse_payload("not-json")


def test_parse_payload_rejects_non_object_json(adapter_factory):
    adapter, *_ = adapter_factory()

    with pytest.raises(ObservationParseError, match="Expected JSON object payload"):
        adapter._parse_payload('["not", "an", "object"]')


def test_parse_payload_rejects_unsupported_type(adapter_factory):
    adapter, *_ = adapter_factory()

    with pytest.raises(ObservationParseError, match="Unsupported payload type"):
        adapter._parse_payload(123)


def test_to_observation_maps_payload_and_defaults(adapter_factory):
    adapter, *_ = adapter_factory()

    observation = adapter._to_observation(
        make_signal(),
        {
            "value": True,
            "seq": "7",
            "source_ts": "1.5",
            "publish_ts": 2,
            "quality": "uncertain",
        },
        recv_ts=9.5,
    )

    assert observation == SignalObservation(
        signal="controller_status",
        value=True,
        seq=7,
        source_ts=1.5,
        publish_ts=2.0,
        recv_ts=9.5,
        quality="UNCERTAIN",
    )


def test_to_observation_defaults_quality_to_good(adapter_factory):
    adapter, *_ = adapter_factory()

    observation = adapter._to_observation(
        make_signal(),
        {"value": "RUNNING", "seq": 1},
        recv_ts=1.0,
    )

    assert observation.quality == "GOOD"


@pytest.mark.parametrize(
    "payload, message",
    [
        ({"seq": 1}, "Missing required field: value"),
        ({"value": "RUNNING"}, "Missing required field: seq"),
        ({"value": "RUNNING", "seq": "abc"}, "Invalid seq field: abc"),
        ({"value": "RUNNING", "seq": 1, "quality": "BROKEN"}, "Unsupported quality value: BROKEN"),
    ],
)
def test_to_observation_rejects_invalid_payloads(adapter_factory, payload, message):
    adapter, *_ = adapter_factory()

    with pytest.raises(ObservationParseError, match=message):
        adapter._to_observation(make_signal(), payload, recv_ts=1.0)


def test_to_observation_rejects_bad_timestamp(adapter_factory):
    adapter, *_ = adapter_factory()

    with pytest.raises(ValueError):
        adapter._to_observation(
            make_signal(),
            {"value": "RUNNING", "seq": 1, "source_ts": "bad"},
            recv_ts=1.0,
        )


@pytest.mark.asyncio
async def test_get_or_resolve_node_caches_resolved_node(adapter_factory):
    adapter, client, root, _ = adapter_factory()

    first = await adapter._get_or_resolve_node("0:controller_status")
    second = await adapter._get_or_resolve_node("0:controller_status")

    assert first is second
    assert root.get_child_calls == [["0:Objects", "0:simulator"], "0:controller_status"]
    assert first.read_value.await_count == 0
    assert client.nodes.root.get_child_calls == root.get_child_calls


@pytest.mark.asyncio
async def test_read_returns_observation_and_uses_recv_ts(monkeypatch, adapter_factory):
    adapter, _, root, nodes_by_address = adapter_factory()
    signal = make_signal()

    monkeypatch.setattr(adapter_module.time, "time", Mock(return_value=111.222))

    observation = await adapter.read(signal)

    assert observation == SignalObservation(
        signal="controller_status",
        value="RUNNING",
        seq=1,
        source_ts=None,
        publish_ts=None,
        recv_ts=111.222,
        quality="GOOD",
    )
    assert nodes_by_address["0:controller_status"].read_value.await_count == 1
    assert root.get_child_calls == [["0:Objects", "0:simulator"], "0:controller_status"]


@pytest.mark.asyncio
async def test_read_reuses_cached_node(adapter_factory):
    adapter, _, root, nodes_by_address = adapter_factory()
    signal = make_signal()
    node = nodes_by_address["0:controller_status"]
    node.read_value = AsyncMock(
        side_effect=[
            '{"value": "RUNNING", "seq": 1}',
            '{"value": "STOPPED", "seq": 2}',
        ]
    )

    first = await adapter.read(signal)
    second = await adapter.read(signal)

    assert first.seq == 1
    assert second.seq == 2
    assert node.read_value.await_count == 2
    assert root.get_child_calls == [["0:Objects", "0:simulator"], "0:controller_status"]


@pytest.mark.asyncio
async def test_read_rejects_parse_error(monkeypatch, adapter_factory):
    adapter, _, _, nodes_by_address = adapter_factory()
    signal = make_signal()
    nodes_by_address["0:controller_status"].read_value = AsyncMock(return_value="not-json")
    monkeypatch.setattr(adapter_module.time, "time", Mock(return_value=222.333))

    with pytest.raises(ObservationParseError, match="Invalid JSON payload"):
        await adapter.read(signal)


@pytest.mark.asyncio
async def test_read_rejects_bad_timestamp(adapter_factory):
    adapter, _, _, nodes_by_address = adapter_factory()
    signal = make_signal()
    nodes_by_address["0:controller_status"].read_value = AsyncMock(
        return_value='{"value": "RUNNING", "seq": 1, "source_ts": "bad"}'
    )

    with pytest.raises(ValueError):
        await adapter.read(signal)
