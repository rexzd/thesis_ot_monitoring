import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

import clients.opcua_subscription_adapter as adapter_module
from clients.opcua_subscription_adapter import (
    ObservationParseError,
    OpcUaSubscriptionAdapter,
    _DataChangeHandler,
    _OpcUaSubscriptionHandle,
)
from clients.contracts import SignalDefinition, SignalObservation


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
    def __init__(self, node_id: str) -> None:
        self.nodeid = FakeNodeId(node_id)


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
    def __init__(self, root: FakeRoot, subscription) -> None:
        self.nodes = SimpleNamespace(root=root)
        self.create_subscription = AsyncMock(return_value=subscription)


class FakeSubscription:
    def __init__(self) -> None:
        self.subscribe_data_change = AsyncMock(side_effect=[11, 22])
        self.unsubscribe = AsyncMock()
        self.delete = AsyncMock()


@pytest.fixture
def adapter_factory():
    def _factory(*, nodes_by_address: dict[str, FakeNode] | None = None, publishing_interval_ms: int = 500):
        nodes_by_address = nodes_by_address or {
            "0:controller_status": FakeNode("ns=1;s=controller_status"),
            "0:communication_status": FakeNode("ns=1;s=communication_status"),
        }
        subscription = FakeSubscription()
        root = FakeRoot(nodes_by_address)
        client = FakeClient(root=root, subscription=subscription)
        adapter = OpcUaSubscriptionAdapter(
            opcua_client=client,
            root_path=["0:Objects", "0:simulator"],
            publishing_interval_ms=publishing_interval_ms,
        )
        return adapter, client, subscription, root, nodes_by_address

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
    signal = make_signal()

    observation = adapter._to_observation(
        signal=signal,
        payload={
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
    adapter, client, subscription, root, _ = adapter_factory()

    first = await adapter._get_or_resolve_node("0:controller_status")
    second = await adapter._get_or_resolve_node("0:controller_status")

    assert first is second
    assert root.get_child_calls == [["0:Objects", "0:simulator"], "0:controller_status"]
    assert client.create_subscription.await_count == 0
    assert subscription.subscribe_data_change.await_count == 0


@pytest.mark.asyncio
async def test_subscribe_creates_subscription_and_subscribes_all_signals(adapter_factory):
    adapter, client, subscription, root, _ = adapter_factory()
    signals = [
        make_signal("controller_status", "0:controller_status"),
        make_signal("communication_status", "0:communication_status"),
    ]
    on_update = AsyncMock()

    handle = await adapter.subscribe(signals=signals, on_update=on_update)

    assert isinstance(handle, _OpcUaSubscriptionHandle)
    client.create_subscription.assert_awaited_once()
    assert client.create_subscription.await_args.args[0] == 500
    assert isinstance(client.create_subscription.await_args.args[1], _DataChangeHandler)
    assert root.get_child_calls == [
        ["0:Objects", "0:simulator"],
        "0:controller_status",
        ["0:Objects", "0:simulator"],
        "0:communication_status",
    ]
    assert subscription.subscribe_data_change.await_args_list[0].args[0] is root._nodes_by_address["0:controller_status"]
    assert subscription.subscribe_data_change.await_args_list[1].args[0] is root._nodes_by_address["0:communication_status"]


@pytest.mark.asyncio
async def test_subscription_handle_close_is_idempotent():
    subscription = SimpleNamespace(
        unsubscribe=AsyncMock(),
        delete=AsyncMock(),
    )
    handle = _OpcUaSubscriptionHandle(subscription=subscription, handles=[11, 22])

    await handle.close()
    await handle.close()

    subscription.unsubscribe.assert_awaited_once_with([11, 22])
    subscription.delete.assert_awaited_once()


@pytest.mark.asyncio
async def test_datachange_notification_skips_first_notification_then_dispatches(monkeypatch, adapter_factory):
    adapter, _, _, _, _ = adapter_factory()
    signal = make_signal()
    on_update = AsyncMock()
    handler = _DataChangeHandler(
        signal_by_node_id={"ns=1;s=controller_status": signal},
        on_update=on_update,
        parse_payload=adapter._parse_payload,
        to_observation=adapter._to_observation,
    )
    node = FakeNode("ns=1;s=controller_status")
    payload = '{"value": "RUNNING", "seq": 8, "quality": "GOOD"}'

    monkeypatch.setattr(adapter_module.time, "time", Mock(return_value=111.222))

    handler.datachange_notification(node, payload, None)
    await asyncio.sleep(0)
    on_update.assert_not_awaited()

    handler.datachange_notification(node, payload, None)
    await asyncio.sleep(0)

    on_update.assert_awaited_once()
    awaited_call = on_update.await_args_list[0]
    observation = awaited_call.args[0]
    assert observation.signal == "controller_status"
    assert observation.value == "RUNNING"
    assert observation.seq == 8
    assert observation.recv_ts == 111.222


@pytest.mark.asyncio
async def test_datachange_notification_logs_parse_errors_and_skips_update(monkeypatch, adapter_factory):
    adapter, _, _, _, _ = adapter_factory()
    signal = make_signal()
    on_update = AsyncMock()
    handler = _DataChangeHandler(
        signal_by_node_id={"ns=1;s=controller_status": signal},
        on_update=on_update,
        parse_payload=adapter._parse_payload,
        to_observation=adapter._to_observation,
    )
    node = FakeNode("ns=1;s=controller_status")
    print_mock = Mock()

    monkeypatch.setattr(adapter_module.time, "time", Mock(return_value=444.0))
    monkeypatch.setattr("builtins.print", print_mock)

    handler.datachange_notification(node, "not-json", None)
    handler.datachange_notification(node, "not-json", None)
    await asyncio.sleep(0)

    on_update.assert_not_awaited()
    assert any(
        "subscription_parse_error (controller_status)" in str(call.args[0])
        for call in print_mock.call_args_list
    )


@pytest.mark.asyncio
async def test_datachange_notification_logs_handler_errors(monkeypatch, adapter_factory):
    adapter, _, _, _, _ = adapter_factory()
    signal = make_signal()

    async def failing_on_update(_observation):
        raise RuntimeError("boom")

    handler = _DataChangeHandler(
        signal_by_node_id={"ns=1;s=controller_status": signal},
        on_update=failing_on_update,
        parse_payload=adapter._parse_payload,
        to_observation=adapter._to_observation,
    )
    node = FakeNode("ns=1;s=controller_status")
    print_mock = Mock()

    monkeypatch.setattr(adapter_module.time, "time", Mock(return_value=111.0))
    monkeypatch.setattr("builtins.print", print_mock)

    handler.datachange_notification(node, '{"value": "RUNNING", "seq": 1}', None)
    handler.datachange_notification(node, '{"value": "RUNNING", "seq": 1}', None)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert any(
        "subscription_handler_error (controller_status): boom" in str(call.args[0])
        for call in print_mock.call_args_list
    )


def test_unrecognized_node_is_ignored(adapter_factory):
    adapter, _, _, _, _ = adapter_factory()
    signal = make_signal()
    on_update = AsyncMock()
    handler = _DataChangeHandler(
        signal_by_node_id={"ns=1;s=controller_status": signal},
        on_update=on_update,
        parse_payload=adapter._parse_payload,
        to_observation=adapter._to_observation,
    )

    handler.datachange_notification(FakeNode("ns=1;s=other"), '{"value": "RUNNING", "seq": 1}', None)

    on_update.assert_not_awaited()
