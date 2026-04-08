from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from clients import subscription_client
from clients.contracts import SignalObservation


def make_config(*, log_to_console: bool = True) -> SimpleNamespace:
    return SimpleNamespace(
        transport=SimpleNamespace(
            endpoint="opc.tcp://example:4840/server/",
            root_path=["0:Objects", "0:simulator"],
        ),
        signals=SimpleNamespace(
            controller_status="0:controller_status",
            communication_status="0:communication_status",
            alarm_active="0:alarm_active",
        ),
        experiment=SimpleNamespace(
            duration_seconds=15,
            subscription_publishing_interval_ms=750,
            results_dir="test-results",
            log_to_console=log_to_console,
        ),
    )


class FakeClient:
    def __init__(self) -> None:
        self.entered = False
        self.exited = False

    async def __aenter__(self):
        self.entered = True
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.exited = True
        return False


def patch_main_dependencies(monkeypatch, *, config, adapter, fake_client=None, logger=None):
    fake_client = fake_client or FakeClient()
    logger = logger or SimpleNamespace(log_observation=Mock(), close=Mock())

    monkeypatch.setattr(subscription_client.Config, "from_env", Mock(return_value=config))
    monkeypatch.setattr(subscription_client, "Client", Mock(return_value=fake_client))
    monkeypatch.setattr(subscription_client, "OpcUaSubscriptionAdapter", Mock(return_value=adapter))
    monkeypatch.setattr(subscription_client, "ExperimentLogger", Mock(return_value=logger))

    return fake_client, logger


@pytest.mark.asyncio
async def test_print_observation_formats_expected_fields(capsys):
    observation = SignalObservation(
        signal="controller_status",
        value="RUNNING",
        seq=12,
        source_ts=1.25,
        publish_ts=2.5,
        recv_ts=123.4567,
        quality="GOOD",
    )

    subscription_client.print_observation(observation)

    output = capsys.readouterr().out
    assert "signal      : controller_status" in output
    assert "value       : RUNNING" in output
    assert "seq         : 12" in output
    assert "quality     : GOOD" in output
    assert "source_ts   : 1.25" in output
    assert "publish_ts  : 2.5" in output
    assert "recv_ts     : 123.457" in output


@pytest.mark.asyncio
async def test_main_subscribes_logs_update_and_closes_resources(monkeypatch):
    config = make_config(log_to_console=True)
    fake_subscription = SimpleNamespace(close=AsyncMock())
    logger = SimpleNamespace(log_observation=Mock(), close=Mock())
    print_mock = Mock()

    async def subscribe_side_effect(*, signals, on_update):
        assert [signal.logical_name for signal in signals] == [
            "controller_status",
            "communication_status",
            "alarm_active",
        ]

        await on_update(
            SignalObservation(
                signal="controller_status",
                value="RUNNING",
                seq=7,
                source_ts=1.2,
                publish_ts=2.3,
                recv_ts=88.123,
                quality="GOOD",
            )
        )
        return fake_subscription

    adapter = SimpleNamespace(subscribe=AsyncMock(side_effect=subscribe_side_effect))
    sleep_mock = AsyncMock()
    time_mock = Mock(return_value=123.456)

    fake_client, logger = patch_main_dependencies(
        monkeypatch,
        config=config,
        adapter=adapter,
        logger=logger,
    )
    monkeypatch.setattr(subscription_client.asyncio, "sleep", sleep_mock)
    monkeypatch.setattr(subscription_client.time, "time", time_mock)
    monkeypatch.setattr(subscription_client, "print_observation", print_mock)

    await subscription_client.main()

    adapter.subscribe.assert_awaited_once()
    assert adapter.subscribe.await_args.kwargs["signals"][0].logical_name == "controller_status"
    assert adapter.subscribe.await_args.kwargs["signals"][1].logical_name == "communication_status"
    assert adapter.subscribe.await_args.kwargs["signals"][2].logical_name == "alarm_active"
    assert callable(adapter.subscribe.await_args.kwargs["on_update"])
    sleep_mock.assert_awaited_once_with(15)
    fake_subscription.close.assert_awaited_once()
    print_mock.assert_called_once()
    logger.log_observation.assert_called_once_with(
        signal="controller_status",
        value="RUNNING",
        seq=7,
        source_ts=1.2,
        publish_ts=2.3,
        recv_ts=88.123,
    )
    logger.close.assert_called_once()
    assert fake_client.entered is True
    assert fake_client.exited is True


@pytest.mark.asyncio
async def test_main_uses_current_time_when_recv_ts_missing(monkeypatch):
    config = make_config(log_to_console=False)
    fake_subscription = SimpleNamespace(close=AsyncMock())
    logger = SimpleNamespace(log_observation=Mock(), close=Mock())

    async def subscribe_side_effect(*, signals, on_update):
        await on_update(
            SignalObservation(
                signal="alarm_active",
                value=True,
                seq=1,
                source_ts=None,
                publish_ts=None,
                recv_ts=None,
                quality="GOOD",
            )
        )
        return fake_subscription

    adapter = SimpleNamespace(subscribe=AsyncMock(side_effect=subscribe_side_effect))
    sleep_mock = AsyncMock()
    time_mock = Mock(return_value=222.333)

    fake_client, logger = patch_main_dependencies(
        monkeypatch,
        config=config,
        adapter=adapter,
        logger=logger,
    )
    monkeypatch.setattr(subscription_client.asyncio, "sleep", sleep_mock)
    monkeypatch.setattr(subscription_client.time, "time", time_mock)

    await subscription_client.main()

    sleep_mock.assert_awaited_once_with(15)
    logger.log_observation.assert_called_once_with(
        signal="alarm_active",
        value=True,
        seq=1,
        source_ts=None,
        publish_ts=None,
        recv_ts=222.333,
    )
    fake_subscription.close.assert_awaited_once()
    logger.close.assert_called_once()


@pytest.mark.asyncio
async def test_main_does_not_print_when_console_logging_disabled(monkeypatch):
    config = make_config(log_to_console=False)
    fake_subscription = SimpleNamespace(close=AsyncMock())
    logger = SimpleNamespace(log_observation=Mock(), close=Mock())
    print_mock = Mock()

    async def subscribe_side_effect(*, signals, on_update):
        await on_update(
            SignalObservation(
                signal="controller_status",
                value="RUNNING",
                seq=1,
                source_ts=1.0,
                publish_ts=2.0,
                recv_ts=3.0,
                quality="GOOD",
            )
        )
        return fake_subscription

    adapter = SimpleNamespace(subscribe=AsyncMock(side_effect=subscribe_side_effect))
    sleep_mock = AsyncMock()

    fake_client, logger = patch_main_dependencies(
        monkeypatch,
        config=config,
        adapter=adapter,
        logger=logger,
    )
    monkeypatch.setattr(subscription_client.asyncio, "sleep", sleep_mock)
    monkeypatch.setattr(subscription_client, "print_observation", print_mock)

    await subscription_client.main()

    print_mock.assert_not_called()
    logger.log_observation.assert_called_once_with(
        signal="controller_status",
        value="RUNNING",
        seq=1,
        source_ts=1.0,
        publish_ts=2.0,
        recv_ts=3.0,
    )
    fake_subscription.close.assert_awaited_once()
    logger.close.assert_called_once()
    sleep_mock.assert_awaited_once_with(15)
    assert fake_client.entered is True
    assert fake_client.exited is True


@pytest.mark.asyncio
async def test_main_prints_when_console_logging_enabled(monkeypatch):
    config = make_config(log_to_console=True)
    fake_subscription = SimpleNamespace(close=AsyncMock())
    logger = SimpleNamespace(log_observation=Mock(), close=Mock())
    print_mock = Mock()

    observation = SignalObservation(
        signal="controller_status",
        value="RUNNING",
        seq=1,
        source_ts=1.0,
        publish_ts=2.0,
        recv_ts=3.0,
        quality="GOOD",
    )

    async def subscribe_side_effect(*, signals, on_update):
        await on_update(observation)
        return fake_subscription

    adapter = SimpleNamespace(subscribe=AsyncMock(side_effect=subscribe_side_effect))
    sleep_mock = AsyncMock()

    fake_client, logger = patch_main_dependencies(
        monkeypatch,
        config=config,
        adapter=adapter,
        logger=logger,
    )
    monkeypatch.setattr(subscription_client.asyncio, "sleep", sleep_mock)
    monkeypatch.setattr(subscription_client, "print_observation", print_mock)

    await subscription_client.main()

    print_mock.assert_called_once_with(observation)
    logger.log_observation.assert_called_once_with(
        signal="controller_status",
        value="RUNNING",
        seq=1,
        source_ts=1.0,
        publish_ts=2.0,
        recv_ts=3.0,
    )
    fake_subscription.close.assert_awaited_once()
    logger.close.assert_called_once()
    sleep_mock.assert_awaited_once_with(15)
    assert fake_client.entered is True
    assert fake_client.exited is True


@pytest.mark.asyncio
async def test_main_closes_subscription_when_sleep_fails(monkeypatch):
    config = make_config(log_to_console=False)
    fake_subscription = SimpleNamespace(close=AsyncMock())
    logger = SimpleNamespace(log_observation=Mock(), close=Mock())
    adapter = SimpleNamespace(subscribe=AsyncMock(return_value=fake_subscription))

    fake_client, logger = patch_main_dependencies(
        monkeypatch,
        config=config,
        adapter=adapter,
        logger=logger,
    )
    monkeypatch.setattr(subscription_client.asyncio, "sleep", AsyncMock(side_effect=RuntimeError("sleep failed")))

    with pytest.raises(RuntimeError, match="sleep failed"):
        await subscription_client.main()

    fake_subscription.close.assert_awaited_once()
    logger.close.assert_called_once()
    assert fake_client.entered is True
    assert fake_client.exited is True


@pytest.mark.asyncio
async def test_main_closes_logger_when_subscription_close_fails(monkeypatch):
    config = make_config(log_to_console=False)
    fake_subscription = SimpleNamespace(close=AsyncMock(side_effect=RuntimeError("close failed")))
    logger = SimpleNamespace(log_observation=Mock(), close=Mock())
    adapter = SimpleNamespace(subscribe=AsyncMock(return_value=fake_subscription))

    fake_client, logger = patch_main_dependencies(
        monkeypatch,
        config=config,
        adapter=adapter,
        logger=logger,
    )
    monkeypatch.setattr(subscription_client.asyncio, "sleep", AsyncMock())

    with pytest.raises(RuntimeError, match="close failed"):
        await subscription_client.main()

    fake_subscription.close.assert_awaited_once()
    logger.close.assert_called_once()
    assert fake_client.entered is True
    assert fake_client.exited is True


@pytest.mark.asyncio
async def test_main_closes_logger_when_subscription_setup_fails(monkeypatch):
    config = make_config(log_to_console=False)
    logger = SimpleNamespace(log_observation=Mock(), close=Mock())
    adapter = SimpleNamespace(subscribe=AsyncMock(side_effect=RuntimeError("subscribe failed")))

    fake_client, logger = patch_main_dependencies(
        monkeypatch,
        config=config,
        adapter=adapter,
        logger=logger,
    )
    monkeypatch.setattr(subscription_client.asyncio, "sleep", AsyncMock())

    with pytest.raises(RuntimeError, match="subscribe failed"):
        await subscription_client.main()

    logger.close.assert_called_once()
    assert fake_client.entered is True
    assert fake_client.exited is True
