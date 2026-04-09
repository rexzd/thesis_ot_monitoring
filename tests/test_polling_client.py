from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from clients import polling_client
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
            poll_interval_seconds=2,
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
    client_factory = Mock(return_value=fake_client)
    adapter_factory = Mock(return_value=adapter)
    logger_factory = Mock(return_value=logger)

    monkeypatch.setattr(polling_client.Config, "from_env", Mock(return_value=config))
    monkeypatch.setattr(polling_client, "Client", client_factory)
    monkeypatch.setattr(polling_client, "OpcUaPollingAdapter", adapter_factory)
    monkeypatch.setattr(polling_client, "ExperimentLogger", logger_factory)

    return fake_client, logger, client_factory, adapter_factory, logger_factory


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

    polling_client.print_observation(observation)

    output = capsys.readouterr().out
    assert "signal      : controller_status" in output
    assert "value       : RUNNING" in output
    assert "seq         : 12" in output
    assert "quality     : GOOD" in output
    assert "source_ts   : 1.25" in output
    assert "publish_ts  : 2.5" in output
    assert "recv_ts     : 123.457" in output


@pytest.mark.asyncio
async def test_main_reads_logs_and_closes_resources(monkeypatch):
    config = make_config(log_to_console=True)
    observations = [
        SignalObservation(
            signal="controller_status",
            value="RUNNING",
            seq=1,
            source_ts=1.0,
            publish_ts=2.0,
            recv_ts=3.0,
            quality="GOOD",
        ),
        SignalObservation(
            signal="communication_status",
            value="ONLINE",
            seq=2,
            source_ts=1.1,
            publish_ts=2.1,
            recv_ts=3.1,
            quality="GOOD",
        ),
        SignalObservation(
            signal="alarm_active",
            value=False,
            seq=3,
            source_ts=1.2,
            publish_ts=2.2,
            recv_ts=3.2,
            quality="GOOD",
        ),
    ]
    adapter = SimpleNamespace(read=AsyncMock(side_effect=observations))
    fake_client = FakeClient()
    logger = SimpleNamespace(log_observation=Mock(), close=Mock())
    print_observation_mock = Mock()
    print_mock = Mock()
    sleep_mock = AsyncMock()
    time_mock = Mock(side_effect=[0.0, 0.0, 0.0, 0.0, 15.1])

    fake_client, logger, client_factory, adapter_factory, logger_factory = patch_main_dependencies(
        monkeypatch,
        config=config,
        adapter=adapter,
        fake_client=fake_client,
        logger=logger,
    )
    monkeypatch.setattr(polling_client.asyncio, "sleep", sleep_mock)
    monkeypatch.setattr(polling_client.time, "time", time_mock)
    monkeypatch.setattr(polling_client, "print_observation", print_observation_mock)
    monkeypatch.setattr("builtins.print", print_mock)

    await polling_client.main()

    client_factory.assert_called_once_with(config.transport.endpoint)
    adapter_factory.assert_called_once_with(
        opcua_client=fake_client,
        root_path=config.transport.root_path,
    )
    logger_factory.assert_called_once_with("polling_client", output_dir=config.experiment.results_dir)
    assert adapter.read.await_args_list[0].args[0].logical_name == "controller_status"
    assert adapter.read.await_args_list[1].args[0].logical_name == "communication_status"
    assert adapter.read.await_args_list[2].args[0].logical_name == "alarm_active"
    assert sleep_mock.await_count == 1
    sleep_mock.assert_awaited_once_with(2.0)
    assert print_observation_mock.call_count == 3
    assert logger.log_observation.call_count == 3
    logger.log_observation.assert_any_call(
        signal="controller_status",
        value="RUNNING",
        seq=1,
        source_ts=1.0,
        publish_ts=2.0,
        recv_ts=3.0,
    )
    logger.log_observation.assert_any_call(
        signal="communication_status",
        value="ONLINE",
        seq=2,
        source_ts=1.1,
        publish_ts=2.1,
        recv_ts=3.1,
    )
    logger.log_observation.assert_any_call(
        signal="alarm_active",
        value=False,
        seq=3,
        source_ts=1.2,
        publish_ts=2.2,
        recv_ts=3.2,
    )
    logger.close.assert_called_once()
    assert fake_client.entered is True
    assert fake_client.exited is True


@pytest.mark.asyncio
async def test_main_does_not_print_when_console_logging_disabled(monkeypatch):
    config = make_config(log_to_console=False)
    observations = [
        SignalObservation(
            signal="controller_status",
            value="RUNNING",
            seq=1,
            source_ts=1.0,
            publish_ts=2.0,
            recv_ts=3.0,
            quality="GOOD",
        ),
        SignalObservation(
            signal="communication_status",
            value="ONLINE",
            seq=2,
            source_ts=1.1,
            publish_ts=2.1,
            recv_ts=3.1,
            quality="GOOD",
        ),
        SignalObservation(
            signal="alarm_active",
            value=False,
            seq=3,
            source_ts=1.2,
            publish_ts=2.2,
            recv_ts=3.2,
            quality="GOOD",
        ),
    ]
    adapter = SimpleNamespace(read=AsyncMock(side_effect=observations))
    fake_client = FakeClient()
    logger = SimpleNamespace(log_observation=Mock(), close=Mock())
    print_observation_mock = Mock()
    print_mock = Mock()
    sleep_mock = AsyncMock()
    time_mock = Mock(side_effect=[0.0, 0.0, 0.0, 0.0, 15.1])

    fake_client, logger, *_ = patch_main_dependencies(
        monkeypatch,
        config=config,
        adapter=adapter,
        fake_client=fake_client,
        logger=logger,
    )
    monkeypatch.setattr(polling_client.asyncio, "sleep", sleep_mock)
    monkeypatch.setattr(polling_client.time, "time", time_mock)
    monkeypatch.setattr(polling_client, "print_observation", print_observation_mock)
    monkeypatch.setattr("builtins.print", print_mock)

    await polling_client.main()

    print_observation_mock.assert_not_called()
    print_mock.assert_not_called()
    assert logger.log_observation.call_count == 3
    logger.log_observation.assert_any_call(
        signal="controller_status",
        value="RUNNING",
        seq=1,
        source_ts=1.0,
        publish_ts=2.0,
        recv_ts=3.0,
    )
    logger.log_observation.assert_any_call(
        signal="communication_status",
        value="ONLINE",
        seq=2,
        source_ts=1.1,
        publish_ts=2.1,
        recv_ts=3.1,
    )
    logger.log_observation.assert_any_call(
        signal="alarm_active",
        value=False,
        seq=3,
        source_ts=1.2,
        publish_ts=2.2,
        recv_ts=3.2,
    )
    logger.close.assert_called_once()
    sleep_mock.assert_awaited_once_with(2.0)
    assert fake_client.entered is True
    assert fake_client.exited is True


@pytest.mark.asyncio
async def test_main_continues_when_one_read_fails(monkeypatch):
    config = make_config(log_to_console=False)
    observations = [
        SignalObservation(
            signal="controller_status",
            value="RUNNING",
            seq=1,
            source_ts=1.0,
            publish_ts=2.0,
            recv_ts=3.0,
            quality="GOOD",
        ),
        RuntimeError("boom"),
        SignalObservation(
            signal="alarm_active",
            value=False,
            seq=3,
            source_ts=1.2,
            publish_ts=2.2,
            recv_ts=3.2,
            quality="GOOD",
        ),
    ]
    adapter = SimpleNamespace(read=AsyncMock(side_effect=observations))
    fake_client = FakeClient()
    logger = SimpleNamespace(log_observation=Mock(), close=Mock())
    print_mock = Mock()
    sleep_mock = AsyncMock()
    time_mock = Mock(side_effect=[0.0, 0.0, 0.0, 0.0, 15.1])

    fake_client, logger, *_ = patch_main_dependencies(
        monkeypatch,
        config=config,
        adapter=adapter,
        fake_client=fake_client,
        logger=logger,
    )
    monkeypatch.setattr(polling_client.asyncio, "sleep", sleep_mock)
    monkeypatch.setattr(polling_client.time, "time", time_mock)
    monkeypatch.setattr("builtins.print", print_mock)

    await polling_client.main()

    assert adapter.read.await_count == 3
    logger.log_observation.assert_any_call(
        signal="controller_status",
        value="RUNNING",
        seq=1,
        source_ts=1.0,
        publish_ts=2.0,
        recv_ts=3.0,
    )
    logger.log_observation.assert_any_call(
        signal="alarm_active",
        value=False,
        seq=3,
        source_ts=1.2,
        publish_ts=2.2,
        recv_ts=3.2,
    )
    assert logger.log_observation.call_count == 2
    print_mock.assert_any_call("ERROR reading communication_status: boom")
    logger.close.assert_called_once()
    assert fake_client.entered is True
    assert fake_client.exited is True


@pytest.mark.asyncio
async def test_main_closes_logger_when_sleep_fails(monkeypatch):
    config = make_config(log_to_console=False)
    observations = [
        SignalObservation(
            signal="controller_status",
            value="RUNNING",
            seq=1,
            source_ts=1.0,
            publish_ts=2.0,
            recv_ts=3.0,
            quality="GOOD",
        ),
        SignalObservation(
            signal="communication_status",
            value="ONLINE",
            seq=2,
            source_ts=1.1,
            publish_ts=2.1,
            recv_ts=3.1,
            quality="GOOD",
        ),
        SignalObservation(
            signal="alarm_active",
            value=False,
            seq=3,
            source_ts=1.2,
            publish_ts=2.2,
            recv_ts=3.2,
            quality="GOOD",
        ),
    ]
    adapter = SimpleNamespace(read=AsyncMock(side_effect=observations))
    fake_client = FakeClient()
    logger = SimpleNamespace(log_observation=Mock(), close=Mock())
    sleep_mock = AsyncMock(side_effect=RuntimeError("sleep failed"))
    time_mock = Mock(side_effect=[0.0, 0.0, 0.0, 0.0, 15.1])

    fake_client, logger, *_ = patch_main_dependencies(
        monkeypatch,
        config=config,
        adapter=adapter,
        fake_client=fake_client,
        logger=logger,
    )
    monkeypatch.setattr(polling_client.asyncio, "sleep", sleep_mock)
    monkeypatch.setattr(polling_client.time, "time", time_mock)

    with pytest.raises(RuntimeError, match="sleep failed"):
        await polling_client.main()

    logger.close.assert_called_once()
    assert fake_client.entered is True
    assert fake_client.exited is True
