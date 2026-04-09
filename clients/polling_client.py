"""Polling client: demonstrate request-response OPC UA reads using the shared adapter."""

import asyncio
import time
from asyncua import Client

from .config import Config
from .contracts import SignalDefinition, SignalObservation
from .experiment_logger import ExperimentLogger
from .opcua_polling_adapter import OpcUaPollingAdapter


def print_observation(obs: SignalObservation) -> None:
    """Print a single observation in readable format."""
    print(f"signal      : {obs.signal}")
    print(f"value       : {obs.value}")
    print(f"seq         : {obs.seq}")
    print(f"quality     : {obs.quality}")
    print(f"source_ts   : {obs.source_ts}")
    print(f"publish_ts  : {obs.publish_ts}")
    print(f"recv_ts     : {obs.recv_ts:.3f}")


async def main() -> None:
    """Run polling experiment using configured transport and signals."""
    config = Config.from_env()

    client = Client(config.transport.endpoint)
    adapter = OpcUaPollingAdapter(
        opcua_client=client,
        root_path=config.transport.root_path,
    )

    signals = [
        SignalDefinition(
            logical_name="controller_status",
            transport_address=config.signals.controller_status,
            data_type="string",
            description="Controller operational status",
        ),
        SignalDefinition(
            logical_name="communication_status",
            transport_address=config.signals.communication_status,
            data_type="string",
            description="Network communication health",
        ),
        SignalDefinition(
            logical_name="alarm_active",
            transport_address=config.signals.alarm_active,
            data_type="bool",
            description="Active alarm flag",
        ),
    ]

    logger = ExperimentLogger(
        "polling_client",
        output_dir=config.experiment.results_dir,
    )
    start_time = time.time()
    poll_count = 0

    try:
        async with client:
            while time.time() - start_time < config.experiment.duration_seconds:
                elapsed = time.time() - start_time

                observations = []
                for signal in signals:
                    try:
                        obs = await adapter.read(signal)
                        observations.append(obs)
                    except Exception as exc:
                        print(f"ERROR reading {signal.logical_name}: {exc}")
                        continue

                if config.experiment.log_to_console:
                    separator = "=" * 48
                    print(f"poll_iteration: {poll_count} (elapsed: {elapsed:.1f}s)")
                    print("-" * 48)

                for obs in observations:
                    if config.experiment.log_to_console:
                        print_observation(obs)
                        print("-" * 48)

                    logger.log_observation(
                        signal=obs.signal,
                        value=obs.value,
                        seq=obs.seq,
                        source_ts=obs.source_ts,
                        publish_ts=obs.publish_ts,
                        recv_ts=obs.recv_ts,
                    )

                if config.experiment.log_to_console:
                    print(separator)

                poll_count += 1

                time_until_next_poll = (
                    config.experiment.poll_interval_seconds - (time.time() - start_time - elapsed)
                )
                if time_until_next_poll > 0:
                    await asyncio.sleep(time_until_next_poll)
    finally:
        logger.close()


if __name__ == "__main__":
    asyncio.run(main())