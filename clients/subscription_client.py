"""Subscription client: demonstrate event-driven OPC UA updates using the shared adapter."""

import asyncio
import time

from asyncua import Client

from .config import Config
from .contracts import SignalDefinition, SignalObservation, SignalSubscription
from .experiment_logger import ExperimentLogger
from .opcua_subscription_adapter import OpcUaSubscriptionAdapter


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
    """Run subscription experiment using configured transport and signals."""
    config = Config.from_env()

    client = Client(config.transport.endpoint)
    adapter = OpcUaSubscriptionAdapter(
        opcua_client=client,
        root_path=config.transport.root_path,
        publishing_interval_ms=config.experiment.subscription_publishing_interval_ms,
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
        "subscription_client",
        output_dir=config.experiment.results_dir,
    )
    subscription: SignalSubscription | None = None

    async def on_update(observation: SignalObservation) -> None:
        if config.experiment.log_to_console:
            print_observation(observation)
            print("-" * 48)

        logger.log_observation(
            signal=observation.signal,
            value=observation.value,
            seq=observation.seq,
            source_ts=observation.source_ts,
            publish_ts=observation.publish_ts,
            recv_ts=observation.recv_ts if observation.recv_ts is not None else time.time(),
        )

    try:
        async with client:
            subscription = await adapter.subscribe(
                signals=signals,
                on_update=on_update,
            )

            try:
                if config.experiment.log_to_console:
                    print("Subscribed. Waiting for updates...")

                await asyncio.sleep(config.experiment.duration_seconds)
            finally:
                if subscription is not None:
                    await subscription.close()
                    if config.experiment.log_to_console:
                        print("Subscription closed.")
    finally:
        logger.close()


if __name__ == "__main__":
    asyncio.run(main())
