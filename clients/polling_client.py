import asyncio
import json
import time
from asyncua import Client

# Import logger - works when run from workspace root
try:
    from clients.experiment_logger import ExperimentLogger
except ImportError:
    from experiment_logger import ExperimentLogger


def print_signal_block(signal_name: str, payload: dict, recv_ts: float) -> None:
    print(f"signal      : {signal_name}")
    print(f"value       : {payload.get('value')}")
    print(f"seq         : {payload.get('seq')}")
    print(f"quality     : {payload.get('quality')}")
    print(f"source_ts   : {payload.get('source_ts')}")
    print(f"publish_ts  : {payload.get('publish_ts')}")
    print(f"recv_ts     : {recv_ts:.3f}")


async def main():
    # Connect to the OPC UA server
    client = Client("opc.tcp://localhost:4840/800xa/server/")

    logger = ExperimentLogger("polling_client")

    async with client:
        # Access the simulator object and its variables
        simulator = await client.nodes.root.get_child(["0:Objects", "0:simulator"])
        controller_status = await simulator.get_child("0:controller_status")
        communication_status = await simulator.get_child("0:communication_status")
        alarm_active = await simulator.get_child("0:alarm_active")

        # Poll the variables every 2 seconds for 15 seconds
        poll_interval = 2
        experiment_duration = 15
        start_time = time.time()
        poll_count = 0

        while time.time() - start_time < experiment_duration:
            recv_ts = time.time()
            elapsed = recv_ts - start_time
            
            cs_raw = await controller_status.read_value()
            comm_raw = await communication_status.read_value()
            alarm_raw = await alarm_active.read_value()

            cs = json.loads(cs_raw)
            comm = json.loads(comm_raw)
            alarm = json.loads(alarm_raw)

            separator = "=" * 48
            print(f"poll_iteration: {poll_count} (elapsed: {elapsed:.1f}s)")
            print(f"poll_recv_ts : {recv_ts:.3f}")
            print("-" * 48)
            print_signal_block("controller_status", cs, recv_ts)
            print("-" * 48)
            print_signal_block("communication_status", comm, recv_ts)
            print("-" * 48)
            print_signal_block("alarm_active", alarm, recv_ts)
            print(separator)

            # Log all three signals
            logger.log_observation(
                signal="controller_status",
                value=cs["value"],
                seq=cs["seq"],
                source_ts=cs["source_ts"],
                publish_ts=cs["publish_ts"],
                recv_ts=recv_ts,
            )
            logger.log_observation(
                signal="communication_status",
                value=comm["value"],
                seq=comm["seq"],
                source_ts=comm["source_ts"],
                publish_ts=comm["publish_ts"],
                recv_ts=recv_ts,
            )
            logger.log_observation(
                signal="alarm_active",
                value=alarm["value"],
                seq=alarm["seq"],
                source_ts=alarm["source_ts"],
                publish_ts=alarm["publish_ts"],
                recv_ts=recv_ts,
            )
            
            poll_count += 1
            
            # Sleep until next poll or end of experiment, whichever comes first
            time_until_next_poll = poll_interval - (time.time() - recv_ts)
            if time_until_next_poll > 0:
                await asyncio.sleep(time_until_next_poll)

    logger.close()

if __name__ == "__main__":
    asyncio.run(main())