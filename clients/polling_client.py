import asyncio
import json
import time
from asyncua import Client


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

    async with client:
        # Access the simulator object and its variables
        simulator = await client.nodes.root.get_child(["0:Objects", "0:simulator"])
        controller_status = await simulator.get_child("0:controller_status")
        communication_status = await simulator.get_child("0:communication_status")
        alarm_active = await simulator.get_child("0:alarm_active")

        # Poll the variables every 2 seconds for 5 iterations
        for i in range(5):
            recv_ts = time.time()
            cs_raw = await controller_status.read_value()
            comm_raw = await communication_status.read_value()
            alarm_raw = await alarm_active.read_value()

            cs = json.loads(cs_raw)
            comm = json.loads(comm_raw)
            alarm = json.loads(alarm_raw)

            separator = "=" * 48
            print(f"poll_iteration: {i}")
            print(f"poll_recv_ts : {recv_ts:.3f}")
            print("-" * 48)
            print_signal_block("controller_status", cs, recv_ts)
            print("-" * 48)
            print_signal_block("communication_status", comm, recv_ts)
            print("-" * 48)
            print_signal_block("alarm_active", alarm, recv_ts)
            print(separator)
            await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(main())