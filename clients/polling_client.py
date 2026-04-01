import asyncio
import json
from asyncua import Client

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
            cs_raw = await controller_status.read_value()
            comm_raw = await communication_status.read_value()
            alarm_raw = await alarm_active.read_value()

            cs = json.loads(cs_raw)
            comm = json.loads(comm_raw)
            alarm = json.loads(alarm_raw)

            print(
                f"[{i}] "
                f"controller_status={cs['value']} (seq={cs['seq']}, quality={cs['quality']}), "
                f"communication_status={comm['value']} (seq={comm['seq']}, quality={comm['quality']}), "
                f"alarm_active={alarm['value']} (seq={alarm['seq']}, quality={alarm['quality']})"
            )
            await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(main())