import asyncio
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
            cs = await controller_status.read_value()
            comm = await communication_status.read_value()
            alarm = await alarm_active.read_value()

            print(f"[{i}] controller_status={cs}, communication_status={comm}, alarm_active={alarm}")
            await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(main())