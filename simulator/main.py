import asyncio
from asyncua import Server

async def main():
    # Create an OPC UA server
    server = Server()
    await server.init()

    # Set the endpoint for the server
    server.set_endpoint("opc.tcp://0.0.0.0:4840/800xa/server/")

    # Create simulator object
    node = server.get_objects_node()
    simulator = await node.add_object(0, "simulator")

    # Add signals to the simulator
    controller_status = await simulator.add_variable(0, "controller_status", "RUNNING")
    communication_status = await simulator.add_variable(0, "communication_status", "OK")
    alarm_status = await simulator.add_variable(0, "alarm_status", False)

    # Allow clients to read and write the variables
    await controller_status.set_writable()
    await communication_status.set_writable()
    await alarm_status.set_writable()

    print("OPC UA Server is running at opc.tcp://0.0.0.0:4840/800xa/server/")

    async with server:
        while True:
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())




