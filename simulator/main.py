import asyncio
from datetime import datetime
from asyncua import Server
from signal_state import SignalState

async def main():
    # Create an OPC UA server
    server = Server()
    await server.init()

    # Set the endpoint for the server
    server.set_endpoint("opc.tcp://0.0.0.0:4840/800xa/server/")

    # Test create one signal with SignalState
    now = datetime.now().timestamp()
    controller_state = SignalState(
        name="controller_status",
        value="RUNNING",
        data_type="string",
        quality="GOOD",
        source_ts=now,
        publish_ts=now,
        seq=1
    )

    # Create simulator object
    node = server.get_objects_node()
    simulator = await node.add_object(0, "simulator")

    # Add signals to the simulator
    # Test still publish as before but using the SignalState value
    controller_status = await simulator.add_variable(0, "controller_status", controller_state.value)
    communication_status = await simulator.add_variable(0, "communication_status", "OK")
    alarm_active = await simulator.add_variable(0, "alarm_active", False)

    # Allow clients to read and write the variables
    await controller_status.set_writable()
    await communication_status.set_writable()
    await alarm_active.set_writable()

    print("OPC UA Server is running at opc.tcp://0.0.0.0:4840/800xa/server/")

    async with server:
        while True:
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())




