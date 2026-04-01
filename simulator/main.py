import asyncio
from asyncua import Server
from signal_state import SignalState

async def main():
    # Create an OPC UA server
    server = Server()
    await server.init()

    # Set the endpoint for the server
    server.set_endpoint("opc.tcp://0.0.0.0:4840/800xa/server/")

    # Create initial state for each signal.
    controller_state = SignalState.create(
        name="controller_status",
        value="RUNNING",
        data_type="string",
    )
    communication_state = SignalState.create(
        name="communication_status",
        value="OK",
        data_type="string",
    )
    alarm_state = SignalState.create(
        name="alarm_active",
        value=False,
        data_type="bool",
    )

    # Create simulator object
    node = server.get_objects_node()
    simulator = await node.add_object(0, "simulator")

    # Publish full SignalState envelopes as JSON string payloads.
    controller_status = await simulator.add_variable(0, "controller_status", controller_state.to_json())
    communication_status = await simulator.add_variable(0, "communication_status", communication_state.to_json())
    alarm_active = await simulator.add_variable(0, "alarm_active", alarm_state.to_json())

    # Allow clients to read and write the variables
    await controller_status.set_writable()
    await communication_status.set_writable()
    await alarm_active.set_writable()

    print("OPC UA Server is running at opc.tcp://0.0.0.0:4840/800xa/server/")

    # Deterministic scenario cycles for experiments.
    controller_cycle = ["RUNNING", "RUNNING", "STOPPED", "ERROR"]
    communication_cycle = ["OK", "OK", "DEGRADED", "OK"]
    alarm_cycle = [False, False, True, False]
    tick = 0

    async with server:
        while True:
            controller_state = controller_state.update_value(controller_cycle[tick % len(controller_cycle)])
            communication_state = communication_state.update_value(communication_cycle[tick % len(communication_cycle)])
            alarm_state = alarm_state.update_value(alarm_cycle[tick % len(alarm_cycle)])

            await controller_status.write_value(controller_state.to_json())
            await communication_status.write_value(communication_state.to_json())
            await alarm_active.write_value(alarm_state.to_json())

            tick += 1
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())




