import asyncio
import json
import time

from asyncua import Client

# Import logger - works when run from workspace root
try:
    from clients.experiment_logger import ExperimentLogger
except ImportError:
    from experiment_logger import ExperimentLogger


class SignalSubscriptionHandler:
	def __init__(self, node_name_by_id: dict[str, str], logger: ExperimentLogger):
		self.node_name_by_id = node_name_by_id
		self.logger = logger
		self._seen_first_notification: set[str] = set()

	def _print_signal_block(self, header: str, signal_name: str, payload: dict, recv_ts: float):
		separator = "=" * 48
		print(header)
		print(f"signal      : {signal_name}")
		print(f"value       : {payload.get('value')}")
		print(f"seq         : {payload.get('seq')}")
		print(f"quality     : {payload.get('quality')}")
		print(f"source_ts   : {payload.get('source_ts')}")
		print(f"publish_ts  : {payload.get('publish_ts')}")
		print(f"recv_ts     : {recv_ts:.3f}")
		print(separator)

	def datachange_notification(self, node, val, data):
		recv_ts = time.time()
		node_id = node.nodeid.to_string()
		signal_name = self.node_name_by_id.get(node_id, node_id) or "unknown_signal"

		# OPC UA sends an initial snapshot on subscribe; skip it per node.
		if node_id not in self._seen_first_notification:
			self._seen_first_notification.add(node_id)
			return

		try:
			payload = json.loads(val) if isinstance(val, str) else val
		except json.JSONDecodeError:
			separator = "=" * 48
			print("subscription_event")
			print(f"signal      : {signal_name}")
			print(f"recv_ts     : {recv_ts:.3f}")
			print(f"raw         : {val}")
			print(separator)
			return

		self._print_signal_block("subscription_event", signal_name, payload, recv_ts)
		
		# Log to CSV
		self.logger.log_observation(
			signal=signal_name,
			value=payload.get('value'),
			seq=payload.get('seq'),
			source_ts=payload.get('source_ts'),
			publish_ts=payload.get('publish_ts'),
			recv_ts=recv_ts,
		)

	def event_notification(self, event):
		return None

	def status_change_notification(self, status):
		return None


async def main():
	client = Client("opc.tcp://localhost:4840/800xa/server/")

	async with client:
		simulator = await client.nodes.root.get_child(["0:Objects", "0:simulator"])

		signal_nodes = {
			"controller_status": await simulator.get_child("0:controller_status"),
			"communication_status": await simulator.get_child("0:communication_status"),
			"alarm_active": await simulator.get_child("0:alarm_active"),
		}
		node_name_by_id = {
			node.nodeid.to_string(): name for name, node in signal_nodes.items()
		}

		logger = ExperimentLogger("subscription_client")
		handler = SignalSubscriptionHandler(node_name_by_id, logger)
		subscription = await client.create_subscription(500, handler)
		print("Subscribed to signal updates for 15 seconds...")
		handles = []
		for node in signal_nodes.values():
			handle = await subscription.subscribe_data_change(node)
			handles.append(handle)
		try:
			await asyncio.sleep(15)
		finally:
			await subscription.unsubscribe(handles)
			await subscription.delete()
			logger.close()


if __name__ == "__main__":
	asyncio.run(main())
