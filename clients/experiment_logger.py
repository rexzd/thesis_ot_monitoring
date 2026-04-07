import csv
import json
import os
from typing import Optional, Any
from datetime import datetime


class ExperimentLogger:
    """Simple CSV logger for experiment observations."""

    def __init__(self, experiment_name: str, output_dir: str = "results"):
        """Initialize logger with experiment name and output directory."""
        self.experiment_name = experiment_name
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.csv_file = os.path.join(output_dir, f"{experiment_name}_{timestamp}.csv")
        self.file_handle: Optional[Any] = None
        self.writer: Optional[Any] = None
        self._write_header()

    def _write_header(self) -> None:
        """Write CSV header row if file is new."""
        self.file_handle = open(self.csv_file, "w", newline="")
        self.writer = csv.DictWriter(
            self.file_handle,
            fieldnames=[
                "recv_ts",      # Client-local receive timestamp
                "signal",       # Signal name
                "value",        # Current value
                "seq",          # Sequence number
                "source_ts",    # Server source timestamp
                "publish_ts",   # Server publish timestamp
            ],
        )
        self.writer.writeheader()
        if self.file_handle is not None:
            self.file_handle.flush()

    @staticmethod
    def _serialize_value(value: Any) -> str:
        """Serialize arbitrary observation values for CSV storage."""
        if value is None:
            return ""
        if isinstance(value, (str, int, float, bool)):
            return str(value)
        return json.dumps(value, separators=(",", ":"), sort_keys=True)

    def log_observation(
        self,
        signal: str,
        value: Any,
        seq: int,
        source_ts: float | None,
        publish_ts: float | None,
        recv_ts: float,
    ) -> None:
        """Log a single observation to CSV."""
        if self.writer is None or self.file_handle is None:
            return

        self.writer.writerow(
            {
                "recv_ts": f"{recv_ts:.6f}",
                "signal": signal,
                "value": self._serialize_value(value),
                "seq": seq,
                "source_ts": "" if source_ts is None else f"{source_ts:.6f}",
                "publish_ts": "" if publish_ts is None else f"{publish_ts:.6f}",
            }
        )
        if self.file_handle is not None:
            self.file_handle.flush()

    def close(self) -> None:
        """Close the CSV file."""
        if self.file_handle is not None:
            self.file_handle.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
