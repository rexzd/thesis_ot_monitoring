from dataclasses import dataclass
from datetime import datetime
import json
from typing import ClassVar


@dataclass
class SignalState:
    """
    A signal with metadata.
    
    This is one envelope for a signal value plus context needed for
    fair comparison between polling and subscriptions.
    """
    name: str                  # signal id (e.g., "controller_status")
    value: str | bool | float  # current value (e.g., "RUNNING" or True)
    data_type: str             # "string" | "bool" | "float"
    quality: str               # "GOOD" | "UNCERTAIN" | "BAD"
    source_ts: float           # unix timestamp when value changed at source
    publish_ts: float          # unix timestamp when OPC UA published it
    seq: int                   # per-signal sequence number (1, 2, 3, ...)

    # Class-level tracker: each signal name gets its own sequence counter.
    _seq_by_name: ClassVar[dict[str, int]] = {}

    @classmethod
    def next_seq(cls, name: str) -> int:
        """Return and reserve the next sequence number for a signal name."""
        cls._seq_by_name[name] = cls._seq_by_name.get(name, 0) + 1
        return cls._seq_by_name[name]

    @classmethod
    def reset_sequences(cls) -> None:
        """Reset all sequence counters for deterministic tests."""
        cls._seq_by_name.clear()

    @classmethod
    def create(
        cls,
        name: str,
        value: str | bool | float,
        data_type: str,
        seq: int | None = None,
        quality: str = "GOOD",
    ) -> "SignalState":
        """Create a signal state with source and publish timestamps set to now."""
        now = datetime.now().timestamp()
        if seq is None:
            seq = cls.next_seq(name)
        else:
            # Keep internal counters in sync if a manual sequence is provided.
            current = cls._seq_by_name.get(name, 0)
            if seq > current:
                cls._seq_by_name[name] = seq
        return cls(
            name=name,
            value=value,
            data_type=data_type,
            quality=quality,
            source_ts=now,
            publish_ts=now,
            seq=seq,
        )

    def update_value(
        self,
        value: str | bool | float,
        quality: str | None = None,
    ) -> "SignalState":
        """Return a new state with updated value, fresh timestamps and next sequence."""
        cls = self.__class__
        seq = cls.next_seq(self.name)

        # Ensure monotonic sequence even if current instance came from manual seq.
        if seq <= self.seq:
            seq = self.seq + 1
            cls._seq_by_name[self.name] = seq

        now = datetime.now().timestamp()
        return cls(
            name=self.name,
            value=value,
            data_type=self.data_type,
            quality=self.quality if quality is None else quality,
            source_ts=now,
            publish_ts=now,
            seq=seq,
        )

    def to_dict(self) -> dict[str, str | bool | float | int]:
        """Serialize state to a plain dictionary for transport/storage."""
        return {
            "name": self.name,
            "value": self.value,
            "data_type": self.data_type,
            "quality": self.quality,
            "source_ts": self.source_ts,
            "publish_ts": self.publish_ts,
            "seq": self.seq,
        }

    def to_json(self) -> str:
        """Serialize state to a compact JSON string."""
        return json.dumps(self.to_dict(), separators=(",", ":"))
