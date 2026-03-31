from dataclasses import dataclass
from datetime import datetime


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
