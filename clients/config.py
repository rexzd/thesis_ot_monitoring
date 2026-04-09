"""Configuration loader for OT monitoring experiments.

Reads environment variables from .env and provides typed, validated access to
configuration values. Fails fast with clear error messages if required settings
are missing or invalid.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    raise ImportError(
        "python-dotenv is required. Install with: pip install python-dotenv"
    )


@dataclass(frozen=True)
class TransportConfig:
    """OPC UA connection and transport configuration."""

    endpoint: str
    root_path: list[str]

    @classmethod
    def from_env(cls) -> TransportConfig:
        """Load transport config from environment.

        Required variables:
        - OPCUA_ENDPOINT: OPC UA server URL (e.g., opc.tcp://localhost:4840/...)
        - OPCUA_ROOT_PATH: comma-separated OPC UA path (e.g., 0:Objects,0:simulator)
        """
        endpoint = os.getenv("OPCUA_ENDPOINT")
        if not endpoint:
            raise ValueError(
                "OPCUA_ENDPOINT not set in .env. "
                "See .env.example for required configuration."
            )

        root_path_str = os.getenv("OPCUA_ROOT_PATH")
        if not root_path_str:
            raise ValueError(
                "OPCUA_ROOT_PATH not set in .env. "
                "Expected comma-separated OPC UA path like '0:Objects,0:simulator'."
            )

        root_path = [part.strip() for part in root_path_str.split(",")]
        if not root_path:
            raise ValueError("OPCUA_ROOT_PATH must not be empty.")

        return cls(endpoint=endpoint, root_path=root_path)


@dataclass(frozen=True)
class SignalAddressConfig:
    """Logical signal name to OPC UA address mapping."""

    controller_status: str
    communication_status: str
    alarm_active: str

    @classmethod
    def from_env(cls) -> SignalAddressConfig:
        """Load signal address mappings from environment.

        Required variables:
        - SIGNAL_CONTROLLER_ADDRESS
        - SIGNAL_COMMUNICATION_ADDRESS
        - SIGNAL_ALARM_ADDRESS
        """
        controller_status = os.getenv("SIGNAL_CONTROLLER_ADDRESS")
        communication_status = os.getenv("SIGNAL_COMMUNICATION_ADDRESS")
        alarm_active = os.getenv("SIGNAL_ALARM_ADDRESS")

        if not all([controller_status, communication_status, alarm_active]):
            raise ValueError(
                "All signal address variables must be set in .env: "
                "SIGNAL_CONTROLLER_ADDRESS, SIGNAL_COMMUNICATION_ADDRESS, SIGNAL_ALARM_ADDRESS"
            )

        return cls(
            controller_status=controller_status or "",
            communication_status=communication_status or "",
            alarm_active=alarm_active or "",
        )


@dataclass(frozen=True)
class ExperimentConfig:
    """Experiment runtime parameters."""

    duration_seconds: int
    poll_interval_seconds: float
    subscription_publishing_interval_ms: int
    results_dir: str
    log_to_console: bool

    @classmethod
    def from_env(cls) -> ExperimentConfig:
        """Load experiment configuration from environment."""
        duration_str = os.getenv("EXPERIMENT_DURATION_SECONDS", "15")
        poll_interval_str = os.getenv("POLL_INTERVAL_SECONDS", "2")
        subscription_interval_str = os.getenv("SUBSCRIPTION_PUBLISHING_INTERVAL_MS", "500")
        results_dir = os.getenv("RESULTS_DIR", "results")
        log_to_console_str = os.getenv("LOG_TO_CONSOLE", "1")

        try:
            duration_seconds = int(duration_str)
            poll_interval_seconds = float(poll_interval_str)
            subscription_publishing_interval_ms = int(subscription_interval_str)
        except ValueError as exc:
            raise ValueError(
                "Experiment timing values must be numeric: "
                "EXPERIMENT_DURATION_SECONDS (int), "
                "POLL_INTERVAL_SECONDS (float), "
                "SUBSCRIPTION_PUBLISHING_INTERVAL_MS (int). "
                f"Details: {exc}"
            ) from exc

        if duration_seconds <= 0 or poll_interval_seconds <= 0:
            raise ValueError(
                "EXPERIMENT_DURATION_SECONDS and POLL_INTERVAL_SECONDS must be > 0"
            )

        log_to_console = log_to_console_str.strip().lower() in ("1", "true", "yes")

        return cls(
            duration_seconds=duration_seconds,
            poll_interval_seconds=poll_interval_seconds,
            subscription_publishing_interval_ms=subscription_publishing_interval_ms,
            results_dir=results_dir,
            log_to_console=log_to_console,
        )


@dataclass(frozen=True)
class Config:
    """Complete application configuration."""

    transport: TransportConfig
    signals: SignalAddressConfig
    experiment: ExperimentConfig

    @classmethod
    def from_env(cls, env_path: str | Path | None = None) -> Config:
        """Load all configuration from environment files and variables.

        Args:
            env_path: Optional path to .env file. If not provided, searches for
                      .env in the current directory and parent directories.

        Returns:
            Config: Fully validated configuration object.

        Raises:
            ValueError: If required configuration is missing or invalid.
            FileNotFoundError: If env_path is provided but does not exist.
        """
        env_file = Path(env_path) if env_path else None

        if env_file is None:
            # Search for .env in workspace root and current directory
            env_file = Path(".env")
            if not env_file.exists():
                env_file = Path(__file__).parent.parent / ".env"

        if env_file.exists():
            load_dotenv(env_file, override=False)
        else:
            # If .env not found, load from system environment only
            pass

        transport = TransportConfig.from_env()
        signals = SignalAddressConfig.from_env()
        experiment = ExperimentConfig.from_env()

        return cls(transport=transport, signals=signals, experiment=experiment)
