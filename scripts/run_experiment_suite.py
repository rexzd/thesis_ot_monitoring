#!/usr/bin/env python3
"""Run reproducible polling vs subscription experiment suites.

This script orchestrates:
- OPC UA simulator startup
- Polling + subscription client runs per scenario/repetition
- Analysis execution for each paired run
- Run-level and suite-level metadata/summaries for reproducibility

Usage:
    python scripts/run_experiment_suite.py \
        --suite scripts/suites/thesis_baseline.json \
        --results-root results \
        --seed 42
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import shutil
import socket
import subprocess
import sys
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import psutil  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover - exercised only when dependency missing
    psutil = None

if psutil is not None:
    PSUTIL_PROCESS_EXCEPTIONS = (
        psutil.NoSuchProcess,
        psutil.AccessDenied,
        psutil.ZombieProcess,
    )
else:
    PSUTIL_PROCESS_EXCEPTIONS = (Exception,)


DEFAULT_TELEMETRY_INTERVAL_SECONDS = 1.0
SUITE_TELEMETRY_INTERVAL_ENV_KEY = "SUITE_TELEMETRY_INTERVAL_SECONDS"


@dataclass(frozen=True)
class Scenario:
    name: str
    repetitions: int
    duration_seconds: int
    poll_interval_seconds: float
    subscription_publishing_interval_ms: int
    simulator_update_interval_seconds: float
    warmup_seconds: float
    mode_order: str


class SuiteError(RuntimeError):
    pass


@dataclass(frozen=True)
class TelemetrySample:
    ts_unix: float
    cpu_percent: float
    rss_mb: float
    vms_mb: float


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_psutil_available() -> None:
    if psutil is None:
        raise SuiteError(
            "psutil is required for suite telemetry. Install dependencies with: "
            "pip install -r requirements.txt"
        )


def _telemetry_interval_seconds() -> float:
    raw = os.getenv(
        SUITE_TELEMETRY_INTERVAL_ENV_KEY,
        str(DEFAULT_TELEMETRY_INTERVAL_SECONDS),
    )
    try:
        interval = float(raw)
    except ValueError as exc:
        raise SuiteError(
            f"{SUITE_TELEMETRY_INTERVAL_ENV_KEY} must be a float > 0, got: {raw}"
        ) from exc

    if interval <= 0:
        raise SuiteError(
            f"{SUITE_TELEMETRY_INTERVAL_ENV_KEY} must be > 0, got: {interval}"
        )
    return interval


def load_suite_config(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if "suite_name" not in data or not isinstance(data["suite_name"], str):
        raise SuiteError("suite_name is required and must be a string")
    if "scenarios" not in data or not isinstance(data["scenarios"], list):
        raise SuiteError("scenarios is required and must be a list")

    return data


def parse_scenarios(raw_scenarios: list[dict[str, Any]]) -> list[Scenario]:
    scenarios: list[Scenario] = []
    for raw in raw_scenarios:
        scenario = Scenario(
            name=str(raw["name"]),
            repetitions=int(raw.get("repetitions", 1)),
            duration_seconds=int(raw.get("duration_seconds", 120)),
            poll_interval_seconds=float(raw.get("poll_interval_seconds", 1.0)),
            subscription_publishing_interval_ms=int(
                raw.get("subscription_publishing_interval_ms", 250)
            ),
            simulator_update_interval_seconds=float(
                raw.get("simulator_update_interval_seconds", 1.0)
            ),
            warmup_seconds=float(raw.get("warmup_seconds", 2.0)),
            mode_order=str(raw.get("mode_order", "fixed")),
        )
        if scenario.poll_interval_seconds <= 0:
            raise SuiteError(
                f"scenario '{scenario.name}' has invalid poll_interval_seconds="
                f"{scenario.poll_interval_seconds} (must be > 0)"
            )
        if scenario.mode_order not in {"fixed", "alternating", "random"}:
            raise SuiteError(
                f"scenario '{scenario.name}' has invalid mode_order={scenario.mode_order} "
                "(expected fixed|alternating|random)"
            )
        scenarios.append(scenario)
    return scenarios


def wait_for_port(host: str, port: int, timeout_seconds: float) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.2)
    return False


def latest_csv(results_dir: Path, prefix: str, since_ts: float) -> Path:
    matches = sorted(results_dir.glob(f"{prefix}_*.csv"), key=lambda p: p.stat().st_mtime)
    if not matches:
        raise SuiteError(f"No CSV found for prefix '{prefix}' in {results_dir}")

    for candidate in reversed(matches):
        if candidate.stat().st_mtime >= since_ts - 1:
            return candidate

    raise SuiteError(
        f"Found CSVs for prefix '{prefix}', but none created after run start in {results_dir}"
    )


def mode_order_for_run(scenario: Scenario, repetition_index: int, rng: random.Random) -> list[str]:
    base = ["polling", "subscription"]
    if scenario.mode_order == "fixed":
        return base
    if scenario.mode_order == "alternating":
        return base if repetition_index % 2 == 1 else list(reversed(base))
    if rng.random() < 0.5:
        return base
    return list(reversed(base))


def _percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    sorted_values = sorted(values)
    if len(sorted_values) == 1:
        return sorted_values[0]

    position = (len(sorted_values) - 1) * quantile
    lower = int(position)
    upper = min(lower + 1, len(sorted_values) - 1)
    if lower == upper:
        return sorted_values[lower]
    fraction = position - lower
    return sorted_values[lower] + (sorted_values[upper] - sorted_values[lower]) * fraction


def _sample_process_telemetry(process: Any) -> TelemetrySample | None:
    try:
        memory = process.memory_info()
        return TelemetrySample(
            ts_unix=time.time(),
            cpu_percent=process.cpu_percent(interval=None),
            rss_mb=float(memory.rss) / (1024 * 1024),
            vms_mb=float(memory.vms) / (1024 * 1024),
        )
    except PSUTIL_PROCESS_EXCEPTIONS:
        return None


def collect_process_telemetry(
    proc: subprocess.Popen[str],
    interval_s: float,
    stop_event: threading.Event,
    out_samples: list[dict[str, float]],
) -> None:
    if psutil is None:
        return

    try:
        process = psutil.Process(proc.pid)
    except PSUTIL_PROCESS_EXCEPTIONS:
        return

    try:
        process.cpu_percent(interval=None)
    except PSUTIL_PROCESS_EXCEPTIONS:
        return

    while not stop_event.is_set():
        if proc.poll() is not None:
            break
        sample = _sample_process_telemetry(process)
        if sample is not None:
            out_samples.append(asdict(sample))
        stop_event.wait(interval_s)

    final_sample = _sample_process_telemetry(process)
    if final_sample is not None:
        out_samples.append(asdict(final_sample))


def summarize_telemetry(samples: list[dict[str, float]]) -> dict[str, float | int | None]:
    cpu_values = [float(sample["cpu_percent"]) for sample in samples]
    rss_values = [float(sample["rss_mb"]) for sample in samples]
    vms_values = [float(sample["vms_mb"]) for sample in samples]

    return {
        "sample_count": len(samples),
        "cpu_mean_percent": (sum(cpu_values) / len(cpu_values)) if cpu_values else None,
        "cpu_p95_percent": _percentile(cpu_values, 0.95),
        "cpu_max_percent": max(cpu_values) if cpu_values else None,
        "rss_mean_mb": (sum(rss_values) / len(rss_values)) if rss_values else None,
        "rss_p95_mb": _percentile(rss_values, 0.95),
        "rss_max_mb": max(rss_values) if rss_values else None,
        "vms_mean_mb": (sum(vms_values) / len(vms_values)) if vms_values else None,
        "vms_p95_mb": _percentile(vms_values, 0.95),
        "vms_max_mb": max(vms_values) if vms_values else None,
    }


def run_command_with_telemetry(
    cmd: list[str],
    cwd: Path,
    env: dict[str, str],
    log_path: Path,
    timeout_seconds: int,
    telemetry_interval_seconds: float,
) -> tuple[int, float, list[dict[str, float]], dict[str, float | int | None]]:
    started = time.time()
    telemetry_samples: list[dict[str, float]] = []
    with log_path.open("w", encoding="utf-8") as log_file:
        proc = subprocess.Popen(
            cmd,
            cwd=str(cwd),
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            text=True,
        )

        stop_event = threading.Event()
        telemetry_thread = threading.Thread(
            target=collect_process_telemetry,
            args=(proc, telemetry_interval_seconds, stop_event, telemetry_samples),
            daemon=True,
        )
        telemetry_thread.start()

        try:
            proc.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            raise SuiteError(f"Command timed out ({timeout_seconds}s): {' '.join(cmd)}")
        finally:
            stop_event.set()
            telemetry_thread.join(timeout=max(1.0, telemetry_interval_seconds * 2))

    return (
        proc.returncode,
        time.time() - started,
        telemetry_samples,
        summarize_telemetry(telemetry_samples),
    )


def csv_value(value: float | int | None) -> float | int | str:
    return "" if value is None else value


def write_json(path: Path, payload: Any) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def append_suite_summary_row(path: Path, row: dict[str, Any]) -> None:
    fieldnames = [
        "suite_name",
        "scenario_name",
        "repetition",
        "mode_order",
        "run_dir",
        "polling_csv",
        "subscription_csv",
        "analysis_dir",
        "polling_latency_p95_ms",
        "subscription_latency_p95_ms",
        "polling_samples_per_second",
        "subscription_samples_per_second",
        "polling_cpu_mean_percent",
        "subscription_cpu_mean_percent",
        "polling_cpu_p95_percent",
        "subscription_cpu_p95_percent",
        "polling_rss_mean_mb",
        "subscription_rss_mean_mb",
        "polling_rss_p95_mb",
        "subscription_rss_p95_mb",
        "polling_telemetry_samples",
        "subscription_telemetry_samples",
    ]

    file_exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def run_suite(
    workspace_root: Path,
    suite_path: Path,
    results_root: Path,
    seed: int,
) -> Path:
    ensure_psutil_available()
    telemetry_interval_seconds = _telemetry_interval_seconds()

    config = load_suite_config(suite_path)
    scenarios = parse_scenarios(config["scenarios"])
    suite_name = config["suite_name"]

    suite_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suite_dir = results_root / "suites" / f"{suite_name}_{suite_timestamp}"
    suite_dir.mkdir(parents=True, exist_ok=True)

    summary_csv = suite_dir / "suite_summary.csv"
    rng = random.Random(seed)

    suite_manifest = {
        "suite_name": suite_name,
        "created_at_utc": utc_now(),
        "suite_config_path": str(suite_path),
        "seed": seed,
        "python_executable": sys.executable,
        "python_version": sys.version,
        "workspace_root": str(workspace_root),
        "scenarios": [s.__dict__ for s in scenarios],
        "runs": [],
    }
    write_json(suite_dir / "suite_manifest.json", suite_manifest)
    shutil.copy2(suite_path, suite_dir / "suite_config_snapshot.json")

    for scenario in scenarios:
        for repetition in range(1, scenario.repetitions + 1):
            run_id = f"{scenario.name}_rep{repetition:02d}"
            run_dir = suite_dir / "runs" / run_id
            run_dir.mkdir(parents=True, exist_ok=True)

            mode_order = mode_order_for_run(scenario, repetition, rng)
            env_base = os.environ.copy()
            env_base.update(
                {
                    "EXPERIMENT_DURATION_SECONDS": str(scenario.duration_seconds),
                    "POLL_INTERVAL_SECONDS": str(scenario.poll_interval_seconds),
                    "SUBSCRIPTION_PUBLISHING_INTERVAL_MS": str(
                        scenario.subscription_publishing_interval_ms
                    ),
                    "RESULTS_DIR": str(run_dir),
                    "LOG_TO_CONSOLE": "0",
                    "SIMULATOR_UPDATE_INTERVAL_SECONDS": str(
                        scenario.simulator_update_interval_seconds
                    ),
                }
            )

            simulator_log = run_dir / "simulator.log"
            simulator_log_handle = simulator_log.open("w", encoding="utf-8")
            simulator_proc = subprocess.Popen(
                [sys.executable, "simulator/main.py"],
                cwd=str(workspace_root),
                env=env_base,
                stdout=simulator_log_handle,
                stderr=subprocess.STDOUT,
                text=True,
            )

            try:
                if not wait_for_port("127.0.0.1", 4840, timeout_seconds=15):
                    raise SuiteError("Simulator did not become reachable on port 4840")

                time.sleep(scenario.warmup_seconds)

                mode_csv: dict[str, Path] = {}
                mode_runtime_seconds: dict[str, float] = {}
                mode_telemetry_summary: dict[str, dict[str, float | int | None]] = {}
                mode_telemetry_path: dict[str, Path] = {}

                for mode in mode_order:
                    cmd = [sys.executable, "-m", f"clients.{mode}_client"]
                    started_at = time.time()
                    mode_log = run_dir / f"{mode}.log"
                    rc, elapsed, telemetry_samples, telemetry_summary = run_command_with_telemetry(
                        cmd=cmd,
                        cwd=workspace_root,
                        env=env_base,
                        log_path=mode_log,
                        timeout_seconds=max(60, scenario.duration_seconds * 4),
                        telemetry_interval_seconds=telemetry_interval_seconds,
                    )
                    if rc != 0:
                        raise SuiteError(f"{mode} client failed with exit code {rc}")
                    mode_runtime_seconds[mode] = elapsed
                    mode_csv[mode] = latest_csv(run_dir, f"{mode}_client", since_ts=started_at)

                    telemetry_path = run_dir / f"{mode}_telemetry.json"
                    write_json(
                        telemetry_path,
                        {
                            "mode": mode,
                            "sampling_interval_seconds": telemetry_interval_seconds,
                            "samples": telemetry_samples,
                            "summary": telemetry_summary,
                        },
                    )
                    mode_telemetry_path[mode] = telemetry_path
                    mode_telemetry_summary[mode] = telemetry_summary

                analysis_log = run_dir / "analysis.log"
                analysis_cmd = [
                    sys.executable,
                    "scripts/analyze_experiment.py",
                    "--polling",
                    str(mode_csv["polling"]),
                    "--subscription",
                    str(mode_csv["subscription"]),
                    "--results-dir",
                    str(run_dir),
                ]
                rc, _, _, _ = run_command_with_telemetry(
                    cmd=analysis_cmd,
                    cwd=workspace_root,
                    env=env_base,
                    log_path=analysis_log,
                    timeout_seconds=120,
                    telemetry_interval_seconds=telemetry_interval_seconds,
                )
                if rc != 0:
                    raise SuiteError(f"analysis failed with exit code {rc}")

                analysis_dirs = sorted(
                    run_dir.glob("analysis_*"), key=lambda p: p.stat().st_mtime
                )
                if not analysis_dirs:
                    raise SuiteError("No analysis output directory found")
                analysis_dir = analysis_dirs[-1]

                metrics_path = analysis_dir / "metrics_summary.json"
                with metrics_path.open("r", encoding="utf-8") as f:
                    metrics = json.load(f)

                run_manifest = {
                    "run_id": run_id,
                    "suite_name": suite_name,
                    "scenario": scenario.__dict__,
                    "repetition": repetition,
                    "mode_order": mode_order,
                    "started_at_utc": utc_now(),
                    "paths": {
                        "run_dir": str(run_dir),
                        "polling_csv": str(mode_csv["polling"]),
                        "subscription_csv": str(mode_csv["subscription"]),
                        "analysis_dir": str(analysis_dir),
                        "metrics_summary": str(metrics_path),
                        "simulator_log": str(simulator_log),
                        "polling_telemetry": str(mode_telemetry_path["polling"]),
                        "subscription_telemetry": str(mode_telemetry_path["subscription"]),
                    },
                    "runtime_seconds": mode_runtime_seconds,
                    "telemetry_summary": mode_telemetry_summary,
                }
                write_json(run_dir / "run_manifest.json", run_manifest)

                suite_manifest["runs"].append(run_manifest)
                write_json(suite_dir / "suite_manifest.json", suite_manifest)

                append_suite_summary_row(
                    summary_csv,
                    {
                        "suite_name": suite_name,
                        "scenario_name": scenario.name,
                        "repetition": repetition,
                        "mode_order": ",".join(mode_order),
                        "run_dir": str(run_dir),
                        "polling_csv": str(mode_csv["polling"]),
                        "subscription_csv": str(mode_csv["subscription"]),
                        "analysis_dir": str(analysis_dir),
                        "polling_latency_p95_ms": metrics["polling"]["latency_publish"][
                            "p95_ms"
                        ],
                        "subscription_latency_p95_ms": metrics["subscription"][
                            "latency_publish"
                        ]["p95_ms"],
                        "polling_samples_per_second": metrics["polling"][
                            "samples_per_second"
                        ],
                        "subscription_samples_per_second": metrics["subscription"][
                            "samples_per_second"
                        ],
                        "polling_cpu_mean_percent": csv_value(
                            mode_telemetry_summary["polling"]["cpu_mean_percent"]
                        ),
                        "subscription_cpu_mean_percent": csv_value(
                            mode_telemetry_summary["subscription"]["cpu_mean_percent"]
                        ),
                        "polling_cpu_p95_percent": csv_value(
                            mode_telemetry_summary["polling"]["cpu_p95_percent"]
                        ),
                        "subscription_cpu_p95_percent": csv_value(
                            mode_telemetry_summary["subscription"]["cpu_p95_percent"]
                        ),
                        "polling_rss_mean_mb": csv_value(
                            mode_telemetry_summary["polling"]["rss_mean_mb"]
                        ),
                        "subscription_rss_mean_mb": csv_value(
                            mode_telemetry_summary["subscription"]["rss_mean_mb"]
                        ),
                        "polling_rss_p95_mb": csv_value(
                            mode_telemetry_summary["polling"]["rss_p95_mb"]
                        ),
                        "subscription_rss_p95_mb": csv_value(
                            mode_telemetry_summary["subscription"]["rss_p95_mb"]
                        ),
                        "polling_telemetry_samples": csv_value(
                            mode_telemetry_summary["polling"]["sample_count"]
                        ),
                        "subscription_telemetry_samples": csv_value(
                            mode_telemetry_summary["subscription"]["sample_count"]
                        ),
                    },
                )

            finally:
                if simulator_proc.poll() is None:
                    simulator_proc.terminate()
                    try:
                        simulator_proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        simulator_proc.kill()
                        simulator_proc.wait()
                simulator_log_handle.close()

    return suite_dir


def main() -> None:
    parser = argparse.ArgumentParser(description="Run reproducible experiment suite")
    parser.add_argument(
        "--suite",
        required=True,
        help="Path to suite JSON configuration file",
    )
    parser.add_argument(
        "--results-root",
        default="results",
        help="Root directory for suite outputs",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Deterministic seed used for any randomized ordering",
    )
    args = parser.parse_args()

    workspace_root = Path(__file__).resolve().parent.parent
    suite_path = Path(args.suite).resolve()
    results_root = Path(args.results_root).resolve()

    suite_dir = run_suite(
        workspace_root=workspace_root,
        suite_path=suite_path,
        results_root=results_root,
        seed=args.seed,
    )

    print("Suite run complete")
    print(f"Suite output directory: {suite_dir}")
    print(f"Suite summary CSV     : {suite_dir / 'suite_summary.csv'}")
    print(f"Suite manifest JSON   : {suite_dir / 'suite_manifest.json'}")


if __name__ == "__main__":
    main()
