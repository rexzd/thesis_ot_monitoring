import argparse
import csv
import glob
import json
import os
from dataclasses import dataclass
from datetime import datetime
from statistics import mean, median

import matplotlib.pyplot as plt


@dataclass
class Row:
    recv_ts: float
    signal: str
    value: str
    seq: int
    source_ts: float
    publish_ts: float


def load_rows(csv_path: str) -> list[Row]:
    rows: list[Row] = []
    with open(csv_path, newline="", encoding="utf-8") as file_handle:
        reader = csv.DictReader(file_handle)
        for raw in reader:
            rows.append(
                Row(
                    recv_ts=float(raw["recv_ts"]),
                    signal=raw["signal"],
                    value=raw["value"],
                    seq=int(raw["seq"]),
                    source_ts=float(raw["source_ts"]),
                    publish_ts=float(raw["publish_ts"]),
                )
            )
    return rows


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    idx = int(round((len(sorted_values) - 1) * p))
    return sorted_values[idx]


def compute_sequence_quality(rows: list[Row]) -> dict[str, dict[str, int]]:
    grouped: dict[str, list[int]] = {}
    for row in rows:
        grouped.setdefault(row.signal, []).append(row.seq)

    quality: dict[str, dict[str, int]] = {}
    for signal, seq_list in grouped.items():
        gaps = 0
        duplicates = 0
        for i in range(1, len(seq_list)):
            delta = seq_list[i] - seq_list[i - 1]
            if delta == 0:
                duplicates += 1
            elif delta > 1:
                gaps += delta - 1
        quality[signal] = {
            "samples": len(seq_list),
            "missed_seq_count": gaps,
            "duplicate_seq_count": duplicates,
        }
    return quality


def compute_metrics(rows: list[Row]) -> dict:
    if not rows:
        return {}

    publish_latency = [row.recv_ts - row.publish_ts for row in rows]
    source_latency = [row.recv_ts - row.source_ts for row in rows]

    recv_start = min(row.recv_ts for row in rows)
    recv_end = max(row.recv_ts for row in rows)
    duration = max(recv_end - recv_start, 1e-9)

    return {
        "samples_total": len(rows),
        "duration_seconds": duration,
        "samples_per_second": len(rows) / duration,
        "latency_publish": {
            "mean_ms": mean(publish_latency) * 1000,
            "median_ms": median(publish_latency) * 1000,
            "p95_ms": percentile(publish_latency, 0.95) * 1000,
            "max_ms": max(publish_latency) * 1000,
        },
        "latency_source": {
            "mean_ms": mean(source_latency) * 1000,
            "median_ms": median(source_latency) * 1000,
            "p95_ms": percentile(source_latency, 0.95) * 1000,
            "max_ms": max(source_latency) * 1000,
        },
        "sequence_quality": compute_sequence_quality(rows),
    }


def latest_result(pattern: str) -> str | None:
    matches = glob.glob(pattern)
    if not matches:
        return None
    return max(matches, key=os.path.getmtime)


def plot_latency_comparison(
    polling_rows: list[Row],
    subscription_rows: list[Row],
    output_dir: str,
) -> str:
    polling_lat = [(row.recv_ts - row.publish_ts) * 1000 for row in polling_rows]
    subscription_lat = [(row.recv_ts - row.publish_ts) * 1000 for row in subscription_rows]

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.boxplot([polling_lat, subscription_lat], tick_labels=["Polling", "Subscription"])
    ax.set_title("Publish-to-Receive Latency (ms)")
    ax.set_ylabel("Latency (ms)")
    ax.grid(True, axis="y", alpha=0.3)

    out_path = os.path.join(output_dir, "latency_boxplot.png")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path


def plot_sequence_trace(
    polling_rows: list[Row],
    subscription_rows: list[Row],
    signal_name: str,
    output_dir: str,
) -> str:
    poll = [row for row in polling_rows if row.signal == signal_name]
    sub = [row for row in subscription_rows if row.signal == signal_name]

    fig, ax = plt.subplots(figsize=(9, 5))

    if poll:
        t0 = poll[0].recv_ts
        ax.plot(
            [row.recv_ts - t0 for row in poll],
            [row.seq for row in poll],
            marker="o",
            label="Polling",
        )

    if sub:
        t0_sub = sub[0].recv_ts
        ax.plot(
            [row.recv_ts - t0_sub for row in sub],
            [row.seq for row in sub],
            marker="x",
            label="Subscription",
        )

    ax.set_title(f"Sequence Progression for {signal_name}")
    ax.set_xlabel("Time Since First Sample (s)")
    ax.set_ylabel("Sequence Number")
    ax.grid(True, alpha=0.3)
    ax.legend()

    out_path = os.path.join(output_dir, f"sequence_trace_{signal_name}.png")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze polling vs subscription experiment CSV files")
    parser.add_argument("--polling", help="Path to polling CSV file")
    parser.add_argument("--subscription", help="Path to subscription CSV file")
    parser.add_argument(
        "--results-dir",
        default="results",
        help="Directory with experiment CSV files and outputs",
    )
    args = parser.parse_args()

    polling_path = args.polling or latest_result(
        os.path.join(args.results_dir, "polling_client_*.csv")
    )
    subscription_path = args.subscription or latest_result(
        os.path.join(args.results_dir, "subscription_client_*.csv")
    )

    if not polling_path or not subscription_path:
        raise SystemExit("Could not find both polling and subscription CSV files.")

    polling_rows = load_rows(polling_path)
    subscription_rows = load_rows(subscription_path)

    polling_metrics = compute_metrics(polling_rows)
    subscription_metrics = compute_metrics(subscription_rows)

    analysis_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = os.path.join(args.results_dir, f"analysis_{analysis_timestamp}")
    os.makedirs(out_dir, exist_ok=True)

    metrics = {
        "inputs": {
            "polling_csv": polling_path,
            "subscription_csv": subscription_path,
        },
        "polling": polling_metrics,
        "subscription": subscription_metrics,
    }

    metrics_path = os.path.join(out_dir, "metrics_summary.json")
    with open(metrics_path, "w", encoding="utf-8") as file_handle:
        json.dump(metrics, file_handle, indent=2)

    latency_plot = plot_latency_comparison(polling_rows, subscription_rows, out_dir)
    seq_plot = plot_sequence_trace(
        polling_rows,
        subscription_rows,
        signal_name="controller_status",
        output_dir=out_dir,
    )

    print("Analysis complete")
    print(f"Polling CSV      : {polling_path}")
    print(f"Subscription CSV : {subscription_path}")
    print(f"Summary JSON     : {metrics_path}")
    print(f"Latency plot     : {latency_plot}")
    print(f"Sequence plot    : {seq_plot}")


if __name__ == "__main__":
    main()
