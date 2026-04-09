#!/usr/bin/env python3
"""Generate suite-level comparison plots from experiment outputs.

This script reads:
- suite_summary.csv from a suite output directory
- metrics_summary.json for each run listed in suite_summary.csv

It writes four PNG files:
- latency_comparison.png
- throughput_comparison.png
- host_load.png
- data_quality.png
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from statistics import mean, stdev

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def mean_std(values: list[float]) -> tuple[float, float]:
    if not values:
        return 0.0, 0.0
    if len(values) == 1:
        return values[0], 0.0
    return mean(values), stdev(values)


def load_suite_summary(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def collect_scenarios(rows: list[dict[str, str]]) -> list[str]:
    scenarios: list[str] = []
    for row in rows:
        scenario = row["scenario_name"]
        if scenario not in scenarios:
            scenarios.append(scenario)
    return scenarios


def build_metric_maps(
    rows: list[dict[str, str]],
) -> tuple[
    dict[str, dict[str, list[float]]],
    dict[str, dict[str, list[float]]],
]:
    metrics: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    quality: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))

    numeric_columns = [
        "polling_latency_p95_ms",
        "subscription_latency_p95_ms",
        "polling_samples_per_second",
        "subscription_samples_per_second",
        "polling_cpu_mean_percent",
        "subscription_cpu_mean_percent",
        "polling_rss_mean_mb",
        "subscription_rss_mean_mb",
    ]

    for row in rows:
        scenario = row["scenario_name"]
        for column in numeric_columns:
            metrics[scenario][column].append(float(row[column]))

        metrics_path = Path(row["analysis_dir"]) / "metrics_summary.json"
        with metrics_path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        for mode in ("polling", "subscription"):
            sequence_quality = data.get(mode, {}).get("sequence_quality", {}) or {}
            missed = sum(
                int(signal_data.get("missed_seq_count", 0))
                for signal_data in sequence_quality.values()
                if isinstance(signal_data, dict)
            )
            duplicates = sum(
                int(signal_data.get("duplicate_seq_count", 0))
                for signal_data in sequence_quality.values()
                if isinstance(signal_data, dict)
            )
            quality[scenario][f"{mode}_missed"].append(float(missed))
            quality[scenario][f"{mode}_duplicate"].append(float(duplicates))

    return metrics, quality


def plot_latency(scenarios: list[str], metrics: dict[str, dict[str, list[float]]], output_path: Path) -> None:
    idx = list(range(len(scenarios)))
    width = 0.36
    colors = {"polling": "#1f77b4", "subscription": "#ff7f0e"}

    polling = [mean_std(metrics[s]["polling_latency_p95_ms"]) for s in scenarios]
    subscription = [mean_std(metrics[s]["subscription_latency_p95_ms"]) for s in scenarios]

    fig, ax = plt.subplots(figsize=(10, 5), dpi=200)
    ax.bar(
        [i - width / 2 for i in idx],
        [m for m, _ in polling],
        width,
        yerr=[e for _, e in polling],
        capsize=4,
        label="Polling",
        color=colors["polling"],
    )
    ax.bar(
        [i + width / 2 for i in idx],
        [m for m, _ in subscription],
        width,
        yerr=[e for _, e in subscription],
        capsize=4,
        label="Subscription",
        color=colors["subscription"],
    )
    ax.set_xticks(idx)
    ax.set_xticklabels(scenarios, rotation=10, ha="right")
    ax.set_ylabel("P95 latency (ms)")
    ax.set_title("End-to-End Latency Comparison by Scenario")
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)


def plot_throughput(
    scenarios: list[str],
    metrics: dict[str, dict[str, list[float]]],
    output_path: Path,
) -> None:
    idx = list(range(len(scenarios)))
    width = 0.36
    colors = {"polling": "#1f77b4", "subscription": "#ff7f0e"}

    polling = [mean_std(metrics[s]["polling_samples_per_second"]) for s in scenarios]
    subscription = [mean_std(metrics[s]["subscription_samples_per_second"]) for s in scenarios]

    fig, ax = plt.subplots(figsize=(10, 5), dpi=200)
    ax.bar(
        [i - width / 2 for i in idx],
        [m for m, _ in polling],
        width,
        yerr=[e for _, e in polling],
        capsize=4,
        label="Polling",
        color=colors["polling"],
    )
    ax.bar(
        [i + width / 2 for i in idx],
        [m for m, _ in subscription],
        width,
        yerr=[e for _, e in subscription],
        capsize=4,
        label="Subscription",
        color=colors["subscription"],
    )
    ax.set_xticks(idx)
    ax.set_xticklabels(scenarios, rotation=10, ha="right")
    ax.set_ylabel("Samples per second")
    ax.set_title("Throughput Comparison by Scenario")
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)


def plot_host_load(
    scenarios: list[str],
    metrics: dict[str, dict[str, list[float]]],
    output_path: Path,
) -> None:
    idx = list(range(len(scenarios)))
    width = 0.36
    colors = {"polling": "#1f77b4", "subscription": "#ff7f0e"}

    fig, (ax_cpu, ax_rss) = plt.subplots(2, 1, figsize=(10, 8), dpi=200, sharex=True)

    polling_cpu = [mean_std(metrics[s]["polling_cpu_mean_percent"]) for s in scenarios]
    subscription_cpu = [mean_std(metrics[s]["subscription_cpu_mean_percent"]) for s in scenarios]
    ax_cpu.bar(
        [i - width / 2 for i in idx],
        [m for m, _ in polling_cpu],
        width,
        yerr=[e for _, e in polling_cpu],
        capsize=4,
        label="Polling",
        color=colors["polling"],
    )
    ax_cpu.bar(
        [i + width / 2 for i in idx],
        [m for m, _ in subscription_cpu],
        width,
        yerr=[e for _, e in subscription_cpu],
        capsize=4,
        label="Subscription",
        color=colors["subscription"],
    )
    ax_cpu.set_ylabel("CPU mean (%)")
    ax_cpu.set_title("Host Load by Scenario")
    ax_cpu.legend()

    polling_rss = [mean_std(metrics[s]["polling_rss_mean_mb"]) for s in scenarios]
    subscription_rss = [mean_std(metrics[s]["subscription_rss_mean_mb"]) for s in scenarios]
    ax_rss.bar(
        [i - width / 2 for i in idx],
        [m for m, _ in polling_rss],
        width,
        yerr=[e for _, e in polling_rss],
        capsize=4,
        label="Polling",
        color=colors["polling"],
    )
    ax_rss.bar(
        [i + width / 2 for i in idx],
        [m for m, _ in subscription_rss],
        width,
        yerr=[e for _, e in subscription_rss],
        capsize=4,
        label="Subscription",
        color=colors["subscription"],
    )
    ax_rss.set_ylabel("RSS mean (MB)")
    ax_rss.set_xticks(idx)
    ax_rss.set_xticklabels(scenarios, rotation=10, ha="right")

    fig.tight_layout()
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)


def plot_data_quality(
    scenarios: list[str],
    quality: dict[str, dict[str, list[float]]],
    output_path: Path,
) -> None:
    idx = list(range(len(scenarios)))
    width = 0.36
    colors = {"polling": "#1f77b4", "subscription": "#ff7f0e"}

    fig, axes = plt.subplots(1, 2, figsize=(12, 5), dpi=200, sharey=True)

    for ax, suffix, title in (
        (axes[0], "missed", "Average Aggregated Missed Sequences"),
        (axes[1], "duplicate", "Average Aggregated Duplicate Sequences"),
    ):
        polling_vals = [
            mean(quality[s][f"polling_{suffix}"]) if quality[s][f"polling_{suffix}"] else 0.0
            for s in scenarios
        ]
        subscription_vals = [
            mean(quality[s][f"subscription_{suffix}"])
            if quality[s][f"subscription_{suffix}"]
            else 0.0
            for s in scenarios
        ]

        ax.bar([i - width / 2 for i in idx], polling_vals, width, label="Polling", color=colors["polling"])
        ax.bar(
            [i + width / 2 for i in idx],
            subscription_vals,
            width,
            label="Subscription",
            color=colors["subscription"],
        )
        ax.set_xticks(idx)
        ax.set_xticklabels(scenarios, rotation=10, ha="right")
        ax.set_title(title)
        ax.set_ylabel("Count per run (avg)")

    axes[1].legend()
    fig.suptitle("Data Quality by Scenario and Mode", y=1.02)
    fig.tight_layout()
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate suite-level comparison plots")
    parser.add_argument(
        "--suite-dir",
        required=True,
        help="Path to suite output directory (contains suite_summary.csv)",
    )
    parser.add_argument(
        "--output-dir",
        default="docs/figures",
        help="Directory where comparison PNGs will be written",
    )
    args = parser.parse_args()

    suite_dir = Path(args.suite_dir).resolve()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    suite_summary_path = suite_dir / "suite_summary.csv"
    if not suite_summary_path.exists():
        raise FileNotFoundError(f"Could not find suite_summary.csv in: {suite_dir}")

    rows = load_suite_summary(suite_summary_path)
    if not rows:
        raise ValueError(f"No rows found in {suite_summary_path}")

    scenarios = collect_scenarios(rows)
    metrics, quality = build_metric_maps(rows)

    # Keep labels short and readable in figures.
    label_map = {
        "baseline_60s": "Baseline (60 s)",
        "faster_polling_60s": "Faster Polling (60 s)",
        "stress_updates_120s": "Stress Updates (120 s)",
    }
    scenario_labels = [label_map.get(s, s) for s in scenarios]

    latency_path = output_dir / "latency_comparison.png"
    throughput_path = output_dir / "throughput_comparison.png"
    host_load_path = output_dir / "host_load.png"
    data_quality_path = output_dir / "data_quality.png"

    plot_latency(scenario_labels, {label_map.get(k, k): v for k, v in metrics.items()}, latency_path)
    plot_throughput(
        scenario_labels,
        {label_map.get(k, k): v for k, v in metrics.items()},
        throughput_path,
    )
    plot_host_load(
        scenario_labels,
        {label_map.get(k, k): v for k, v in metrics.items()},
        host_load_path,
    )
    plot_data_quality(
        scenario_labels,
        {label_map.get(k, k): v for k, v in quality.items()},
        data_quality_path,
    )

    print(f"Generated: {latency_path}")
    print(f"Generated: {throughput_path}")
    print(f"Generated: {host_load_path}")
    print(f"Generated: {data_quality_path}")


if __name__ == "__main__":
    main()
