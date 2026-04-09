# Project Setup Guide (WSL)

This guide explains how to set up a clean development environment and run the project using a Python virtual environment.

---

## 1. Reset Virtual Environment (Recommended)

If you already have a virtual environment, remove it to ensure a clean setup.

### Deactivate (if active)

```bash
deactivate
```

Or simply open a new terminal.

### Delete existing venv

```bash
rm -rf .venv
```

---

## 2. Create a New Virtual Environment

```bash
python3 -m venv .venv
```

---

## 3. Activate the Virtual Environment

```bash
source .venv/bin/activate
```

---

## 4. Upgrade pip

```bash
python -m pip install --upgrade pip
```

---

## 5. Install Dependencies

```bash
pip install -r requirements.txt
```

---

## 6. Setup Environment Variables

Copy the example environment file:

```bash
cp .env.example .env
```

This creates a local `.env` file using default configuration values.

---

## 8. Full Setup (Quick Copy-Paste)

```bash
deactivate
rm -rf .venv
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
cp .env.example .env
```

---

## 10. Running the Project

After setup:

1. Start the OPC UA server
2. Run the polling client

Example:

```bash
python3 simulator/main.py
python3 -m clients.polling_client
```

---

## Notes

* Always activate the virtual environment before running the project:

  ```bash
  source .venv/bin/activate
  ```
* If something behaves unexpectedly, recreate the environment using the steps above.
* `.env` can be modified to test different configurations.

---

## 11. Reproducible Experiment Suite

Run a predefined suite with deterministic scenario settings, automatic manifests,
and per-run analysis output:

```bash
source .venv/bin/activate
python scripts/run_experiment_suite.py \
  --suite scripts/suites/thesis_baseline.json \
  --results-root results \
  --seed 42
```

What this produces:

* `results/suites/<suite_name>_<timestamp>/suite_manifest.json`
* `results/suites/<suite_name>_<timestamp>/suite_summary.csv`
* `results/suites/<suite_name>_<timestamp>/runs/<scenario>_repXX/...`
  * Raw CSV logs for polling + subscription
  * `run_manifest.json`
  * Analysis output (`analysis_*/metrics_summary.json` + plots)

Reproducibility notes:

* Scenario definitions are JSON files under `scripts/suites/`.
* The exact suite configuration is copied to
  `suite_config_snapshot.json` for each run.
* `--seed` makes randomized mode ordering reproducible.

---

## 12. Generate Suite Comparison Figures

After a suite run is complete, generate thesis-ready comparison PNGs from the
suite outputs:

```bash
source .venv/bin/activate
python scripts/plot_suite_comparisons.py \
  --suite-dir results/suites/thesis_baseline_20260408_130024 \
  --output-dir docs/figures
```

Generated files:

* `docs/figures/latency_comparison.png`
* `docs/figures/throughput_comparison.png`
* `docs/figures/host_load.png`
* `docs/figures/data_quality.png`

---
