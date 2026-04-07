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
