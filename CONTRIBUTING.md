# Contributing to MeshWeaver

Thank you for your interest in contributing to MeshWeaver!

MeshWeaver is built with a strict **zero-heavy-dependency** design: it relies solely on the Python Standard Library (`asyncio`, `socket`, `json`, `threading`, `hmac`), `cloudpickle` for serialization, and optional `psutil` for metric collection.

---

## 1. Development Setup

1. **Clone the repository and create a virtual environment**:
   ```bash
   python -m venv .venv
   # Linux / macOS:
   source .venv/bin/activate
   # Windows (PowerShell):
   .venv\Scripts\Activate.ps1
   ```

2. **Install editable package and development dependencies**:
   ```bash
   pip install -e ".[dev]"
   ```

---

## 2. Running Tests

Run the full automated test suite with `pytest`:

```bash
python -m pytest -v
```

All 92+ tests must pass before submitting any change.

---

## 3. Running Real Network Verification

MeshWeaver includes a multi-process real-network verification suite that runs independent OS processes over real UDP sockets and exercises actual process terminations:

```bash
python verify_all.py
```

All 20 real network checks must pass cleanly.

---

## 4. Code Style & Linting

We use [Ruff](https://astral.sh/ruff) for fast, consistent linting and code quality checks.

```bash
ruff check .
```

Please fix all linting warnings prior to opening a PR.

---

## 5. Adding Tests

- Unit tests belong in `tests/test_<module_name>.py`.
- Integration and network tests should verify async event completion without arbitrary `sleep` timeouts where possible (use `asyncio.Event` or `asyncio.wait_for`).
- Never mock away network behavior in integration tests; test against actual bound UDP transports.

---

## 6. Pull Request Expectations

1. Keep pull requests focused on a single concern.
2. Ensure `python -m pytest -q` and `python verify_all.py` succeed cleanly.
3. Ensure no stale background processes remain after your tests finish.
4. Do not introduce new heavy runtime dependencies or compiled C-extensions.
5. Provide a clear PR description explaining the rationale and test evidence.
