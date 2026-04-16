# Tests

```zsh
uv run pytest tests/
```

This command runs all unit tests (worker, scripts) and end-to-end tests. Note that end-to-end tests require docker compose to be running (see below).

## Unit tests

`tests/worker/` covers the playwright engine, the navigator, the crawler loop, the parquet batcher, telemetry integration, and worker integration. `tests/scripts/` covers seed, download, status, and shared settings. Everything is mocked, no NATS or browser required.

## End-to-end tests

Requires docker compose. Spins up real NATS and worker instances.

```zsh
cd tests/e2e
pip install -r requirements.txt
pytest
```

The `dc` fixture in `conftest.py` manages the full docker compose lifecycle for each test so tests always start from a clean state. It also clears a specific set of env vars before each run so values from a local `.env` don't bleed in.

`core/` tests the happy-path crawl from seed to completion and cookie acceptance. `resilience/` covers NATS going down mid-crawl, the worker starting before NATS is available, browser crashes, Xvfb crashes, timeouts, and the dead-letter queue. `infra/` covers stream and consumer auto-creation and model serialisation.
