# A1 offline foundation verification

Verified 24 September 2026 in the assigned `kafka-ops-agent` worktree. All evidence here is offline/synthetic; no Kafka or MCP compatibility is established.

## Repository and tools

- Started clean at bootstrap commit `8792bb67129d27c15b1de0a866ef3c625299e733`, detached HEAD.
- Origin: `https://github.com/borjahernandez/kafka-ops-agent.git`. No remote write or visibility change performed.
- `uv --version`: 0.12.3.
- `python3 --version` on PATH: system Python 3.9.6, left unchanged.
- `uv python list --only-installed`: Python 3.13.15, uv-managed 3.12.12 and system 3.9.6.
- Selected already-installed uv-managed Python 3.12.12, recorded in `.python-version`; package support currently restricted to Python 3.12.
- Docker, Colima and Podman were not found by `command -v`. No system runtime installed.

## Exact successful checks

```sh
uv sync --python 3.12.12
uv run --locked ruff check .
uv run --locked ruff format --check .
uv run --locked pytest -q
uv run --locked kafka-ops lag --synthetic
uv build --no-sources
git diff --check
```

Lint passed; formatting passed for 10 Python files; **57 tests passed**. Build produced an sdist and wheel under ignored `dist/`. Tests include serialization, version/units/timestamps, schema invariants, invalid requests, missing commits, negative/reset-like offset differences, collection and per-partition permissions, incomplete coverage, truncation, stale observations, timeout cancellation, concurrency and installed CLI behavior. Two subprocess CLI runs are compared byte-for-byte.

A second environment was created in a temporary directory using `UV_PROJECT_ENVIRONMENT=<temporary directory>/venv` and `UV_PYTHON_DOWNLOADS=never`. In that environment the following succeeded:

```sh
uv sync --locked --python 3.12.12
uv run --locked python --version
uv run --locked pytest -q
uv run --locked kafka-ops lag --synthetic
```

It reported Python 3.12.12, **57 tests passed**, and the same fixture evidence ID `lag-c0188df77bae8e9913d0f720`. The temporary environment was removed afterward. The project `.venv` remains local and ignored. No model, Kafka or container dependencies were installed.

## Reproducible output

Default CLI: source `synthetic-fixture-v1`, `synthetic: true`, observed/collected time `2026-09-24T12:00:00Z`, status `partial`, distances `[20, 5, null]`, expected/returned/usable coverage `3/3/2`, no truncation. Processing and replication telemetry are unsupported. Fixed replay time is intentional and is clearly disclosed; the fixture is not current cluster evidence.

## Limitations and next decision

A1 demonstrates contracts and deterministic local behavior only. No Kafka client, cluster, live workload, integration test, MCP server/client, model SDK, REST endpoint or investigator is present. Cancellation is cooperative, concurrency is per service instance, and collection bounds have only been exercised with synthetic adapters. No temporal lag trend or root cause is inferred.

Stop here. For A2, the user must select the lab runtime route: a Docker-compatible local runtime (for example Docker Desktop or Colima), or an explicitly selected remote development host. Verify that route before adding the pinned single-node Kafka workload and reversible slow-consumer scenario. Runtime installation has not been authorized by this increment.

Implementation changes remain uncommitted and unpushed. The lockfile is included in the working-tree changes for a later authorized commit.
