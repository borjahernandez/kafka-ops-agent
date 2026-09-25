# Kafka Operations Agent

A self-hosted, model-portable Kafka operations agent and MCP toolkit for investigating cluster incidents with verifiable evidence.

**Status: A1 and A2 verified.** The offline foundation and a real local Confluent Kafka slow-consumer demonstration are implemented and tested. The A2 workload is synthetic and isolated. The reusable Kafka diagnostic adapter, MCP server and investigator are not implemented; Milestone A remains incomplete.

## Offline quickstart

With `uv` and Python 3.12.12 available:

```sh
uv sync --locked --python 3.12.12
uv run --locked kafka-ops lag --synthetic
uv run --locked kafka-ops lag --synthetic --max-partitions 1
uv run --locked ruff check .
uv run --locked ruff format --check .
uv run --locked pytest -q
```

Dependency installation may need package-index access; running the installed CLI and tests needs no network service, container or credentials. The only runtime dependency is Pydantic (and its transitive dependencies). Python, direct dependencies and build backend are pinned; `uv.lock` records the resolved environment.

The default command emits explicitly labelled synthetic JSON with fixed timestamps and a replay clock. Partition offset distances are `20`, `5` and `null`; collection status is `partial` because partition 2 has no commit. Repeated runs produce identical output. This is offset distance, not an exact business-record count, processing progress, replication lag or a cause diagnosis. `ok` means collection succeeded, not that a workload is healthy.

The CLI requires `--synthetic`. Only cluster `synthetic-demo`, topic `demo-orders` and group `demo-consumer` are allowed. JSON reports go to stdout; schema-invalid values and denied scopes produce JSON on stderr with exit code 2. Argument syntax errors use argparse's text usage message and exit code 2. Successfully rendered reports use exit code 0 even when collection is partial, stale or failed; inspect `status` and `coverage`.

## Foundation boundaries

- `domain/contracts.py`: immutable Pydantic evidence/capability models at schema version `1.0`, scope, limits and collection statuses. JSON schemas are available through `LagReport.model_json_schema()` and `ClusterCapabilities.model_json_schema()`.
- `services/lag.py`: exact scope authorization, adapter protocol, deadline/concurrency control, offset arithmetic, coverage and freshness handling. No Kafka, MCP or model SDK imports.
- `adapters/synthetic.py`: three frozen partition observations; no sockets or runtime control.
- `interfaces/cli.py`: argument validation, synthetic composition and JSON serialization.

Collection defaults are 100 partitions (hard maximum 1,000), a 5-second deadline (maximum 30), a 60-second freshness threshold and one concurrent collection per service instance (configurable up to 16). Queue time counts toward the deadline. Scope contains exactly one cluster/topic/group; wildcard discovery is absent. Adapters must honor cancellation and bound their own acquisition. Async timeouts cannot forcibly terminate blocking or cancellation-resistant adapters. These are local foundations, not verified real-cluster or cross-process budgets. A3 will add per-cluster budgets and external response-size enforcement.

This is a point-in-time snapshot contract; time-series windows and trend detection are deferred. The evidence ID identifies the request and source snapshot; freshness is evaluated separately at report time. Missing commits and reset/truncation-like relationships produce unknown distances and warnings. Processing and replication capabilities are explicitly unsupported.

See [A1 verification](docs/A1_VERIFICATION.md) for exact offline checks and [the A2 lab guide](docs/A2_LAB.md) for the pinned Confluent image, Docker host details, real scenario result, reproduction commands and limitations. A2 verifies the workload and recovery path only; the service's real Kafka integration belongs to A3.

## Two intended modes

- **Tools:** connect an existing assistant or workflow to read-only Kafka diagnostics through MCP, CLI and later REST. No model credentials are required by this service.
- **Investigator:** optionally run a bounded investigation with a supported model provider, producing evidence-backed findings and recommended operator checks.

The same diagnostic services and permissions underpin both modes. Configuration, capability checks and tested adapters will support attaching to existing infrastructure. No broad provider or platform compatibility is claimed before verification.

## Start here

1. [Current handoff and next task](docs/HANDOFF.md)
2. [Milestone A checklist](docs/MILESTONE_A.md)
3. [Full implementation plan](docs/PROJECT_PLAN.md)
4. [Development guidance](AGENTS.md)

## First demonstration

Run a synthetic Kafka workload, slow a consumer, investigate the resulting lag through diagnostic tools in a real MCP client, then restore the workload and verify recovery. The first demonstration does not require a custom agent loop, Flink, Kubernetes or a paid model API integration in this service.

## Safety and development boundaries

Diagnostic operations are read-only. Fault injection is isolated to an explicitly configured local lab. Real credentials, production telemetry and employer-owned materials must not enter the repository.

No container runtime, Kafka cluster or Python environment is installed by reading or cloning this repository. The quickstart creates a project-local Python environment only.

## Distribution

The initial repository is private. Public release and an open-source licence will be decided explicitly before distribution; no licence grant is implied by this bootstrap.
