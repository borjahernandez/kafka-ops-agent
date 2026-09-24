# Kafka Operations Agent

A self-hosted, model-portable Kafka operations agent and MCP toolkit for investigating cluster incidents with verifiable evidence.

**Status: project bootstrap.** The repository currently contains the implementation plan and development handoff. Kafka tools, the fixture, MCP server and agent are not implemented yet. Milestone A builds the first useful local diagnostic toolkit; the investigator follows later.

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

No container runtime, Kafka cluster or Python environment is installed by reading or cloning this repository. Reproducible setup commands will be added and tested during Milestone A.

## Distribution

The initial repository is private. Public release and an open-source licence will be decided explicitly before distribution; no licence grant is implied by this bootstrap.
