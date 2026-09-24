# Milestone A: local diagnostic toolkit and MCP demonstration

Status: bootstrap only. All implementation and integration checks below remain open until verified.

## Outcome

Run a synthetic Kafka workload, deliberately slow its consumer, and let an existing assistant investigate the resulting lag through read-only diagnostic tools exposed over local stdio MCP. Restore the workload and verify recovery.

The shared diagnostic toolkit is the core. CLI and MCP are thin interfaces to the same typed services; the service needs no LLM credentials. Milestone A does not include a custom agent loop.

Use the full target design in [PROJECT_PLAN.md](PROJECT_PLAN.md) as context, not an instruction to implement later milestones now. Follow [AGENTS.md](../AGENTS.md) and the bounded next task in [HANDOFF.md](HANDOFF.md).

## A1. Establish a testable, offline foundation

This is the next implementation increment. It must run without Kafka, Docker, network services or model credentials.

- [ ] Verify local tooling and choose one supported, already-installed Python version through `uv`; record it in the project configuration. Do not modify system Python.
- [ ] Create the minimal package, project-local environment, dependency lockfile, linting configuration and unit-test setup. Keep dependencies small; do not install model SDKs or an agent framework.
- [ ] Define typed, versioned evidence and capability contracts, plus request scope and collection limits. Include source, cluster/resource identity, evidence ID, observed/collected timestamps, units, coverage, truncation and explicit collection status.
- [ ] Represent `ok`, `partial`, `stale`, `unsupported`, `permission_denied`, `unavailable` and `timeout` without treating missing values as zero or healthy.
- [ ] Add one deterministic synthetic adapter and a shared lag-report service. Label fixture data clearly; do not create a fake integration that appears to query Kafka.
- [ ] Expose that service through a minimal `kafka-ops` CLI with structured JSON output, using the `kafka_ops_agent` package name from the target plan. Commands in the plan are proposals until implemented and tested.
- [ ] Test schema validation, serialization, timestamps/units, invalid inputs, missing commits, negative/reset-like offset differences and permission/coverage failures. Do not silently clamp invalid offset relationships into a healthy result.
- [ ] Run and report the exact lint and test commands, with reproducible fixture output.

Exit evidence: a fresh project environment can run the offline CLI and tests with no Kafka or LLM configuration. This proves contracts and local behavior, not Kafka or MCP compatibility.

Stop after this bounded increment and report the next lab-runtime decision.

## A2. Select a lab runtime and build one reversible workload

- [ ] Ask the user to select the container/runtime route before installing a system runtime. Options may include an existing Docker-compatible runtime or an explicitly selected remote development host. Verify the selected route rather than assuming Docker is available.
- [ ] Add a minimal single-node Kafka fixture with pinned dependencies/images and synthetic producer/consumer applications. Bind host-facing services locally where possible; do not expose an unauthenticated public endpoint.
- [ ] Make input rate, consumer processing delay and commit behavior explicit. Emit processing-progress measurements so growing committed lag is not the only evidence of slow processing.
- [ ] Isolate the lab with explicit project/resource identifiers and credentials. Keep any runtime control privileges outside the diagnostic service.
- [ ] Define the slow-consumer scenario as data: prerequisites, target, injection, observation window, expected evidence, cleanup and recovery checks. Keep expected diagnosis and injector state out of agent-visible evidence.
- [ ] Demonstrate baseline processing, rising lag under delay, and recovery after removing delay. Capture timestamped, sanitized evidence for offline tests.
- [ ] Ensure interrupted/failed scenarios attempt cleanup and report failed recovery. Limit stop/delete operations to the exact fixture resources.

Exit evidence: a documented run demonstrates the intended effect and verified recovery on the tested host/runtime. Record actual versions and resource needs; do not claim universal setup compatibility.

The initial slow-consumer demonstration does not need a three-broker cluster, broker failures, Alertmanager or a full monitoring dashboard. Add metrics only as needed for this evidence path; broader monitoring follows later.

## A3. Connect real, bounded, read-only diagnostics

- [ ] Implement small Kafka adapter contracts and a tested `confluent-kafka` implementation without leaking its SDK objects into the domain layer.
- [ ] Add the minimum useful operations: configured cluster listing/capabilities, cluster/topic/group descriptions and per-partition committed/end-offset lag.
- [ ] Add a bounded processing-metrics source for the lab; if Prometheus is selected, use named approved queries instead of arbitrary model-generated PromQL.
- [ ] Enforce cluster/topic/group allow-lists, request deadlines, result limits and per-cluster collection/concurrency budgets in shared services.
- [ ] Preserve source timestamps, incomplete coverage and typed access/connectivity failures. Treat offset distance as distinct from an exact count of unprocessed business records.
- [ ] Handle absent commits, unsupported group types, non-atomic snapshots and reset/truncation-like observations explicitly. Do not infer processing progress solely from commits.
- [ ] Verify unknown-topic requests cannot auto-create topics. Enforce read-only behavior in the adapter and test it with appropriately restricted Kafka credentials.
- [ ] Run the CLI against the synthetic cluster and compare its results with independent fixture observations. Mark integration tests separately from offline tests.

Exit evidence: CLI tools return useful real evidence for the lab's healthy, slow-consumer and recovered states, and explicitly report missing access/telemetry. No production cluster is in scope.

## A4. Expose the same services over stdio MCP

- [ ] Select and pin a maintained official MCP SDK/protocol combination; verify its current primary documentation during implementation.
- [ ] Publish only the implemented diagnostic operations with clear descriptions, input schemas and structured outputs.
- [ ] Keep handlers thin: reuse the same request scope, policy checks, limits and diagnostic services as the CLI.
- [ ] Keep protocol stdout clean; route diagnostic logging appropriately without secrets or raw sensitive payloads.
- [ ] Add protocol-level tests for tool discovery, valid calls, invalid arguments, denied scope, unavailable Kafka and bounded failure handling.
- [ ] Document one working local client configuration. Do not change the user's global client settings or replace existing integrations without agreement.

Exit evidence: a real MCP client can discover and invoke the implemented tools, and receives equivalent evidence to the CLI. A mock protocol test alone does not satisfy this condition.

## A5. Prove the first investigation and document its limits

- [ ] Connect one user-approved existing assistant/client to the stdio server. Use the user's existing authorized model access; do not introduce paid API calls or subscribe to a service for this milestone.
- [ ] Run baseline, slow-consumer and recovery phases using the isolated lab.
- [ ] Ask the assistant to investigate without revealing the injected diagnosis. Check that its explanation cites real lag and processing evidence, separates observations from hypotheses, and identifies missing evidence.
- [ ] Save a sanitized example and exact reproduction steps. Verify no credentials, private logs or evaluator-only labels enter committed examples.
- [ ] Publish the tested Kafka/runtime/MCP-client combination and known limitations. If a real client is not yet available, report that this exit gate remains open.
- [ ] Add a concise walkthrough: learning objective, start/stop instructions, guided investigation, independent check and cleanup/recovery verification.

Milestone A is complete only when the lab and real-client investigation are demonstrated, not merely when package tests pass or the MCP server starts.

## Deferred work

Keep REST, remote MCP/authentication, Alertmanager, durable investigation queues, provider adapters, automatic agent loops, multiple fault catalogues, Flink, Kubernetes diagnostics and public distribution outside this first slice. They remain in the target plan and require their own acceptance evidence.

Production diagnostics always remain read-only. Lab fault injection is a separate, explicitly authorized path; an assistant cannot obtain mutation privileges by requesting them through a diagnostic tool.
