# Development handoff

Prepared 24 September 2026. The dedicated Codex project is rooted in `Documents/GitHub/kafka-ops-agent`. Work in the task's assigned checkout, which may be a managed worktree attached to that project.

## Current state

**A1 update, 24 September 2026:** the offline foundation is implemented and verified in the assigned worktree, with no implementation commit or push. The bootstrap notes and original task below are historical. Read [A1_VERIFICATION.md](A1_VERIFICATION.md) and the README for current commands/results. A1 is complete; A2–A5 remain open. Next, select a lab runtime with the user before installing one or starting A2.

**A2 update, 24 September 2026:** user selected local Docker and requested Confluent Docker Hub images. The pinned `confluentinc/cp-kafka:8.3.2` ARM64 fixture and bounded synthetic producer/slow-consumer observer completed a real local baseline/slow/recovery run; a sanitized trace is covered by offline schema/evidence tests. Docker Desktop 4.92.0, Engine/CLI 29.8.1 and Compose 5.5.1 were verified. See [A2_LAB.md](A2_LAB.md) for commands, exact results and safety scope. A2 is complete for the lab demonstration. Continue to A3 only: implement a shared, bounded, read-only Kafka adapter and compare its CLI evidence with this fixture; do not add MCP or an investigator until their milestones.

**A2 review fixes:** replaced the altered fixture with unchanged original capture lines and opaque IDs, recorded provenance separately, added live slowdown acceptance checks and cleanup-aware exit status, and deferred Kafka SDK imports so offline tests work without the lab extra. All 82 tests pass in an isolated default environment. Run `20260924T144019Z` passed the new acceptance checks and recovered; see the lab guide for measured rates and evidence.

- The local Git repository was initialized on `main` with bootstrap documentation, `README.md`, `AGENTS.md` and `.gitignore`.
- The approved design is [PROJECT_PLAN.md](PROJECT_PLAN.md); the active delivery scope is [Milestone A](MILESTONE_A.md).
- No application, diagnostic tools, Kafka lab, MCP server or investigator has been implemented or tested.
- Bootstrap observations: `uv` 0.12.3; Homebrew Python 3.13.15 and uv-managed Python 3.12.12 were available. System Python was 3.9.6. Docker, Colima and Podman were not found on PATH. Recheck these facts before using them; no project runtime choice has been made.
- The private GitHub repository [borjahernandez/kafka-ops-agent](https://github.com/borjahernandez/kafka-ops-agent) was created and configured as `origin`. Inspect `git status`, the branch and `git remote -v`; do not infer a successful push or the current remote state from this document.

## Next bounded task

Use this prompt in the new project:

> Continue Milestone A in this task's assigned checkout under the dedicated project. Read `AGENTS.md`, `docs/HANDOFF.md`, `docs/MILESTONE_A.md` and the approved `docs/PROJECT_PLAN.md`, then inspect the repository and installed tools. Implement A1 only: minimal Python packaging with uv using one verified already-installed Python, typed/versioned evidence and capability contracts, a deterministic synthetic adapter, one mock-backed diagnostic CLI path, and model-independent unit tests/linting. Clearly label fixture output and keep shared services independent of MCP, Kafka and model SDKs. Preserve unrelated changes. Do not install a system container runtime, connect to a real cluster, add paid LLM usage, build REST or a custom agent loop, change repository visibility, or commit/push without a request covering that action. Run the checks, report changed files and exact results, update only verified checklist items, and stop with the runtime choice needed for A2.

After A1, select the lab runtime with the user. Then proceed through one reversible slow-consumer fixture, real read-only tools, CLI, stdio MCP and a real-client investigation. Do not implement later milestones merely because the full plan describes them.

## Evidence and safety expectations

Keep offline unit checks, real Kafka integration checks and actual MCP-client demonstrations distinct in progress reports. Report partial, stale, unavailable and denied data explicitly; never turn absent evidence into a healthy result.

Use original code and synthetic data. Keep secrets, private configuration and raw operational data out of Git. Production tools have no mutation capabilities. Fault injection belongs to an isolated lab-only harness with explicit targets, cleanup and verified recovery.
