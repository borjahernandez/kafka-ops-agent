# A2 local synthetic Kafka lab

Prepared 24 September 2026. This is a development fixture only. Confluent documents one-node combined broker/controller KRaft mode as local experimentation, not production deployment.

## Exact fixture and scope

- Mac observed at setup: Apple Silicon (`arm64`), 8 CPU cores, 16 GiB physical memory.
- Runtime route selected by the user: local Docker.
- Broker: `confluentinc/cp-kafka:8.3.2`, pinned to image index digest `sha256:5e8f3ab5b4977c9a8fd6137d26af2caad878aca316f24c55f08206217e3cec48`. Docker resolves the ARM64 child digest for this host: `sha256:8ebc8a5a3741e3d02e5cbf69eb7d0f27e4546479c1171b05bfeec672502964ba`.
- Python workload client: `confluent-kafka==2.15.1`, an optional `lab` extra. The A1 core imports no Kafka SDK.
- Compose project and only broker service: `kafka-ops-a2` / `broker`.
- Explicit fixture labels: `com.kafka-ops.project=kafka-ops-agent` and `com.kafka-ops.fixture=kafka-ops-a2`.
- Broker publishes `127.0.0.1:9092` only; controller and internal listeners stay on the private Compose network.
- Topic/group: `demo-orders` / `demo-consumer`; exactly three partitions, one replica, manual synchronous commit for every processed message, auto-topic-creation disabled.
- The topic itself is created explicitly by the synthetic workload with Admin API calls scoped to this fixture topic. It is not created by the diagnostic service.
- Broker storage uses only the named Compose volume `kafka-ops-a2-kafka-data` within the `kafka-ops-a2` project. No runtime control privileges enter the diagnostic service.

Verified host on 24 September 2026: Docker Desktop 4.92.0, Docker Engine and CLI 29.8.1, Docker Compose 5.5.1, Apple Silicon ARM64, 8 CPUs, 16 GiB host RAM, and 7.748 GiB allocated to Docker. The Compose project uses the image index digest above; the broker container reports the same pinned digest. The broker used approximately 674 MiB at the time of inspection. Docker is accessed with `/Applications/Docker.app/Contents/Resources/bin` added to the command's `PATH`, which makes the signed app's credential helper available without changing Docker's global configuration.

## Start and run

From the repository root after Docker Desktop is installed and running:

```sh
uv sync --locked --extra lab --python 3.12.12
docker-compose -f lab/compose.yaml up -d --wait
docker-compose -f lab/compose.yaml ps
uv run --locked --extra lab python -m kafka_ops_agent.lab.scenario
```

Compose pulls the pinned Confluent image on first start (approximately 287 MB compressed for ARM64). The scenario creates one run directory under ignored `.data/a2-runs/` and records timestamped offset and processing-progress JSONL. This data is synthetic and contains no event payloads. Operational evidence does not include scenario phase, injected delay or expected diagnosis; those stay in a separate local controller log. The broker remains running after the scenario for inspection.

The data-driven `lab/scenarios/slow-consumer.json` specifies prerequisites, target, phase timing, delay values, expected observations and cleanup behavior. The controller validates that its broker, endpoint, image, topic, group and partition count match the isolated Compose fixture. It checks Docker labels before making Kafka connections. It never runs arbitrary Docker commands, mounts the Docker socket or stops/removes the broker.

The producer emits at most 600 small synthetic JSON events at 20 per second, explicitly distributed over the three fixture partitions. The consumer starts with a 5 ms delay, then the harness raises the delay to 250 ms for 16 seconds, then returns it to 5 ms. It records timestamped processing counts and last processed offsets each second. The observer separately reads group committed offsets and partition watermarks, so the report keeps committed offset distance separate from processing progress.

The harness checks that lag grows during the controlled interval and that all partitions catch up to their end offsets in three consecutive observations, within 45 seconds. The verified run on this host produced 600/600 records, 17 valid snapshots, a peak of 65 offsets on each partition during the slow interval, and zero distance across all three partitions in multiple recovery snapshots. It ended with `status: recovered`. Consumer progress and broker identity match in every snapshot. A prior run caught an evidence-reader group-ID defect and reported failed recovery; the reader was fixed to inspect the workload group's committed offsets before the successful run.

The selected synthetic observations are frozen in `tests/fixtures/a2_slow_consumer.jsonl`. After review, the initially hand-transcribed fixture was replaced with 13 complete, unchanged lines from run `20260924T142012Z`; the original offsets, timestamps and opaque evidence IDs are preserved. `tests/fixtures/a2_trace_provenance.json` separately records the source checksum, selected line numbers and fixture checksum. Byte-for-byte comparison against the original capture passed. Scenario phases, configured delays and expected diagnosis remain outside the evidence. Tests check the contracts, opaque IDs, absence of phase labels and recorded checksum. Generated run state stays under the ignored `.data/a2-runs/` path.

Acceptance thresholds are explicit in the scenario's `acceptance` data: baseline offset distance at most 10 in total, at least 30 offsets of growth during the slow interval, and a slow processing rate at most half the baseline rate. Rates use the consumer's own measurement timestamps and counters; progress older than 3 seconds is rejected. Initial group warm-up without progress is excluded, but each phase still requires at least two measured observations. Missing or contradictory evidence fails the demonstration even if the consumer eventually catches up. Assessment results go only to the controller log.

The corrected runner passed a new live run, `20260924T144019Z`: baseline processing was 18.42 records/second, slow processing was 4.54 records/second, and offset distance grew by 207 during the slow interval. Producer delivery and final consumer processing both reached 600 records. The final status was `recovered`, with cleanup verified. A failed cleanup now returns a nonzero exit code consistent with the summary, covered by regression tests.

## Cleanup and recovery

The scenario owns only the producer and consumer child processes it starts. On normal exit, error or interruption, it sets the delay back to 5 ms, lets its bounded producer finish (up to 35 seconds), attempts to verify catch-up, then terminates only those exact child processes. It records failed recovery in `summary.json` and returns nonzero when recovery cannot be proved. It leaves the broker available for inspection.

After inspecting the evidence, stop and remove this named fixture and its data volume:

```sh
docker-compose -f lab/compose.yaml down --volumes
```

This command targets only the Compose project declared at the top of `lab/compose.yaml`. It does not stop other Docker projects.

## Verification state

Verified: Docker Desktop/Engine and Compose startup, the pinned Confluent ARM64 fixture, baseline/slow/recovery against a running broker, bounded child-process cleanup, matching broker/consumer cluster identity, typed evidence and the preserved trace. Offline verification after review fixes: `uv run --isolated --locked pytest -q` (82 passed without the optional Kafka SDK), `uv run --locked ruff check .`, `uv run --locked ruff format --check .`, `uv lock --check`, `docker-compose -f lab/compose.yaml config --quiet`, and `git diff --check`. Kafka SDK imports occur only when live operations are invoked; fixture and controller regression tests require no Kafka dependency, broker or Docker daemon.

The A2 exit demonstration is complete on this host. Interruption/failure handling attempts catch-up, records an unproved recovery as failed and owns only its producer/consumer children; it does not stop the broker. The broker intentionally remains up for inspection. A3's reusable read-only Kafka adapter and actual diagnostic CLI path have not been built yet, so no claim is made about the toolkit querying a Kafka cluster.
