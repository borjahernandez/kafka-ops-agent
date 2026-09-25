"""Run one bounded A2 scenario against only the named local fixture broker."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field

from kafka_ops_agent.domain.contracts import Status
from kafka_ops_agent.lab.contracts import LabCoverage, LabObservation, LabPartition, LabProgress
from kafka_ops_agent.lab.validation import AcceptanceCriteria, validate_effect

if TYPE_CHECKING:
    from confluent_kafka import Consumer
    from confluent_kafka.admin import AdminClient

BOOTSTRAP = "127.0.0.1:9092"
TOPIC = "demo-orders"
GROUP = "demo-consumer"
PARTITIONS = 3
PROJECT = "kafka-ops-a2"
ROOT = Path(__file__).resolve().parents[3]
SCENARIO_FILE = ROOT / "lab" / "scenarios" / "slow-consumer.json"
DATA_ROOT = ROOT / ".data" / "a2-runs"
IMAGE = (
    "confluentinc/cp-kafka:8.3.2@"
    "sha256:5e8f3ab5b4977c9a8fd6137d26af2caad878aca316f24c55f08206217e3cec48"
)


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ScheduleStep(StrictModel):
    name: str
    duration_seconds: int | None = Field(default=None, ge=1, le=120)
    maximum_duration_seconds: int | None = Field(default=None, ge=1, le=120)
    consumer_delay_seconds: float = Field(ge=0, le=0.5)


class ScenarioPlan(StrictModel):
    schema_version: str = "1.0"
    scenario_id: str
    description: str
    prerequisites: dict[str, str | int | float]
    schedule: tuple[ScheduleStep, ...]
    expected_evidence: dict[str, Any]
    acceptance: AcceptanceCriteria
    cleanup: dict[str, Any]


def load_plan() -> ScenarioPlan:
    plan = ScenarioPlan.model_validate_json(SCENARIO_FILE.read_text(encoding="utf-8"))
    expected = {
        "compose_project": PROJECT,
        "broker_service": "broker",
        "bootstrap_servers": BOOTSTRAP,
        "kafka_image": IMAGE,
        "topic": TOPIC,
        "group": GROUP,
        "partition_count": PARTITIONS,
    }
    if plan.schema_version != "1.0" or plan.scenario_id != "slow-consumer-v1":
        raise ValueError("unsupported scenario contract")
    if any(plan.prerequisites.get(key) != value for key, value in expected.items()):
        raise ValueError("scenario prerequisites differ from the isolated fixture")
    if tuple(item.name for item in plan.schedule) != ("baseline", "slow_processing", "recovery"):
        raise ValueError(
            "scenario schedule must have baseline, slow-processing and recovery phases"
        )
    return plan


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def json_line(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(value, sort_keys=True) + "\n")
        stream.flush()


def ensure_fixture_broker() -> None:
    result = subprocess.run(
        ["docker", "inspect", "kafka-ops-a2-broker"],
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    if result.returncode:
        raise RuntimeError("named A2 broker is not running; start it with lab/compose.yaml")
    inspected = json.loads(result.stdout)
    if len(inspected) != 1:
        raise RuntimeError("Docker returned an unexpected number of A2 containers")
    container = inspected[0]
    labels = container["Config"]["Labels"]
    expected = {
        "com.docker.compose.project": PROJECT,
        "com.docker.compose.service": "broker",
        "com.kafka-ops.project": "kafka-ops-agent",
        "com.kafka-ops.fixture": "kafka-ops-a2",
    }
    if any(labels.get(key) != value for key, value in expected.items()):
        raise RuntimeError("broker container labels do not identify the A2 fixture")
    if container["Config"]["Image"] != IMAGE or container["State"]["Status"] != "running":
        raise RuntimeError("broker is not the running pinned A2 image")
    binding = container["HostConfig"]["PortBindings"]["9092/tcp"][0]
    if binding["HostIp"] != "127.0.0.1" or binding["HostPort"] != "9092":
        raise RuntimeError("broker listener is not bound only to the local host")


def wait_for_broker(admin: AdminClient, deadline_seconds: float = 45) -> None:
    from confluent_kafka import KafkaException

    deadline = time.monotonic() + deadline_seconds
    error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            admin.list_topics(timeout=3)
            return
        except KafkaException as caught:
            error = caught
            time.sleep(1)
    raise RuntimeError(f"A2 broker did not become ready: {type(error).__name__}")


def ensure_topic(admin: AdminClient) -> None:
    from confluent_kafka import KafkaException
    from confluent_kafka.admin import NewTopic

    metadata = admin.list_topics(timeout=5).topics.get(TOPIC)
    if metadata is None:
        try:
            admin.create_topics([NewTopic(TOPIC, PARTITIONS, replication_factor=1)])[TOPIC].result(
                timeout=10
            )
        except KafkaException as error:
            if error.args[0].code() != 36:  # TOPIC_ALREADY_EXISTS after concurrent startup
                raise
        metadata = admin.list_topics(topic=TOPIC, timeout=5).topics.get(TOPIC)
    if metadata is None or metadata.error is not None or len(metadata.partitions) != PARTITIONS:
        raise RuntimeError("A2 topic must exist with exactly three partitions")


def latest_progress(path: Path) -> dict[str, Any] | None:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
        return json.loads(lines[-1]) if lines else None
    except (OSError, ValueError, IndexError):
        return None


def evidence_consumer_config() -> dict[str, Any]:
    """Query stored commits for the workload group without joining that group."""
    return {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP,
        "client.id": "kafka-ops-a2-evidence-reader",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "allow.auto.create.topics": False,
        "socket.timeout.ms": 5000,
    }


def capture(admin: AdminClient, observer: Consumer, output: Path, evidence: Path) -> dict[str, Any]:
    from confluent_kafka import TopicPartition

    cluster_metadata = admin.list_topics(topic=TOPIC, timeout=5)
    metadata = cluster_metadata.topics[TOPIC]
    if not cluster_metadata.cluster_id:
        raise RuntimeError("broker did not return a Kafka cluster ID")
    topic_partitions = [TopicPartition(TOPIC, number) for number in sorted(metadata.partitions)]
    if len(topic_partitions) != PARTITIONS:
        raise RuntimeError("partition metadata changed during the scenario")
    committed = observer.committed(topic_partitions, timeout=5)
    by_partition = {item.partition: item.offset for item in committed}
    rows = []
    usable = 0
    for item in topic_partitions:
        low, high = observer.get_watermark_offsets(item, timeout=5, cached=False)
        value = by_partition.get(item.partition, -1)
        committed_offset = value if value >= 0 else None
        distance = high - value if value >= 0 and value <= high else None
        usable += distance is not None
        rows.append(
            {
                "partition": item.partition,
                "committed_offset": committed_offset,
                "log_start_offset": low,
                "end_offset": high,
                "offset_distance": distance,
                "status": "ok" if distance is not None else "partial",
            }
        )
    collected_at = utc_now()
    progress = latest_progress(output / "processing-progress.jsonl")
    bundle = LabObservation(
        schema_version="1.0",
        evidence_id=f"a2-{uuid.uuid4().hex[:16]}",
        source="confluent-kafka-python-local-lab",
        synthetic=True,
        cluster_id=cluster_metadata.cluster_id,
        topic=TOPIC,
        group=GROUP,
        observed_at=collected_at,
        collected_at=collected_at,
        status=Status.OK if usable == PARTITIONS else Status.PARTIAL,
        units="offset_distance",
        coverage=LabCoverage(
            expected_partitions=PARTITIONS,
            returned_partitions=len(rows),
            usable_partitions=usable,
            truncated=False,
        ),
        partitions=tuple(LabPartition.model_validate(row) for row in rows),
        processing_progress=(LabProgress.model_validate(progress) if progress else None),
        warnings=(
            "Synthetic local fixture; no production cluster was queried.",
            "Committed offset distance is not a business-record count.",
        ),
    )
    json_line(evidence, bundle.model_dump(mode="json"))
    return bundle.model_dump(mode="json")


class Scenario:
    def __init__(self, output: Path) -> None:
        from confluent_kafka import Consumer
        from confluent_kafka.admin import AdminClient

        self.output = output
        self.control = output / "scenario-control.json"
        self.evidence = output / "evidence.jsonl"
        self.log = output / "scenario-runner.log"
        self.plan = load_plan()
        self.children: list[subprocess.Popen] = []
        self.producer: subprocess.Popen | None = None
        self.consumer: subprocess.Popen | None = None
        self.observer_closed = False
        self.cleanup_verified = False
        self.cleanup_error: str | None = None
        self.admin = AdminClient(
            {"bootstrap.servers": BOOTSTRAP, "client.id": "kafka-ops-a2-observer"}
        )
        self.observer = Consumer(evidence_consumer_config())

    def set_delay(self, delay: float) -> None:
        if not 0 <= delay <= 0.5:
            raise ValueError("fixture delay must be from 0 through 0.5 seconds")
        temporary = self.control.with_suffix(".tmp")
        temporary.write_text(json.dumps({"delay_seconds": delay}), encoding="utf-8")
        os.replace(temporary, self.control)

    def start(self, *arguments: str, **extra_environment: str) -> subprocess.Popen:
        environment = os.environ | {"KAFKA_OPS_LAB_DATA": str(self.output)} | extra_environment
        stream_path = self.output / f"{arguments[0]}.log"
        stream = stream_path.open("ab")
        child = subprocess.Popen(
            [sys.executable, "-m", "kafka_ops_agent.lab.workload", *arguments],
            cwd=ROOT,
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=stream,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        child._kafka_ops_log_stream = stream
        self.children.append(child)
        return child

    def sample_for(self, seconds: float) -> list[LabObservation]:
        samples = []
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            snapshot = capture(self.admin, self.observer, self.output, self.evidence)
            samples.append(LabObservation.model_validate(snapshot))
            distances = [row["offset_distance"] for row in snapshot["partitions"]]
            json_line(
                self.log,
                {
                    "phase": "progress marker; not operational evidence",
                    "collected_at": snapshot["collected_at"],
                    "offset_distances": distances,
                    "records_processed_total": (snapshot["processing_progress"] or {}).get(
                        "records_processed_total"
                    ),
                },
            )
            time.sleep(min(2, max(0, deadline - time.monotonic())))
        return samples

    def recovered(self) -> bool:
        latest = latest_progress(self.output / "processing-progress.jsonl") or {}
        if latest.get("stopped"):
            return False
        snapshot = capture(self.admin, self.observer, self.output, self.evidence)
        offsets = [row["offset_distance"] for row in snapshot["partitions"]]
        return len(offsets) == PARTITIONS and all(value == 0 for value in offsets)

    def run(self) -> bool:
        ensure_fixture_broker()
        wait_for_broker(self.admin)
        ensure_topic(self.admin)
        baseline, intervention, recovery = self.plan.schedule
        self.set_delay(baseline.consumer_delay_seconds)
        self.consumer = self.start("consume", "--control-file", str(self.control))
        time.sleep(3)
        if self.consumer.poll() is not None:
            raise RuntimeError("synthetic consumer exited before the producer started")
        count = int(self.plan.prerequisites["producer_record_limit"])
        rate = float(self.plan.prerequisites["producer_rate_records_per_second"])
        self.producer = self.start("produce", "--records", str(count), "--rate", str(rate))
        baseline_samples = self.sample_for(baseline.duration_seconds)
        # Phase labels and expected outcomes are stored in the private run log;
        # evidence.jsonl contains observations only.
        self.set_delay(intervention.consumer_delay_seconds)
        json_line(self.log, {"scenario_phase": intervention.name, "at": utc_now()})
        slow_samples = self.sample_for(intervention.duration_seconds)
        self.set_delay(recovery.consumer_delay_seconds)
        assessment = validate_effect(baseline_samples, slow_samples, self.plan.acceptance)
        json_line(self.log, {"acceptance": assessment, "at": utc_now()})
        json_line(self.log, {"scenario_phase": recovery.name, "at": utc_now()})
        self.producer.wait(timeout=45)
        if self.producer.returncode != 0:
            raise RuntimeError("synthetic producer exited unsuccessfully")
        deadline = time.monotonic() + recovery.maximum_duration_seconds
        consecutive = 0
        samples_required = int(self.plan.cleanup["recovery_samples_required"])
        while time.monotonic() < deadline and consecutive < samples_required:
            consecutive = consecutive + 1 if self.recovered() else 0
            if consecutive < samples_required:
                time.sleep(2)
        if consecutive < samples_required:
            raise RuntimeError("consumer did not recover to end offsets three times")
        return True

    def cleanup(self) -> None:
        if self.cleanup_verified or self.cleanup_error:
            return
        recovery_step = self.plan.schedule[-1]
        try:
            self.set_delay(recovery_step.consumer_delay_seconds)
            producer_finished = self.producer is not None
            if self.producer is not None and self.producer.poll() is None:
                try:
                    self.producer.wait(timeout=35)
                except subprocess.TimeoutExpired:
                    producer_finished = False
                    self.producer.terminate()
            if self.producer is not None and self.producer.returncode != 0:
                producer_finished = False
            deadline = time.monotonic() + float(self.plan.cleanup["recovery_timeout_seconds"])
            samples_required = int(self.plan.cleanup["recovery_samples_required"])
            consecutive = 0
            while (
                producer_finished
                and self.consumer is not None
                and self.consumer.poll() is None
                and time.monotonic() < deadline
                and consecutive < samples_required
            ):
                consecutive = consecutive + 1 if self.recovered() else 0
                if consecutive < samples_required:
                    time.sleep(2)
            self.cleanup_verified = consecutive >= samples_required
            if not self.cleanup_verified:
                self.cleanup_error = "Recovery could not be verified during cleanup."
        except Exception as error:
            self.cleanup_error = f"Cleanup/recovery failed: {type(error).__name__}."
        finally:
            for child in self.children:
                if child.poll() is None:
                    child.terminate()
            child_cleanup_ok = True
            for child in self.children:
                if child.poll() is None:
                    try:
                        child.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        child.kill()
                        child.wait(timeout=5)
                        child_cleanup_ok = False
                child._kafka_ops_log_stream.close()
            if not child_cleanup_ok:
                self.cleanup_verified = False
                self.cleanup_error = "A workload process required forced termination."
            if not self.observer_closed:
                try:
                    self.observer.close()
                finally:
                    self.observer_closed = True


def main() -> int:
    output = DATA_ROOT / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output.mkdir(parents=True, exist_ok=False)
    scenario: Scenario | None = None
    recovered = False
    try:
        scenario = Scenario(output)
        recovered = scenario.run()
    except KeyboardInterrupt:
        print("Interrupted; stopping only the workload processes started by this run.")
        recovered = False
    except Exception as error:
        print(f"A2 scenario failed: {type(error).__name__}: {error}")
        recovered = False
    finally:
        if scenario is not None:
            scenario.cleanup()
    summary = {
        "schema_version": "1.0",
        "synthetic": True,
        "status": "recovered"
        if recovered and scenario and scenario.cleanup_verified
        else "failed_recovery_or_interrupted",
        "evidence_file": "evidence.jsonl",
        "collected_at": utc_now(),
    }
    if scenario is not None and scenario.cleanup_error:
        summary["cleanup_error"] = scenario.cleanup_error
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(f"Synthetic run: {output.relative_to(ROOT)}")
    print(f"Scenario status: {summary['status']}")
    return 0 if summary["status"] == "recovered" else 1


if __name__ == "__main__":
    raise SystemExit(main())
