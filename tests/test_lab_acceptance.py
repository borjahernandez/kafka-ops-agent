import copy
import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from kafka_ops_agent.lab import scenario
from kafka_ops_agent.lab.contracts import LabObservation
from kafka_ops_agent.lab.validation import validate_effect


@pytest.fixture
def phases():
    path = Path(__file__).parent / "fixtures" / "a2_slow_consumer.jsonl"
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    return rows[:2], rows[2:10]


def assess(phases):
    baseline, slow = phases
    return validate_effect(
        [LabObservation.model_validate(row) for row in baseline],
        [LabObservation.model_validate(row) for row in slow],
        scenario.load_plan().acceptance,
    )


def test_actual_capture_demonstrates_slowdown(phases):
    result = assess(phases)
    assert result["offset_distance_growth"] == 194
    assert result["slow_records_per_second"] < result["baseline_records_per_second"] / 2


def test_initial_group_warmup_requires_two_later_measured_samples(phases):
    baseline, _ = phases
    warmup = copy.deepcopy(baseline[0])
    warmup["processing_progress"] = None
    warmup["coverage"]["usable_partitions"] = 0
    warmup["status"] = "partial"
    for partition in warmup["partitions"]:
        partition.update(committed_offset=None, offset_distance=None, status="partial")
    baseline.insert(0, warmup)
    assess(phases)
    baseline.pop()
    with pytest.raises(RuntimeError, match="insufficient"):
        assess(phases)


@pytest.mark.parametrize("fault", ["empty", "missing_progress", "flat_lag", "fast", "stale"])
def test_missing_or_contradictory_evidence_cannot_pass(phases, fault):
    baseline, slow = phases
    if fault == "empty":
        baseline.clear()
    elif fault == "missing_progress":
        for row in baseline:
            row["processing_progress"] = None
    elif fault == "flat_lag":
        for row in slow:
            for partition in row["partitions"]:
                partition["committed_offset"] = partition["end_offset"]
                partition["offset_distance"] = 0
    elif fault == "fast":
        for index, row in enumerate(slow):
            row["processing_progress"]["records_processed_total"] = 100 + index * 100
    else:
        slow[-1]["processing_progress"]["observed_at"] = baseline[0]["observed_at"]
    with pytest.raises(RuntimeError):
        assess(phases)


@pytest.mark.parametrize("cleanup_ok, expected_exit", [(True, 0), (False, 1)])
def test_exit_code_agrees_with_cleanup_status(tmp_path, monkeypatch, cleanup_ok, expected_exit):
    fake = Mock(cleanup_verified=cleanup_ok, cleanup_error=None)
    fake.run.return_value = True
    monkeypatch.setattr(scenario, "Scenario", Mock(return_value=fake))
    monkeypatch.setattr(scenario, "ROOT", tmp_path)
    monkeypatch.setattr(scenario, "DATA_ROOT", tmp_path / "runs")
    assert scenario.main() == expected_exit
    fake.cleanup.assert_called_once()
    summary = json.loads(next(tmp_path.glob("runs/*/summary.json")).read_text())
    assert (summary["status"] == "recovered") is cleanup_ok


def test_runner_rejects_missing_slowdown_and_still_cleans_up(tmp_path, monkeypatch):
    obj = scenario.Scenario.__new__(scenario.Scenario)
    obj.plan = scenario.load_plan()
    obj.admin = Mock()
    obj.control = tmp_path / "control.json"
    obj.log = tmp_path / "runner.log"
    obj.set_delay = Mock()
    obj.sample_for = Mock(return_value=[])
    obj.recovered = Mock(return_value=True)
    obj.cleanup = Mock()
    obj.cleanup_verified = True
    obj.cleanup_error = None
    consumer = Mock()
    consumer.poll.return_value = None
    obj.start = Mock(side_effect=[consumer, Mock(returncode=0)])
    for name in ("ensure_fixture_broker", "wait_for_broker", "ensure_topic"):
        monkeypatch.setattr(scenario, name, Mock())
    monkeypatch.setattr(scenario.time, "sleep", Mock())
    monkeypatch.setattr(scenario, "Scenario", Mock(return_value=obj))
    monkeypatch.setattr(scenario, "ROOT", tmp_path)
    monkeypatch.setattr(scenario, "DATA_ROOT", tmp_path / "runs")
    assert scenario.main() == 1
    obj.cleanup.assert_called_once()
    obj.recovered.assert_not_called()
