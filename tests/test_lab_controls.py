import json
from pathlib import Path

import pytest

from kafka_ops_agent.lab.scenario import GROUP, IMAGE, evidence_consumer_config, load_plan
from kafka_ops_agent.lab.workload import read_delay


def test_scenario_plan_targets_only_pinned_local_fixture():
    plan = load_plan()
    assert plan.scenario_id == "slow-consumer-v1"
    assert plan.prerequisites["kafka_image"] == IMAGE
    assert plan.prerequisites["bootstrap_servers"] == "127.0.0.1:9092"
    assert [step.name for step in plan.schedule] == [
        "baseline",
        "slow_processing",
        "recovery",
    ]
    assert plan.schedule[1].consumer_delay_seconds == 0.25


def test_observer_reads_workload_group_without_committing():
    config = evidence_consumer_config()
    assert config["group.id"] == GROUP
    assert config["enable.auto.commit"] is False
    assert config["enable.auto.offset.store"] is False
    assert config["allow.auto.create.topics"] is False


@pytest.mark.parametrize("value", [0, 0.005, 0.25, 0.5])
def test_control_delay_accepts_only_bounded_numbers(tmp_path: Path, value: float):
    path = tmp_path / "control.json"
    path.write_text(json.dumps({"delay_seconds": value}), encoding="utf-8")
    assert read_delay(path) == float(value)


@pytest.mark.parametrize("value", [-0.01, 0.5001, True, "0.1", None, float("nan")])
def test_control_delay_rejects_unbounded_or_non_numeric_values(tmp_path: Path, value):
    path = tmp_path / "control.json"
    path.write_text(json.dumps({"delay_seconds": value}), encoding="utf-8")
    with pytest.raises(RuntimeError):
        read_delay(path)


def test_control_delay_rejects_missing_or_malformed_file(tmp_path: Path):
    with pytest.raises(RuntimeError):
        read_delay(tmp_path / "missing.json")
    path = tmp_path / "control.json"
    path.write_text("{}", encoding="utf-8")
    with pytest.raises(RuntimeError):
        read_delay(path)
