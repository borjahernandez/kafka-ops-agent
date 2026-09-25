import hashlib
import json
import re
from pathlib import Path

from kafka_ops_agent.lab.contracts import LabObservation

FIXTURE = Path(__file__).parent / "fixtures" / "a2_slow_consumer.jsonl"


def test_sanitized_a2_trace_has_valid_contracts_and_expected_evidence():
    rows = [
        LabObservation.model_validate_json(line)
        for line in FIXTURE.read_text(encoding="utf-8").splitlines()
    ]

    assert len(rows) >= 7
    assert all(row.synthetic is True for row in rows)
    assert all(row.coverage.usable_partitions == 3 for row in rows)
    assert all(row.status.value == "ok" for row in rows)
    assert rows[0].partitions and sum(p.offset_distance for p in rows[0].partitions) == 0
    slow_totals = [
        sum(part.offset_distance for part in row.partitions)
        for row in rows
        if any(part.offset_distance for part in row.partitions)
    ]
    assert slow_totals == sorted(slow_totals)
    assert slow_totals[-1] >= 100
    assert all(sum(part.offset_distance for part in row.partitions) == 0 for row in rows[-2:])
    assert all(row.processing_progress is not None for row in rows)
    initial_progress = rows[0].processing_progress.records_processed_total
    final_progress = rows[-1].processing_progress.records_processed_total
    assert final_progress > initial_progress

    for line in FIXTURE.read_text(encoding="utf-8").splitlines():
        value = json.loads(line)
        assert re.fullmatch(r"a2-[0-9a-f]{16}", value["evidence_id"])
        assert not re.search(r"baseline|slow|recovery|delay|diagnosis|phase", line, re.IGNORECASE)
        hidden_control_fields = {
            "phase",
            "scenario_phase",
            "delay_seconds",
            "expected_evidence",
        }
        assert not hidden_control_fields.intersection(value)
        assert not {"delay_seconds", "phase"}.intersection(value.get("processing_progress", {}))


def test_trace_matches_recorded_provenance():
    provenance = json.loads(FIXTURE.with_name("a2_trace_provenance.json").read_text())
    assert hashlib.sha256(FIXTURE.read_bytes()).hexdigest() == provenance["fixture_sha256"]
    assert len(FIXTURE.read_text().splitlines()) == len(provenance["source_lines_1_based"])
