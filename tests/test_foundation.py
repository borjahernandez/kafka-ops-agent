import asyncio
import json
import subprocess
from datetime import timedelta

import pytest
from pydantic import ValidationError

from kafka_ops_agent.adapters.synthetic import FIXTURE_SCOPE, FIXTURE_TIME, SyntheticAdapter
from kafka_ops_agent.domain.contracts import (
    Coverage,
    LagReport,
    LagRequest,
    Limits,
    OffsetObservation,
    PartitionLag,
    Scope,
    Snapshot,
    Status,
)
from kafka_ops_agent.services.lag import CollectionFailure, LagService


def run_report(adapter=None, *, limits=None, now=FIXTURE_TIME):
    return asyncio.run(
        LagService(
            adapter or SyntheticAdapter(),
            frozenset({FIXTURE_SCOPE}),
            clock=lambda: now,
        ).report(LagRequest(scope=FIXTURE_SCOPE, limits=limits or Limits()))
    )


class StubAdapter(SyntheticAdapter):
    def __init__(self, **changes):
        self.changes = changes

    async def collect(self, request):
        data = (await super().collect(request)).model_dump()
        data.update(self.changes)
        return Snapshot.model_validate(data)


def test_fixture_roundtrip_and_semantics():
    report = run_report()
    assert report == LagReport.model_validate_json(report.model_dump_json())
    assert report == run_report()
    assert report.synthetic and report.source == "synthetic-fixture-v1"
    assert report.status == Status.PARTIAL
    assert report.units == "offset_distance"
    assert report.observed_at == report.collected_at == FIXTURE_TIME
    assert [p.offset_distance for p in report.partitions] == [20, 5, None]
    assert report.coverage.usable_partitions == 2
    assert [c.status for c in report.capabilities.operations] == [
        Status.OK,
        Status.UNSUPPORTED,
        Status.UNSUPPORTED,
    ]


@pytest.mark.parametrize(
    "changes",
    [
        {"max_partitions": 0},
        {"max_partitions": 1001},
        {"max_partitions": True},
        {"timeout_seconds": 0},
        {"timeout_seconds": float("nan")},
        {"max_age_seconds": -1},
        {"unexpected": 1},
    ],
)
def test_invalid_limits(changes):
    with pytest.raises(ValidationError):
        Limits(**changes)


@pytest.mark.parametrize(
    "changes",
    [
        {"cluster_id": ""},
        {"topic": "*"},
        {"group": "a/b"},
    ],
)
def test_invalid_scope(changes):
    with pytest.raises(ValidationError):
        Scope.model_validate(FIXTURE_SCOPE.model_dump() | changes)


@pytest.mark.parametrize(
    "changes",
    [
        {"schema_version": "2.0"},
        {"units": "records"},
        {"extra": 1},
        {"observed_at": "2026-09-24T12:00:00"},
    ],
)
def test_invalid_report_schema(changes):
    with pytest.raises(ValidationError):
        LagReport.model_validate(run_report().model_dump() | changes)


@pytest.mark.parametrize(
    "changes",
    [
        {"status": "ok"},
        {"source": "other"},
        {"observed_at": None},
        {"observed_at": FIXTURE_TIME + timedelta(seconds=1)},
        {"coverage": {"expected_partitions": 3, "returned_partitions": 2, "usable_partitions": 2}},
    ],
)
def test_inconsistent_report_rejected(changes):
    with pytest.raises(ValidationError):
        LagReport.model_validate(run_report().model_dump() | changes)


@pytest.mark.parametrize(
    "committed,end,expected",
    [
        (None, 100, None),
        (5, None, None),
        (None, None, None),
        (101, 100, None),
        (0, 0, 0),
        (90, 100, 10),
    ],
)
def test_offset_relationships(committed, end, expected):
    report = run_report(
        StubAdapter(
            expected_partitions=1,
            offsets=(OffsetObservation(partition=0, committed_offset=committed, end_offset=end),),
        )
    )
    assert report.partitions[0].offset_distance == expected
    assert report.status == (Status.PARTIAL if expected is None else Status.OK)


@pytest.mark.parametrize("value", [-1, True, 1.5, "1"])
def test_invalid_raw_offsets(value):
    with pytest.raises(ValidationError):
        OffsetObservation(partition=0, committed_offset=value)


def test_distance_cannot_be_fabricated():
    with pytest.raises(ValidationError):
        PartitionLag(
            partition=0,
            committed_offset=1,
            end_offset=5,
            offset_distance=0,
            status=Status.OK,
            warnings=(),
        )


@pytest.mark.parametrize("status", list(Status))
def test_collection_status_preserved(status):
    report = run_report(StubAdapter(status=status, expected_partitions=None, offsets=()))
    assert report.status == (Status.PARTIAL if status == Status.OK else status)
    assert report.coverage.usable_partitions == 0


def test_partition_permission_failure():
    report = run_report(
        StubAdapter(
            expected_partitions=1,
            offsets=(OffsetObservation(partition=0, status=Status.PERMISSION_DENIED),),
        )
    )
    assert report.status == Status.PARTIAL
    assert report.partitions[0].status == Status.PERMISSION_DENIED
    assert report.partitions[0].offset_distance is None


def test_truncation_and_incomplete_coverage():
    report = run_report(limits=Limits(max_partitions=1))
    assert report.coverage == Coverage(
        expected_partitions=3, returned_partitions=1, usable_partitions=1, truncated=True
    )
    assert report.status == Status.PARTIAL
    assert run_report(StubAdapter(expected_partitions=4)).status == Status.PARTIAL


def test_staleness_retains_values_and_coverage():
    report = run_report(now=FIXTURE_TIME + timedelta(seconds=61))
    assert report.status == Status.STALE
    assert report.partitions[0].offset_distance == 20
    assert report.coverage.usable_partitions == 2
    assert run_report(now=FIXTURE_TIME + timedelta(seconds=60)).status == Status.PARTIAL


@pytest.mark.parametrize(
    "changes",
    [
        {"observed_at": FIXTURE_TIME + timedelta(seconds=1)},
        {"observed_at": None},
        {"expected_partitions": 1},
        {"offsets": (OffsetObservation(partition=0), OffsetObservation(partition=0))},
        {"collected_at": FIXTURE_TIME + timedelta(seconds=1)},
        {"source": "wrong-source"},
    ],
)
def test_rejects_broken_adapter_contract(changes):
    with pytest.raises(ValueError):
        run_report(StubAdapter(**changes))


def test_denied_scope_never_calls_adapter():
    class NeverAdapter(SyntheticAdapter):
        def capabilities(self, scope):
            pytest.fail("unauthorized adapter access")

    service = LagService(NeverAdapter(), frozenset())
    with pytest.raises(CollectionFailure) as error:
        asyncio.run(service.report(LagRequest(scope=FIXTURE_SCOPE)))
    assert error.value.status == Status.PERMISSION_DENIED


@pytest.mark.parametrize(
    "status", [Status.PERMISSION_DENIED, Status.UNAVAILABLE, Status.UNSUPPORTED, Status.TIMEOUT]
)
def test_typed_failures_return_evidence(status):
    class FailedAdapter(SyntheticAdapter):
        async def collect(self, request):
            raise CollectionFailure(status)

    report = run_report(FailedAdapter())
    assert report.status == status
    assert report.observed_at is None
    assert report.partitions == ()


def test_timeout_cancels_collection():
    cancelled = []

    class SlowAdapter(SyntheticAdapter):
        async def collect(self, request):
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.append(True)

    assert run_report(SlowAdapter(), limits=Limits(timeout_seconds=0.01)).status == Status.TIMEOUT
    assert cancelled == [True]


def test_shared_concurrency_budget():
    active = peak = 0

    class CountingAdapter(SyntheticAdapter):
        async def collect(self, request):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            try:
                await asyncio.sleep(0)
                return await super().collect(request)
            finally:
                active -= 1

    async def collect_all():
        service = LagService(
            CountingAdapter(),
            frozenset({FIXTURE_SCOPE}),
            max_concurrency=2,
            clock=lambda: FIXTURE_TIME,
        )
        return await asyncio.gather(
            *(service.report(LagRequest(scope=FIXTURE_SCOPE)) for _ in range(10))
        )

    assert len(asyncio.run(collect_all())) == 10
    assert peak == 2 and active == 0


def test_installed_cli_json():
    command = ["kafka-ops", "lag", "--synthetic"]
    first = subprocess.run(command, capture_output=True, text=True, check=True)
    second = subprocess.run(command, capture_output=True, text=True, check=True)
    assert first.stdout == second.stdout and first.stderr == ""
    assert LagReport.model_validate_json(first.stdout) == run_report()


@pytest.mark.parametrize(
    "args,status",
    [
        (["--max-partitions", "0"], "invalid_request"),
        (["--cluster", "production"], "permission_denied"),
    ],
)
def test_cli_rejects_invalid_requests(args, status):
    result = subprocess.run(
        ["kafka-ops", "lag", "--synthetic", *args], capture_output=True, text=True
    )
    assert result.returncode == 2 and result.stdout == ""
    assert json.loads(result.stderr)["status"] == status
