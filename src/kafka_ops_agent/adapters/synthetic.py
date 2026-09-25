"""Frozen offline fixture; never opens a network connection."""

from datetime import UTC, datetime

from kafka_ops_agent.domain.contracts import (
    Capability,
    ClusterCapabilities,
    LagRequest,
    OffsetObservation,
    Scope,
    Snapshot,
    Status,
)
from kafka_ops_agent.services.lag import CollectionFailure

FIXTURE_TIME = datetime(2026, 9, 24, 12, tzinfo=UTC)
FIXTURE_SCOPE = Scope(cluster_id="synthetic-demo", topic="demo-orders", group="demo-consumer")


class SyntheticAdapter:
    def capabilities(self, scope: Scope) -> ClusterCapabilities:
        if scope != FIXTURE_SCOPE:
            raise CollectionFailure(Status.PERMISSION_DENIED)
        return ClusterCapabilities(
            source="synthetic-fixture-v1",
            synthetic=True,
            scope=scope,
            operations=(
                Capability(
                    operation="committed_offset_lag",
                    status=Status.OK,
                    reason="Frozen synthetic offset observations only.",
                ),
                Capability(
                    operation="processing_progress",
                    status=Status.UNSUPPORTED,
                    reason="No processing telemetry in this fixture.",
                ),
                Capability(
                    operation="replication_lag",
                    status=Status.UNSUPPORTED,
                    reason="No replication telemetry in this fixture.",
                ),
            ),
            limitations=(
                "Synthetic data; no Kafka connection or compatibility evidence.",
                "Fixed replay clock; not a live freshness measurement.",
            ),
        )

    async def collect(self, request: LagRequest) -> Snapshot:
        self.capabilities(request.scope)
        rows = (
            OffsetObservation(partition=0, committed_offset=80, end_offset=100),
            OffsetObservation(partition=1, committed_offset=120, end_offset=125),
            OffsetObservation(partition=2, committed_offset=None, end_offset=50),
        )
        return Snapshot(
            source="synthetic-fixture-v1",
            synthetic=True,
            scope=request.scope,
            observed_at=FIXTURE_TIME,
            collected_at=FIXTURE_TIME,
            status=Status.OK,
            expected_partitions=len(rows),
            offsets=rows[: request.limits.max_partitions],
            truncated=len(rows) > request.limits.max_partitions,
            warnings=("SYNTHETIC OFFLINE FIXTURE: no Kafka or metrics were queried.",),
        )
