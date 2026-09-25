"""Bounded snapshot collection and committed offset-distance calculation."""

import asyncio
import hashlib
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Protocol

from kafka_ops_agent.domain.contracts import (
    ClusterCapabilities,
    Coverage,
    LagReport,
    LagRequest,
    PartitionLag,
    Scope,
    Snapshot,
    Status,
    utc_timestamp,
)


class OffsetAdapter(Protocol):
    """Trusted adapters must honor limits and cancellation; perform no mutations."""

    def capabilities(self, scope: Scope) -> ClusterCapabilities: ...

    async def collect(self, request: LagRequest) -> Snapshot: ...


class CollectionFailure(Exception):
    """Expected operational failure, without leaking raw SDK error text."""

    def __init__(self, status: Status):
        if status not in {
            Status.PERMISSION_DENIED,
            Status.UNAVAILABLE,
            Status.UNSUPPORTED,
            Status.TIMEOUT,
        }:
            raise ValueError("not a collection failure status")
        self.status = status
        super().__init__(status.value)


class LagService:
    def __init__(
        self,
        adapter: OffsetAdapter,
        allowed_scopes: frozenset[Scope],
        *,
        max_concurrency: int = 1,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if type(max_concurrency) is not int or not 1 <= max_concurrency <= 16:
            raise ValueError("max_concurrency must be between 1 and 16")
        self.adapter = adapter
        self.allowed_scopes = allowed_scopes
        self.clock = clock
        self._slots = asyncio.Semaphore(max_concurrency)

    async def report(self, request: LagRequest) -> LagReport:
        # Authorization precedes even capability discovery.
        if request.scope not in self.allowed_scopes:
            raise CollectionFailure(Status.PERMISSION_DENIED)
        capabilities = self.adapter.capabilities(request.scope)
        if capabilities.scope != request.scope:
            raise ValueError("adapter capability scope mismatch")
        try:
            # Deadline includes time waiting for the shared collection slot.
            async with asyncio.timeout(request.limits.timeout_seconds):
                async with self._slots:
                    snapshot = await self.adapter.collect(request)
        except (TimeoutError, CollectionFailure) as error:
            status = Status.TIMEOUT if isinstance(error, TimeoutError) else error.status
            snapshot = Snapshot(
                source=capabilities.source,
                synthetic=capabilities.synthetic,
                scope=request.scope,
                observed_at=None,
                collected_at=utc_timestamp(self.clock()),
                status=status,
                expected_partitions=None,
                offsets=(),
                warnings=(f"Collection failed: {status.value}.",),
            )
        if (
            snapshot.scope != request.scope
            or snapshot.source != capabilities.source
            or snapshot.synthetic != capabilities.synthetic
        ):
            raise ValueError("adapter provenance mismatch")
        now = utc_timestamp(self.clock())
        if snapshot.collected_at > now:
            raise ValueError("collection timestamp is in the future")
        rows = snapshot.offsets[: request.limits.max_partitions]
        truncated = snapshot.truncated or len(rows) < len(snapshot.offsets)
        partitions = []
        for row in rows:
            status, distance, warnings = row.status, None, list(row.warnings)
            if status in {Status.OK, Status.PARTIAL, Status.STALE}:
                if row.committed_offset is None or row.end_offset is None:
                    status = Status.PARTIAL
                    warnings.append("Missing committed or end offset; distance is unknown.")
                elif row.committed_offset > row.end_offset:
                    status = Status.PARTIAL
                    warnings.append(
                        "Commit exceeds end offset: reset, truncation or snapshot skew."
                    )
                else:
                    distance = row.end_offset - row.committed_offset
            partitions.append(
                PartitionLag(
                    partition=row.partition,
                    committed_offset=row.committed_offset,
                    end_offset=row.end_offset,
                    offset_distance=distance,
                    status=status,
                    warnings=tuple(warnings),
                )
            )
        usable = sum(row.offset_distance is not None for row in partitions)
        warnings = list(snapshot.warnings)
        status = snapshot.status
        incomplete = (
            not rows
            or truncated
            or snapshot.expected_partitions is None
            or len(rows) != snapshot.expected_partitions
            or any(row.status != Status.OK for row in partitions)
        )
        if status == Status.OK and incomplete:
            status = Status.PARTIAL
        if incomplete:
            warnings.append("Incomplete or unusable partition coverage; no health conclusion.")
        if (
            snapshot.observed_at is not None
            and (now - snapshot.observed_at).total_seconds() > request.limits.max_age_seconds
        ):
            warnings.append("Observation exceeds the requested freshness limit.")
            if status in {Status.OK, Status.PARTIAL}:
                status = Status.STALE
        warnings.extend(
            (
                "Committed offset distance does not establish processing progress or a cause.",
                "Processing progress and replication lag are not collected.",
            )
        )
        # Content identity is reproducible and changes with request limits or evidence.
        digest = hashlib.sha256(
            (request.model_dump_json() + snapshot.model_dump_json()).encode()
        ).hexdigest()[:24]
        return LagReport(
            evidence_id=f"lag-{digest}",
            source=snapshot.source,
            synthetic=snapshot.synthetic,
            scope=request.scope,
            observed_at=snapshot.observed_at,
            collected_at=snapshot.collected_at,
            status=status,
            coverage=Coverage(
                expected_partitions=snapshot.expected_partitions,
                returned_partitions=len(rows),
                usable_partitions=usable,
                truncated=truncated,
            ),
            partitions=tuple(partitions),
            capabilities=capabilities,
            warnings=tuple(warnings),
        )
