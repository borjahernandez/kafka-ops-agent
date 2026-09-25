"""Immutable, validated contracts; no Kafka, protocol or model SDK objects."""

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

Name = Annotated[str, Field(min_length=1, max_length=200, pattern=r"^[a-zA-Z0-9._-]+$")]
Count = Annotated[int, Field(strict=True, ge=0)]


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Status(StrEnum):
    OK = "ok"
    PARTIAL = "partial"
    STALE = "stale"
    UNSUPPORTED = "unsupported"
    PERMISSION_DENIED = "permission_denied"
    UNAVAILABLE = "unavailable"
    TIMEOUT = "timeout"


class Scope(Contract):
    cluster_id: Name
    topic: Name
    group: Name


class Limits(Contract):
    max_partitions: Annotated[int, Field(strict=True, ge=1, le=1000)] = 100
    timeout_seconds: Annotated[float, Field(gt=0, le=30, allow_inf_nan=False)] = 5
    max_age_seconds: Annotated[float, Field(ge=0, le=86400, allow_inf_nan=False)] = 60


class LagRequest(Contract):
    scope: Scope
    limits: Limits = Limits()


class Capability(Contract):
    operation: Literal["committed_offset_lag", "processing_progress", "replication_lag"]
    status: Status
    reason: str


class ClusterCapabilities(Contract):
    schema_version: Literal["1.0"] = "1.0"
    source: Name
    synthetic: bool
    scope: Scope
    operations: tuple[Capability, ...]
    limitations: tuple[str, ...]


class Coverage(Contract):
    expected_partitions: Count | None
    returned_partitions: Count
    usable_partitions: Count
    truncated: bool = False

    @model_validator(mode="after")
    def consistent(self) -> Self:
        if self.usable_partitions > self.returned_partitions:
            raise ValueError("usable coverage exceeds returned coverage")
        if self.expected_partitions is not None:
            if self.returned_partitions > self.expected_partitions:
                raise ValueError("returned coverage exceeds expected coverage")
        return self


class OffsetObservation(Contract):
    partition: Count
    committed_offset: Count | None = None
    end_offset: Count | None = None
    status: Status = Status.OK
    warnings: tuple[str, ...] = ()


class Snapshot(Contract):
    source: Name
    synthetic: bool
    scope: Scope
    observed_at: AwareDatetime | None
    collected_at: AwareDatetime
    status: Status
    expected_partitions: Count | None
    truncated: bool = False
    offsets: tuple[OffsetObservation, ...] = Field(max_length=1000)
    warnings: tuple[str, ...] = ()

    @model_validator(mode="after")
    def consistent(self) -> Self:
        if self.observed_at is not None and self.observed_at > self.collected_at:
            raise ValueError("observation cannot follow collection")
        if self.offsets and self.observed_at is None:
            raise ValueError("offset observations require a timestamp")
        if len({row.partition for row in self.offsets}) != len(self.offsets):
            raise ValueError("duplicate partitions")
        if self.expected_partitions is not None and len(self.offsets) > self.expected_partitions:
            raise ValueError("rows exceed expected coverage")
        return self


class PartitionLag(Contract):
    partition: Count
    committed_offset: Count | None
    end_offset: Count | None
    offset_distance: Count | None
    status: Status
    warnings: tuple[str, ...]

    @model_validator(mode="after")
    def valid_distance(self) -> Self:
        if self.offset_distance is not None:
            if self.committed_offset is None or self.end_offset is None:
                raise ValueError("distance requires both offsets")
            if self.offset_distance != self.end_offset - self.committed_offset:
                raise ValueError("incorrect offset distance")
        if self.status == Status.OK and self.offset_distance is None:
            raise ValueError("ok requires an offset distance")
        return self


class LagReport(Contract):
    schema_version: Literal["1.0"] = "1.0"
    evidence_id: Name
    source: Name
    synthetic: bool
    scope: Scope
    observed_at: AwareDatetime | None
    collected_at: AwareDatetime
    status: Status
    units: Literal["offset_distance"] = "offset_distance"
    semantics: Literal["end_offset - committed_offset; not a business-record count"] = (
        "end_offset - committed_offset; not a business-record count"
    )
    coverage: Coverage
    partitions: tuple[PartitionLag, ...] = Field(max_length=1000)
    capabilities: ClusterCapabilities
    warnings: tuple[str, ...]

    @model_validator(mode="after")
    def consistent(self) -> Self:
        if self.observed_at is not None and self.observed_at > self.collected_at:
            raise ValueError("observation cannot follow collection")
        if self.partitions and self.observed_at is None:
            raise ValueError("partition evidence requires an observation timestamp")
        if (
            self.scope != self.capabilities.scope
            or self.source != self.capabilities.source
            or self.synthetic != self.capabilities.synthetic
        ):
            raise ValueError("report and capability provenance differ")
        if len({row.partition for row in self.partitions}) != len(self.partitions):
            raise ValueError("duplicate partitions")
        if self.coverage.returned_partitions != len(self.partitions):
            raise ValueError("returned coverage differs from evidence")
        if self.coverage.usable_partitions != sum(
            row.offset_distance is not None for row in self.partitions
        ):
            raise ValueError("usable coverage differs from evidence")
        if self.status == Status.OK and (
            not self.partitions
            or self.coverage.truncated
            or self.coverage.expected_partitions != len(self.partitions)
            or any(row.status != Status.OK for row in self.partitions)
        ):
            raise ValueError("ok requires complete usable coverage")
        return self


def utc_timestamp(value: datetime) -> datetime:
    """Validate injected clocks, including clocks supplied by embedded callers."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("clock must be timezone-aware")
    return value
