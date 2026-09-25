"""Validated evidence captured from the isolated Confluent Kafka lab."""

from typing import Annotated, Literal, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from kafka_ops_agent.domain.contracts import Status

Count = Annotated[int, Field(strict=True, ge=0)]


class LabContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class LabCoverage(LabContract):
    expected_partitions: Count
    returned_partitions: Count
    usable_partitions: Count
    truncated: bool


class LabPartition(LabContract):
    partition: Count
    committed_offset: Count | None
    log_start_offset: Count
    end_offset: Count
    offset_distance: Count | None
    status: Literal["ok", "partial"]

    @model_validator(mode="after")
    def valid_distance(self) -> Self:
        if self.offset_distance is not None:
            if self.committed_offset is None:
                raise ValueError("distance requires a committed offset")
            if self.end_offset < self.committed_offset:
                raise ValueError("commit exceeds end offset")
            if self.offset_distance != self.end_offset - self.committed_offset:
                raise ValueError("incorrect offset distance")
        if (self.status == "ok") != (self.offset_distance is not None):
            raise ValueError("partition status differs from offset coverage")
        return self


class LabProgress(LabContract):
    observed_at: AwareDatetime
    cluster_id: str
    topic: str
    group: str
    records_processed_total: Count
    last_processed_offsets: dict[str, Count]


class LabObservation(LabContract):
    schema_version: Literal["1.0"]
    evidence_id: str
    source: Literal["confluent-kafka-python-local-lab"]
    synthetic: Literal[True]
    cluster_id: str
    topic: Literal["demo-orders"]
    group: Literal["demo-consumer"]
    observed_at: AwareDatetime
    collected_at: AwareDatetime
    status: Status
    units: Literal["offset_distance"]
    coverage: LabCoverage
    partitions: tuple[LabPartition, ...] = Field(max_length=3)
    processing_progress: LabProgress | None
    warnings: tuple[str, ...]

    @model_validator(mode="after")
    def consistent(self) -> Self:
        if self.observed_at > self.collected_at:
            raise ValueError("observation cannot follow collection")
        if len(self.partitions) != self.coverage.returned_partitions:
            raise ValueError("returned coverage differs from partition evidence")
        if self.coverage.expected_partitions != 3 or self.coverage.returned_partitions > 3:
            raise ValueError("lab evidence must be bounded to its three partitions")
        usable = sum(row.offset_distance is not None for row in self.partitions)
        if usable != self.coverage.usable_partitions:
            raise ValueError("usable coverage differs from partition evidence")
        if (self.status == Status.OK) != (usable == 3):
            raise ValueError("snapshot status differs from collection coverage")
        progress = self.processing_progress
        if progress is not None and (
            progress.cluster_id != self.cluster_id
            or progress.topic != self.topic
            or progress.group != self.group
        ):
            raise ValueError("processing progress belongs to a different resource")
        return self
