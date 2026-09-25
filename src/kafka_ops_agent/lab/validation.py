"""Offline acceptance checks; assessment results stay in the controller log."""

from pydantic import BaseModel, ConfigDict, Field

from kafka_ops_agent.lab.contracts import LabObservation


class AcceptanceCriteria(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    maximum_baseline_distance: int = Field(ge=0, le=100)
    minimum_distance_growth: int = Field(gt=0, le=10000)
    maximum_slow_rate_ratio: float = Field(gt=0, lt=1)
    maximum_progress_age_seconds: float = Field(gt=0, le=10)


def phase_measurements(
    samples: list[LabObservation], criteria: AcceptanceCriteria
) -> tuple[list[int], float]:
    # A new group may have no commits/progress before its first message. Exclude
    # only that leading warm-up; still require two usable measured observations.
    while samples and samples[0].processing_progress is None:
        samples = samples[1:]
    if len(samples) < 2:
        raise RuntimeError("insufficient observations to demonstrate the scenario")
    distances = []
    progress = []
    for sample in samples:
        if sample.coverage.usable_partitions != 3 or sample.coverage.truncated:
            raise RuntimeError("scenario acceptance requires complete offset coverage")
        distances.append(sum(row.offset_distance for row in sample.partitions))
        metric = sample.processing_progress
        if metric is None:
            continue
        age = (sample.collected_at - metric.observed_at).total_seconds()
        if not 0 <= age <= criteria.maximum_progress_age_seconds:
            raise RuntimeError("processing progress is stale or from the future")
        if progress and metric.observed_at == progress[-1].observed_at:
            continue
        if progress and (
            metric.observed_at < progress[-1].observed_at
            or metric.records_processed_total < progress[-1].records_processed_total
        ):
            raise RuntimeError("processing progress is not monotonic")
        progress.append(metric)
    if len(progress) < 2:
        raise RuntimeError("insufficient processing progress to measure a rate")
    elapsed = (progress[-1].observed_at - progress[0].observed_at).total_seconds()
    rate = (progress[-1].records_processed_total - progress[0].records_processed_total) / elapsed
    if rate <= 0:
        raise RuntimeError("processing progress must advance during each phase")
    return distances, rate


def validate_effect(
    baseline: list[LabObservation], slow: list[LabObservation], criteria: AcceptanceCriteria
) -> dict[str, float | int]:
    baseline_distances, baseline_rate = phase_measurements(baseline, criteria)
    slow_distances, slow_rate = phase_measurements(slow, criteria)
    if max(baseline_distances) > criteria.maximum_baseline_distance:
        raise RuntimeError("baseline did not demonstrate low committed lag")
    growth = slow_distances[-1] - slow_distances[0]
    if growth < criteria.minimum_distance_growth:
        raise RuntimeError("slow interval did not demonstrate growing committed lag")
    if slow_rate / baseline_rate > criteria.maximum_slow_rate_ratio:
        raise RuntimeError("slow interval did not demonstrate reduced processing rate")
    return {
        "baseline_records_per_second": baseline_rate,
        "slow_records_per_second": slow_rate,
        "offset_distance_growth": growth,
    }
