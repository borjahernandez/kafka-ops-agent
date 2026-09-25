"""Offline JSON CLI. A real Kafka path is deliberately not registered."""

import argparse
import asyncio
import json
import sys

from pydantic import ValidationError

from kafka_ops_agent.adapters.synthetic import FIXTURE_SCOPE, FIXTURE_TIME, SyntheticAdapter
from kafka_ops_agent.domain.contracts import LagRequest, Limits, Scope
from kafka_ops_agent.services.lag import CollectionFailure, LagService


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Offline synthetic diagnostics; no Kafka connection"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    lag = commands.add_parser("lag", help="Report frozen synthetic committed offset distances")
    lag.add_argument("--synthetic", action="store_true", required=True)
    lag.add_argument("--cluster", default=FIXTURE_SCOPE.cluster_id)
    lag.add_argument("--topic", default=FIXTURE_SCOPE.topic)
    lag.add_argument("--group", default=FIXTURE_SCOPE.group)
    lag.add_argument("--max-partitions", type=int, default=100)
    lag.add_argument("--timeout-seconds", type=float, default=5)
    args = parser.parse_args()
    try:
        request = LagRequest(
            scope=Scope(cluster_id=args.cluster, topic=args.topic, group=args.group),
            limits=Limits(max_partitions=args.max_partitions, timeout_seconds=args.timeout_seconds),
        )
        service = LagService(
            SyntheticAdapter(), frozenset({FIXTURE_SCOPE}), clock=lambda: FIXTURE_TIME
        )
        report = asyncio.run(service.report(request))
    except (ValidationError, CollectionFailure) as error:
        status = error.status.value if isinstance(error, CollectionFailure) else "invalid_request"
        print(json.dumps({"schema_version": "1.0", "status": status}), file=sys.stderr)
        return 2
    print(report.model_dump_json(indent=2))
    return 0
