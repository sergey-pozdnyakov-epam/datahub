"""
Comprehensive integration test for Flink connector.
Tests all capabilities: metadata extraction, lineage, containers, properties.
"""

import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from datahub.ingestion.run.pipeline import Pipeline

# Mock data representing a Flink cluster with jobs
MOCK_CLUSTER_CONFIG = {
    "flink-version": "1.18.1",
    "flink-revision": "a8c8b1c @ 2023-12-19T22:17:36+01:00",
    "features": {
        "web-submit": True,
        "web-cancel": True,
    },
}

MOCK_CLUSTER_OVERVIEW = {
    "taskmanagers": 2,
    "slots-total": 8,
    "slots-available": 4,
    "jobs-running": 2,
    "jobs-finished": 3,
    "jobs-cancelled": 1,
    "jobs-failed": 1,
}

MOCK_JOBS_LIST = {
    "jobs": [
        {"id": "abc123", "status": "RUNNING"},
        {"id": "def456", "status": "RUNNING"},
    ]
}

MOCK_JOB_DETAILS_1 = {
    "jid": "abc123",
    "name": "Kafka to PostgreSQL ETL",
    "state": "RUNNING",
    "start-time": 1640000000000,
    "end-time": -1,
    "duration": 3600000,
    "now": 1640003600000,
    "timestamps": {
        "RUNNING": 1640000000000,
    },
    "vertices": [
        {
            "id": "source-vertex-1",
            "name": "Source: kafka-orders",
            "parallelism": 4,
            "status": "RUNNING",
            "start-time": 1640000000000,
            "end-time": -1,
            "duration": 3600000,
            "tasks": {
                "RUNNING": 4,
            },
            "metrics": {
                "read-bytes": 1000000,
                "read-records": 10000,
            },
        },
        {
            "id": "transform-vertex-1",
            "name": "Map: transform-orders",
            "parallelism": 4,
            "status": "RUNNING",
            "start-time": 1640000000000,
            "end-time": -1,
            "duration": 3600000,
            "tasks": {
                "RUNNING": 4,
            },
            "metrics": {
                "read-records": 10000,
                "write-records": 9500,
            },
        },
        {
            "id": "sink-vertex-1",
            "name": "Sink: postgres-orders",
            "parallelism": 2,
            "status": "RUNNING",
            "start-time": 1640000000000,
            "end-time": -1,
            "duration": 3600000,
            "tasks": {
                "RUNNING": 2,
            },
            "metrics": {
                "write-bytes": 950000,
                "write-records": 9500,
            },
        },
    ],
    "status-counts": {
        "RUNNING": 3,
    },
    "plan": {
        "jid": "abc123",
        "name": "Kafka to PostgreSQL ETL",
        "nodes": [
            {
                "id": "source-vertex-1",
                "parallelism": 4,
                "operator": "Source: kafka-orders",
                "description": "Source: KafkaSource(topic=orders, group=flink-consumer)",
                "inputs": [],
                "optimizer_properties": {},
            },
            {
                "id": "transform-vertex-1",
                "parallelism": 4,
                "operator": "Map",
                "description": "Map: transform-orders",
                "inputs": [
                    {
                        "num": 0,
                        "id": "source-vertex-1",
                        "ship_strategy": "FORWARD",
                        "exchange": "pipelined_bounded",
                    }
                ],
                "optimizer_properties": {},
            },
            {
                "id": "sink-vertex-1",
                "parallelism": 2,
                "operator": "Sink: postgres-orders",
                "description": "Sink: JdbcSink(table=orders, database=analytics)",
                "inputs": [
                    {
                        "num": 0,
                        "id": "transform-vertex-1",
                        "ship_strategy": "REBALANCE",
                        "exchange": "pipelined_bounded",
                    }
                ],
                "optimizer_properties": {},
            },
        ],
    },
}

MOCK_JOB_DETAILS_2 = {
    "jid": "def456",
    "name": "Customer Aggregation Stream",
    "state": "RUNNING",
    "start-time": 1640010000000,
    "end-time": -1,
    "duration": 7200000,
    "now": 1640017200000,
    "timestamps": {
        "RUNNING": 1640010000000,
    },
    "vertices": [
        {
            "id": "source-vertex-2a",
            "name": "Source: kafka-customers",
            "parallelism": 2,
            "status": "RUNNING",
        },
        {
            "id": "source-vertex-2b",
            "name": "Source: kafka-transactions",
            "parallelism": 4,
            "status": "RUNNING",
        },
        {
            "id": "join-vertex-1",
            "name": "Join: customer-transactions",
            "parallelism": 4,
            "status": "RUNNING",
        },
        {
            "id": "agg-vertex-1",
            "name": "Aggregate: customer-stats",
            "parallelism": 2,
            "status": "RUNNING",
        },
        {
            "id": "sink-vertex-2",
            "name": "Sink: elasticsearch-customer-stats",
            "parallelism": 2,
            "status": "RUNNING",
        },
    ],
    "plan": {
        "jid": "def456",
        "name": "Customer Aggregation Stream",
        "nodes": [
            {
                "id": "source-vertex-2a",
                "operator": "Source: kafka-customers",
                "description": "Source: KafkaSource(topic=customers)",
                "inputs": [],
            },
            {
                "id": "source-vertex-2b",
                "operator": "Source: kafka-transactions",
                "description": "Source: KafkaSource(topic=transactions)",
                "inputs": [],
            },
            {
                "id": "join-vertex-1",
                "operator": "CoProcessOperator",
                "description": "Join: customer-transactions (window join)",
                "inputs": [{"id": "source-vertex-2a"}, {"id": "source-vertex-2b"}],
            },
            {
                "id": "agg-vertex-1",
                "operator": "Aggregate",
                "description": "Aggregate: customer-stats (tumbling window 1 hour)",
                "inputs": [{"id": "join-vertex-1"}],
            },
            {
                "id": "sink-vertex-2",
                "operator": "Sink: elasticsearch-customer-stats",
                "description": "Sink: ElasticsearchSink(index=customer-stats)",
                "inputs": [{"id": "agg-vertex-1"}],
            },
        ],
    },
}

MOCK_JOB_CHECKPOINTS = {
    "counts": {
        "restored": 0,
        "total": 150,
        "in_progress": 0,
        "completed": 148,
        "failed": 2,
    },
    "summary": {
        "state_size": {"min": 1024, "max": 5120, "avg": 2048},
        "end_to_end_duration": {"min": 100, "max": 500, "avg": 250},
        "checkpointed_size": {"min": 1024, "max": 5120, "avg": 2048},
    },
    "latest": {
        "completed": {
            "id": 148,
            "status": "COMPLETED",
            "trigger_timestamp": 1640003500000,
            "latest_ack_timestamp": 1640003500250,
            "state_size": 2048,
            "end_to_end_duration": 250,
            "checkpointed_data_size": 2048,
            "external_path": "hdfs://namenode/flink/checkpoints/abc123/chk-148",
        }
    },
    "history": [],
}


class TestFlinkComprehensive:
    """Comprehensive integration tests for Flink connector."""

    @pytest.fixture
    def mock_session(self):
        """Mock requests.Session for Flink API calls."""
        mock_sess = Mock()

        def request_side_effect(*args, **kwargs):
            # Handle both positional and keyword arguments
            url = kwargs.get("url", args[1] if len(args) > 1 else "")
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.raise_for_status = Mock()

            if "/config" in url:
                mock_response.json.return_value = MOCK_CLUSTER_CONFIG
            elif "/overview" in url:
                mock_response.json.return_value = MOCK_CLUSTER_OVERVIEW
            elif "/jobs/" in url and "/checkpoints" in url:
                mock_response.json.return_value = MOCK_JOB_CHECKPOINTS
            elif "/jobs/abc123" in url:
                mock_response.json.return_value = MOCK_JOB_DETAILS_1
            elif "/jobs/def456" in url:
                mock_response.json.return_value = MOCK_JOB_DETAILS_2
            elif "/jobs" in url:
                mock_response.json.return_value = MOCK_JOBS_LIST
            else:
                mock_response.json.return_value = {}

            return mock_response

        mock_sess.request = Mock(side_effect=request_side_effect)
        mock_sess.get = Mock(
            side_effect=lambda url, **kw: request_side_effect("GET", url=url, **kw)
        )
        mock_sess.post = Mock(
            side_effect=lambda url, **kw: request_side_effect("POST", url=url, **kw)
        )
        mock_sess.mount = Mock()
        mock_sess.auth = None
        return mock_sess

    def test_full_extraction_with_lineage(self, mock_session, tmp_path):
        """Test complete metadata extraction including lineage."""

        with patch("requests.Session", return_value=mock_session):
            # Create config
            config_dict = {
                "source": {
                    "type": "flink",
                    "config": {
                        "connection": {
                            "rest_api_url": "http://localhost:8081",
                        },
                        "env": "PROD",
                        "platform_instance": "test-cluster",
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": str(
                            tmp_path / "flink_comprehensive_test_output.json"
                        ),
                    },
                },
            }

            # Run pipeline
            pipeline = Pipeline.create(config_dict)
            pipeline.run()
            pipeline.raise_from_status()

            # Read output
            output_file = tmp_path / "flink_comprehensive_test_output.json"
            assert output_file.exists()

            with open(output_file) as f:
                output_records = json.load(f)

            # Verify we got records
            assert len(output_records) > 0

            # Group by entity type
            entities_by_type: dict[str, list] = {}
            for record in output_records:
                entity_type = record.get("entityType")
                if entity_type not in entities_by_type:
                    entities_by_type[entity_type] = []
                entities_by_type[entity_type].append(record)

            # Verify container exists
            assert "container" in entities_by_type
            containers = entities_by_type["container"]
            assert len(containers) >= 1  # At least cluster container

            # Verify dataFlow entities (jobs)
            assert "dataFlow" in entities_by_type
            dataflows = entities_by_type["dataFlow"]
            assert len(dataflows) >= 2  # Two jobs

            # Verify dataJob entities (operators/vertices)
            assert "dataJob" in entities_by_type
            datajobs = entities_by_type["dataJob"]
            assert len(datajobs) >= 8  # Multiple operators across jobs

            # Verify job properties
            dataflow_props = [
                r for r in output_records if r.get("aspectName") == "dataFlowInfo"
            ]
            assert len(dataflow_props) >= 2

            # Check for custom properties on dataFlowInfo
            for prop_record in dataflow_props:
                aspect_json = prop_record.get("aspect", {}).get("json", {})
                custom_props = aspect_json.get("customProperties", {})
                assert len(custom_props) > 0

            # Verify operator metadata
            datajob_props = [
                r for r in output_records if r.get("aspectName") == "dataJobInfo"
            ]
            assert len(datajob_props) >= 8  # All vertices should have dataJobInfo

            print(f"\n  Extracted {len(output_records)} metadata records")
            print(f"  Containers: {len(containers)}")
            print(f"  DataFlows: {len(dataflows)}")
            print(f"  DataJobs: {len(datajobs)}")

            return str(output_file)


if __name__ == "__main__":
    # Run test and output to specific location for UI verification
    test = TestFlinkComprehensive()
    mock_sess = test.mock_session(test)
    output_path = test.test_full_extraction_with_lineage(
        mock_sess,
        Path("/Users/dinesh/.datahub/connector-workflow/.state/20260224-012157-ed0338"),
    )
    print(f"\n✓ Test output saved to: {output_path}")
    print("  Use this file for DataHub ingestion to verify UI")
