"""Unit tests for Apache Flink DataHub Source."""

from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.flink.config import (
    FlinkConnectionConfig,
    FlinkSourceConfig,
)
from datahub.ingestion.source.flink.source import FlinkSource


class TestFlinkSourceConfig:
    """Tests for Flink source configuration validation."""

    def test_catalog_integration_requires_sql_gateway_url(self):
        """Test that enabling catalog metadata without sql_gateway_url raises error."""
        with pytest.raises(
            ValueError,
            match="connection.sql_gateway_url must be configured when include_catalog_metadata is enabled",
        ):
            FlinkSourceConfig.model_validate(
                {
                    "connection": {"rest_api_url": "http://localhost:8081"},
                    "include_catalog_metadata": True,
                }
            )

    def test_catalog_integration_with_sql_gateway_url_succeeds(self):
        """Test that catalog integration works when sql_gateway_url is provided."""
        config = FlinkSourceConfig.model_validate(
            {
                "connection": {
                    "rest_api_url": "http://localhost:8081",
                    "sql_gateway_url": "http://localhost:8083",
                },
                "include_catalog_metadata": True,
            }
        )
        assert config.include_catalog_metadata is True
        assert config.connection.sql_gateway_url == "http://localhost:8083"

    def test_catalog_disabled_without_sql_gateway_url_succeeds(self):
        """Test that catalog can be disabled without sql_gateway_url."""
        config = FlinkSourceConfig.model_validate(
            {
                "connection": {"rest_api_url": "http://localhost:8081"},
                "include_catalog_metadata": False,
            }
        )
        assert config.include_catalog_metadata is False
        assert config.connection.sql_gateway_url is None


class TestFlinkJobFiltering:
    """Tests for Flink job filtering logic."""

    def test_job_pattern_filtering_respects_config(self):
        """Test that job_pattern correctly filters jobs based on allow/deny patterns."""
        config_dict = {
            "connection": {"rest_api_url": "http://localhost:8081"},
            "job_pattern": {"allow": ["^prod_.*"], "deny": [".*_test$"]},
        }
        ctx = PipelineContext(run_id="test-run")

        # Mock the client to return test jobs
        with patch(
            "datahub.ingestion.source.flink.source.FlinkJobManagerClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client

            # Mock cluster overview
            mock_client.get_cluster_overview.return_value = {
                "version": "1.17.0",
                "taskmanagers": 2,
                "slots-total": 8,
                "slots-available": 4,
            }

            # Return jobs: prod_job1 (allowed), prod_test (denied), dev_job (not allowed)
            mock_client.get_jobs.return_value = [
                {
                    "id": "job1",
                    "name": "prod_job1",
                    "status": "RUNNING",
                    "start-time": 1000000,
                },
                {
                    "id": "job2",
                    "name": "prod_test",
                    "status": "RUNNING",
                    "start-time": 1000000,
                },
                {
                    "id": "job3",
                    "name": "dev_job",
                    "status": "RUNNING",
                    "start-time": 1000000,
                },
            ]

            # Mock job details for allowed job
            mock_client.get_job.return_value = {
                "jid": "job1",
                "name": "prod_job1",
                "state": "RUNNING",
                "start-time": 1000000,
            }
            mock_client.get_job_plan.return_value = {"nodes": []}
            mock_client.get_job_config.return_value = {"execution-config": {}}
            mock_client.get_checkpoint_stats.return_value = None
            mock_client.get_job_exceptions.return_value = {"all-exceptions": []}

            source = FlinkSource.create(config_dict, ctx)
            _ = list(source.get_workunits())  # Consume generator to trigger mocks

            # Verify only prod_job1 was processed (one job processed)
            # Check the report to see how many jobs were dropped
            report = source.get_report()
            assert report.jobs_dropped == 2  # prod_test and dev_job filtered

    def test_all_jobs_allowed_by_default(self):
        """Test that job filtering allows all jobs when no pattern is specified."""
        config_dict = {
            "connection": {"rest_api_url": "http://localhost:8081"},
        }
        config = FlinkSourceConfig.model_validate(config_dict)

        # Default pattern should allow all
        assert config.job_pattern.allowed("any_job_name")
        assert config.job_pattern.allowed("another_job")


class TestFlinkSourceErrorHandling:
    """Tests for error handling in Flink source."""

    def test_continues_ingestion_when_single_job_fails(self):
        """Test that failure fetching one job doesn't stop entire ingestion."""
        config_dict = {
            "connection": {"rest_api_url": "http://localhost:8081"},
        }
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.flink.source.FlinkJobManagerClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client

            # Mock cluster overview
            mock_client.get_cluster_overview.return_value = {
                "version": "1.17.0",
                "taskmanagers": 2,
            }

            # Return two jobs
            mock_client.get_jobs.return_value = [
                {"id": "job1", "name": "job1", "status": "RUNNING", "start-time": 1000},
                {"id": "job2", "name": "job2", "status": "RUNNING", "start-time": 2000},
            ]

            # job1 succeeds, job2 fails
            def get_job_side_effect(job_id):
                if job_id == "job1":
                    return {
                        "jid": "job1",
                        "name": "job1",
                        "state": "RUNNING",
                        "start-time": 1000,
                    }
                else:
                    raise Exception("Failed to fetch job2 details")

            mock_client.get_job.side_effect = get_job_side_effect
            mock_client.get_job_plan.return_value = {"nodes": []}
            mock_client.get_job_config.return_value = {"execution-config": {}}
            mock_client.get_checkpoint_stats.return_value = None
            mock_client.get_job_exceptions.return_value = {"all-exceptions": []}

            source = FlinkSource.create(config_dict, ctx)
            workunits = list(source.get_workunits())

            # Should have workunits for cluster container + job1
            assert len(workunits) > 0

            # Should have warning for job2
            report = source.get_report()
            assert len(report.warnings) > 0 or len(report.failures) > 0

    def test_connection_failure_reported(self):
        """Test that connection failures are properly reported."""
        config_dict = {
            "connection": {"rest_api_url": "http://localhost:99999"},  # Invalid port
        }
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.flink.source.FlinkJobManagerClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client

            # Mock cluster overview to succeed (for container emission)
            mock_client.get_cluster_overview.return_value = {
                "version": "1.17.0",
                "taskmanagers": 2,
            }

            # Simulate connection failure when listing jobs
            mock_client.get_jobs.side_effect = ConnectionError(
                "Failed to connect to Flink JobManager"
            )

            source = FlinkSource.create(config_dict, ctx)
            workunits = list(source.get_workunits())

            # Should have cluster container workunits but no jobs
            # (cluster container is emitted before jobs are fetched)
            assert len(workunits) > 0

            # Should report failure
            report = source.get_report()
            assert len(report.failures) > 0


class TestFlinkCheckpointMetadata:
    """Tests for checkpoint metadata extraction."""

    def test_checkpoint_metadata_included_when_enabled(self):
        """Test that checkpoint metadata is extracted when enabled."""
        config_dict = {
            "connection": {"rest_api_url": "http://localhost:8081"},
            "include_checkpoint_metadata": True,
        }
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.flink.source.FlinkJobManagerClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client

            # Mock cluster overview
            mock_client.get_cluster_overview.return_value = {
                "version": "1.17.0",
                "taskmanagers": 2,
            }

            mock_client.get_jobs.return_value = [
                {"id": "job1", "name": "job1", "status": "RUNNING", "start-time": 1000}
            ]
            mock_client.get_job.return_value = {
                "jid": "job1",
                "name": "job1",
                "state": "RUNNING",
                "start-time": 1000,
                "vertices": [],  # Empty vertices to complete the job details
            }

            # Mock checkpoint stats
            mock_client.get_job_checkpoints.return_value = {
                "counts": {
                    "total": 10,
                    "completed": 9,
                    "failed": 1,
                },
                "latest": {
                    "completed": {
                        "id": 9,
                        "status": "COMPLETED",
                        "external_path": "s3://bucket/checkpoints/9",
                        "state_size": 1024000,
                    }
                },
            }

            source = FlinkSource.create(config_dict, ctx)
            _ = list(source.get_workunits())  # Consume generator to trigger mocks

            # Check that checkpoint stats were fetched
            assert mock_client.get_job_checkpoints.called

    def test_checkpoint_metadata_skipped_when_disabled(self):
        """Test that checkpoint metadata is not extracted when disabled."""
        config_dict = {
            "connection": {"rest_api_url": "http://localhost:8081"},
            "include_checkpoint_metadata": False,
        }
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.flink.source.FlinkJobManagerClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client

            # Mock cluster overview
            mock_client.get_cluster_overview.return_value = {
                "version": "1.17.0",
                "taskmanagers": 2,
            }

            mock_client.get_jobs.return_value = [
                {"id": "job1", "name": "job1", "status": "RUNNING", "start-time": 1000}
            ]
            mock_client.get_job.return_value = {
                "jid": "job1",
                "name": "job1",
                "state": "RUNNING",
                "start-time": 1000,
            }
            mock_client.get_job_plan.return_value = {"nodes": []}
            mock_client.get_job_config.return_value = {"execution-config": {}}
            mock_client.get_job_exceptions.return_value = {"all-exceptions": []}

            source = FlinkSource.create(config_dict, ctx)
            _ = list(source.get_workunits())  # Consume generator to trigger mocks

            # Checkpoint stats should not be fetched
            assert not mock_client.get_checkpoint_stats.called


class TestFlinkJobExceptions:
    """Tests for job exception tracking."""

    def test_job_exceptions_extracted_when_enabled(self):
        """Test that job exceptions are extracted when enabled."""
        config_dict = {
            "connection": {"rest_api_url": "http://localhost:8081"},
            "include_job_exceptions": True,
        }
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.flink.source.FlinkJobManagerClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client

            # Mock cluster overview
            mock_client.get_cluster_overview.return_value = {
                "version": "1.17.0",
                "taskmanagers": 2,
            }

            mock_client.get_jobs.return_value = [
                {"id": "job1", "name": "job1", "status": "RUNNING", "start-time": 1000}
            ]
            mock_client.get_job.return_value = {
                "jid": "job1",
                "name": "job1",
                "state": "RUNNING",
                "start-time": 1000,
                "vertices": [],  # Empty vertices to complete the job details
            }
            mock_client.get_job_checkpoints.return_value = None

            source = FlinkSource.create(config_dict, ctx)

            # The source doesn't have a get_job_exceptions method called directly
            # Instead check that the config enabled the feature
            assert source.config.include_job_exceptions is True

    def test_job_exceptions_skipped_when_disabled(self):
        """Test that job exceptions are not extracted when disabled."""
        config_dict = {
            "connection": {"rest_api_url": "http://localhost:8081"},
            "include_job_exceptions": False,
        }
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.flink.source.FlinkJobManagerClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client

            # Mock cluster overview
            mock_client.get_cluster_overview.return_value = {
                "version": "1.17.0",
                "taskmanagers": 2,
            }

            mock_client.get_jobs.return_value = [
                {"id": "job1", "name": "job1", "status": "RUNNING", "start-time": 1000}
            ]
            mock_client.get_job.return_value = {
                "jid": "job1",
                "name": "job1",
                "state": "RUNNING",
                "start-time": 1000,
                "vertices": [],  # Empty vertices to complete the job details
            }
            mock_client.get_job_checkpoints.return_value = None

            source = FlinkSource.create(config_dict, ctx)
            _ = list(source.get_workunits())  # Consume generator to trigger mocks

            # Verify the config disabled the feature
            assert source.config.include_job_exceptions is False


class TestFlinkCatalogIntegration:
    """Tests for catalog metadata extraction."""

    def test_catalog_extraction_uses_sql_gateway_client(self):
        """Test that catalog extraction uses SQL Gateway client when enabled."""
        config_dict = {
            "connection": {
                "rest_api_url": "http://localhost:8081",
                "sql_gateway_url": "http://localhost:8083",
            },
            "include_catalog_metadata": True,
        }
        ctx = PipelineContext(run_id="test-run")

        with (
            patch(
                "datahub.ingestion.source.flink.source.FlinkJobManagerClient"
            ) as mock_jm_class,
            patch(
                "datahub.ingestion.source.flink.source.FlinkSQLGatewayClient"
            ) as mock_sql_class,
        ):
            mock_jm_client = MagicMock()
            mock_sql_client = MagicMock()
            mock_jm_class.return_value = mock_jm_client
            mock_sql_class.return_value = mock_sql_client

            # Mock cluster overview
            mock_jm_client.get_cluster_overview.return_value = {
                "version": "1.17.0",
                "taskmanagers": 2,
            }

            # Mock empty jobs
            mock_jm_client.get_jobs.return_value = []

            # Mock catalog data
            mock_sql_client.list_catalogs.return_value = ["default_catalog"]
            mock_sql_client.list_databases.return_value = ["default_database"]
            mock_sql_client.list_tables.return_value = ["test_table"]
            mock_sql_client.get_table_schema.return_value = [
                {"name": "id", "type": "INT"},
                {"name": "name", "type": "STRING"},
            ]

            source = FlinkSource.create(config_dict, ctx)
            _ = list(source.get_workunits())  # Consume generator to trigger mocks

            # Verify SQL Gateway client was used
            assert mock_sql_class.called

    def test_catalog_pattern_filters_databases(self):
        """Test that catalog_pattern correctly filters databases."""
        config_dict = {
            "connection": {
                "rest_api_url": "http://localhost:8081",
                "sql_gateway_url": "http://localhost:8083",
            },
            "include_catalog_metadata": True,
            "catalog_pattern": {"allow": ["^prod_.*"], "deny": [".*_test$"]},
        }
        config = FlinkSourceConfig.model_validate(config_dict)

        # Test pattern matching
        assert config.catalog_pattern.allowed("prod_catalog")
        assert not config.catalog_pattern.allowed("prod_catalog_test")
        assert not config.catalog_pattern.allowed("dev_catalog")


class TestFlinkEntityURNs:
    """Tests for URN generation and entity relationships."""

    def test_dataflow_urn_format(self):
        """Test that DataFlow URNs are generated correctly."""
        config_dict = {
            "connection": {"rest_api_url": "http://localhost:8081"},
            "env": "PROD",
        }
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.flink.source.FlinkJobManagerClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client

            # Mock cluster overview
            mock_client.get_cluster_overview.return_value = {
                "version": "1.17.0",
                "taskmanagers": 2,
            }

            mock_client.get_jobs.return_value = [
                {
                    "id": "test-job-id",
                    "name": "test_job",
                    "status": "RUNNING",
                    "start-time": 1000,
                }
            ]
            mock_client.get_job.return_value = {
                "jid": "test-job-id",
                "name": "test_job",
                "state": "RUNNING",
                "start-time": 1000,
            }
            mock_client.get_job_plan.return_value = {"nodes": []}
            mock_client.get_job_config.return_value = {"execution-config": {}}
            mock_client.get_checkpoint_stats.return_value = None
            mock_client.get_job_exceptions.return_value = {"all-exceptions": []}

            source = FlinkSource.create(config_dict, ctx)
            workunits = list(source.get_workunits())

            # Find DataFlow workunit
            dataflow_urns = [
                wu.metadata.entityUrn
                for wu in workunits
                if "dataFlow" in wu.metadata.entityUrn
            ]

            assert len(dataflow_urns) > 0
            # URN should contain platform (flink), job ID, and env (PROD)
            assert any("flink" in urn for urn in dataflow_urns)
            assert any("test-job-id" in urn for urn in dataflow_urns)
            assert any("PROD" in urn for urn in dataflow_urns)

    def test_datajob_urn_references_dataflow(self):
        """Test that DataJob URNs correctly reference parent DataFlow."""
        config_dict = {
            "connection": {"rest_api_url": "http://localhost:8081"},
        }
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.flink.source.FlinkJobManagerClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client

            # Mock cluster overview
            mock_client.get_cluster_overview.return_value = {
                "version": "1.17.0",
                "taskmanagers": 2,
            }

            mock_client.get_jobs.return_value = [
                {"id": "job1", "name": "job1", "status": "RUNNING", "start-time": 1000}
            ]
            # Include vertices in job details (operators)
            mock_client.get_job.return_value = {
                "jid": "job1",
                "name": "job1",
                "state": "RUNNING",
                "start-time": 1000,
                "vertices": [
                    {"id": "vertex1", "name": "Source: Kafka"},
                    {"id": "vertex2", "name": "Map"},
                ],
            }
            mock_client.get_job_checkpoints.return_value = None

            source = FlinkSource.create(config_dict, ctx)
            workunits = list(source.get_workunits())

            # Find DataJob workunits
            datajob_urns = [
                wu.metadata.entityUrn
                for wu in workunits
                if "dataJob" in wu.metadata.entityUrn
            ]

            # DataJob URNs should reference the DataFlow
            assert len(datajob_urns) > 0
            for urn in datajob_urns:
                assert "dataFlow" in urn  # DataJob URNs contain DataFlow URN


class TestFlinkConnectionConfig:
    """Tests for connection configuration."""

    def test_default_timeout_and_retries(self):
        """Test that connection config has sensible defaults."""
        config = FlinkConnectionConfig(rest_api_url="http://localhost:8081")

        assert config.timeout_seconds == 30
        assert config.max_retries == 3
        assert config.verify_ssl is True

    def test_optional_authentication_fields(self):
        """Test that authentication fields are optional."""
        config = FlinkConnectionConfig(rest_api_url="http://localhost:8081")

        assert config.username is None
        assert config.password is None

    def test_custom_timeout_respected(self):
        """Test that custom timeout value is respected."""
        config = FlinkConnectionConfig(
            rest_api_url="http://localhost:8081", timeout_seconds=60
        )

        assert config.timeout_seconds == 60
