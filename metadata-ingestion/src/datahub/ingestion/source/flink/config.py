from typing import Optional

import pydantic
from pydantic import Field, model_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)


class FlinkConnectionConfig(ConfigModel):
    """Connection configuration for Flink REST APIs."""

    rest_api_url: str = Field(
        description=(
            "JobManager REST API endpoint (e.g., http://localhost:8081). "
            "This is the primary endpoint for extracting job and cluster metadata."
        )
    )

    sql_gateway_url: Optional[str] = Field(
        default=None,
        description=(
            "SQL Gateway REST API endpoint (e.g., http://localhost:8083). "
            "Required only when include_catalog_metadata is enabled. "
            "Used to extract Flink catalog metadata (databases, tables, schemas)."
        ),
    )

    username: Optional[str] = Field(
        default=None,
        description=(
            "Username for HTTP basic authentication. "
            "Optional - only needed if Flink cluster has authentication enabled."
        ),
    )

    password: Optional[pydantic.SecretStr] = Field(
        default=None,
        description=(
            "Password for HTTP basic authentication. "
            "Optional - only needed if Flink cluster has authentication enabled."
        ),
    )

    timeout_seconds: int = Field(
        default=30,
        description=(
            "HTTP request timeout in seconds. "
            "Increase this value if API requests frequently timeout on large clusters."
        ),
    )

    max_retries: int = Field(
        default=3,
        description=(
            "Maximum number of retry attempts for failed HTTP requests. "
            "Uses exponential backoff between retries."
        ),
    )

    verify_ssl: bool = Field(
        default=True,
        description=(
            "Verify SSL certificates for HTTPS connections. "
            "Set to False only for testing with self-signed certificates."
        ),
    )


class FlinkSourceConfig(
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
):
    """Source configuration for Flink connector."""

    connection: FlinkConnectionConfig = Field(
        description="Flink REST API connection configuration"
    )

    job_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description=(
            "Regex patterns to filter Flink jobs by name. "
            "Supports allow/deny patterns for fine-grained job selection. "
            "Example: {'allow': ['^prod_.*'], 'deny': ['.*_test$']}"
        ),
    )

    catalog_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description=(
            "Regex patterns to filter catalog databases. "
            "Only applies when include_catalog_metadata is enabled. "
            "Useful for excluding internal or system catalogs."
        ),
    )

    include_job_lineage: bool = Field(
        default=True,
        description=(
            "Extract lineage from job configuration (sources → job → sinks). "
            "Infers table-level lineage from Flink connector configurations. "
            "Supports Kafka, JDBC, and filesystem connectors. "
            "Critical for understanding data flow through Flink pipelines."
        ),
    )

    include_catalog_metadata: bool = Field(
        default=False,
        description=(
            "Extract catalog metadata via SQL Gateway REST API. "
            "Requires sql_gateway_url to be configured in connection settings. "
            "Extracts tables from Flink catalogs (in-memory, Hive Metastore, JDBC catalogs). "
            "Note: In-memory catalogs are session-specific and may not persist."
        ),
    )

    include_checkpoint_metadata: bool = Field(
        default=True,
        description=(
            "Include checkpoint and savepoint metadata as job properties. "
            "Provides operational visibility into job state (checkpoint size, duration, intervals). "
            "Useful for monitoring job health and performance."
        ),
    )

    include_job_exceptions: bool = Field(
        default=True,
        description=(
            "Track job exceptions and failures. "
            "Stored as operation aspects for monitoring and debugging. "
            "Helps identify problematic jobs and failure patterns."
        ),
    )

    cluster_name: str = Field(
        default="default",
        description=(
            "Human-readable cluster name for container identification. "
            "Used when multiple Flink clusters ingest to same DataHub instance. "
            "Helps distinguish jobs and metadata from different clusters."
        ),
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description=(
            "Stateful ingestion configuration for automatic removal of stopped/deleted jobs. "
            "Enables deletion detection and cleanup of stale metadata. "
            "Ensures DataHub reflects current state of Flink cluster."
        ),
    )

    @model_validator(mode="after")  # type: ignore[untyped-decorator]
    def validate_catalog_integration(self) -> "FlinkSourceConfig":
        """Validate catalog integration configuration."""
        if self.include_catalog_metadata and not self.connection.sql_gateway_url:
            raise ValueError(
                "connection.sql_gateway_url must be configured when "
                "include_catalog_metadata is enabled. "
                "The SQL Gateway REST API is required for catalog extraction."
            )
        return self
