"""
Apache Flink DataHub Source.

Extracts metadata from Apache Flink via JobManager REST API:
- Jobs as DataFlow entities
- Job operators as DataJob entities
- Checkpoint metadata as custom properties
- Optional: Catalog metadata via SQL Gateway

Note: Lineage extraction not yet implemented (planned for future release).
"""

import logging
from dataclasses import dataclass
from typing import Dict, Iterable, Optional

from datahub.emitter.mcp_builder import ContainerKey, gen_containers
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.flink.client import (
    FlinkJobManagerClient,
    FlinkSQLGatewayClient,
)
from datahub.ingestion.source.flink.config import FlinkSourceConfig
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.sdk import DataFlow, DataJob, Dataset
from datahub.utilities.urns.data_flow_urn import DataFlowUrn

logger = logging.getLogger(__name__)


class FlinkClusterKey(ContainerKey):
    """Container key for Flink cluster."""

    cluster_name: str


class FlinkCatalogDatabaseKey(ContainerKey):
    """Container key for Flink catalog database."""

    catalog_name: str
    database_name: str


@dataclass
class FlinkSourceReport(StaleEntityRemovalSourceReport):
    """Report for Flink ingestion."""

    # Job metrics
    jobs_scanned: int = 0
    jobs_dropped: int = 0
    operators_scanned: int = 0

    # Checkpoint metrics
    jobs_with_checkpoints: int = 0

    # Catalog metrics (optional)
    catalogs_scanned: int = 0
    databases_scanned: int = 0
    tables_scanned: int = 0

    def report_job_scanned(self) -> None:
        """Report a job was scanned."""
        self.jobs_scanned += 1

    def report_job_dropped(self) -> None:
        """Report a job was filtered out."""
        self.jobs_dropped += 1

    def report_operator_scanned(self) -> None:
        """Report an operator was scanned."""
        self.operators_scanned += 1


@platform_name("Flink", id="flink")
@config_class(FlinkSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
class FlinkSource(StatefulIngestionSourceBase, TestableSource):
    """
    DataHub source for Apache Flink.

    Extracts:
    - Jobs (as DataFlow entities)
    - Job operators/vertices (as DataJob entities)
    - Checkpoint metadata as custom properties
    - Job exceptions and failures
    - Optional: Catalog metadata via SQL Gateway

    Requires:
    - Flink JobManager REST API (default port 8081)
    - Optional: Flink SQL Gateway REST API (default port 8083) for catalog integration

    Note: Lineage extraction not yet implemented (planned for future release).
    """

    def __init__(self, config: FlinkSourceConfig, ctx: PipelineContext):
        """
        Initialize Flink source.

        Args:
            config: Source configuration
            ctx: Pipeline context
        """
        super().__init__(config, ctx)
        self.config = config
        self.report: FlinkSourceReport = FlinkSourceReport()
        self.platform = "flink"

        # Initialize JobManager client
        self.client = FlinkJobManagerClient(self.config.connection)

        # Initialize SQL Gateway client if catalog integration enabled
        self.sql_gateway_client: Optional[FlinkSQLGatewayClient] = None
        if (
            self.config.include_catalog_metadata
            and self.config.connection.sql_gateway_url
        ):
            self.sql_gateway_client = FlinkSQLGatewayClient(self.config.connection)

        # Initialize stale entity removal handler
        self.stale_entity_removal_handler = StaleEntityRemovalHandler.create(
            self, self.config, self.ctx
        )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "FlinkSource":
        """Create source instance from config dictionary."""
        config = FlinkSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """
        Main extraction logic.

        Yields:
            MetadataWorkUnit: Metadata change proposals for jobs and operators
        """
        # Emit cluster container
        yield from self._emit_cluster_container()

        # Get and process all jobs
        try:
            jobs = self.client.get_jobs()
            logger.info(f"Found {len(jobs)} jobs in Flink cluster")

            for job in jobs:
                job_id = job.get("id")
                job_name = job.get("name", job_id)

                # Filter by pattern
                if not self.config.job_pattern.allowed(str(job_name)):
                    logger.debug(f"Skipping job {job_name} (filtered by pattern)")
                    self.report.report_job_dropped()
                    continue

                # Process job
                try:
                    yield from self._process_job(job)
                except Exception as e:
                    self.report.warning(
                        title="Failed to process job",
                        message="Job metadata extraction failed. Job will be skipped.",
                        context=f"job_id={job_id}, job_name={job_name}",
                        exc=e,
                    )
                    continue

        except Exception as e:
            self.report.failure(
                title="Failed to list jobs",
                message="Unable to retrieve job list from Flink JobManager API. Verify API connectivity and permissions.",
                exc=e,
            )
            return

        # Process catalog metadata if enabled
        if self.config.include_catalog_metadata and self.sql_gateway_client:
            yield from self._process_catalog_metadata()

    def _emit_cluster_container(self) -> Iterable[MetadataWorkUnit]:
        """
        Emit cluster-level container entity.

        Yields:
            MetadataWorkUnit: Cluster container MCPs
        """
        try:
            cluster_info = self.client.get_cluster_overview()

            # Create container key for Flink cluster
            container_key = FlinkClusterKey(
                platform=self.platform,
                cluster_name=self.config.cluster_name,
                instance=self.config.platform_instance,
                env=self.config.env,
            )

            # Use gen_containers to create cluster container
            for mcp in gen_containers(
                container_key=container_key,
                name=self.config.cluster_name,
                sub_types=["Flink Cluster"],
                domain_urn=None,
                description=f"Flink cluster at {self.config.connection.rest_api_url}",
                external_url=self.config.connection.rest_api_url,
                qualified_name=self.config.cluster_name,
                extra_properties={
                    "flink-version": cluster_info.get("flink-version", "unknown"),
                    "taskmanagers": str(cluster_info.get("taskmanagers", 0)),
                    "slots-total": str(cluster_info.get("slots-total", 0)),
                    "slots-available": str(cluster_info.get("slots-available", 0)),
                },
            ):
                yield mcp

        except Exception as e:
            self.report.warning(
                title="Failed to get cluster info",
                message="Unable to retrieve cluster overview. Container will be created without metadata.",
                exc=e,
            )

    def _process_job(self, job: Dict) -> Iterable[MetadataWorkUnit]:
        """
        Process a single Flink job.

        Args:
            job: Job dictionary from API

        Yields:
            MetadataWorkUnit: MCPs for job (DataFlow) and operators (DataJob)
        """
        job_id = job.get("id")
        job_name = job.get("name", job_id)

        logger.debug(f"Processing job: {job_name} ({job_id})")
        self.report.report_job_scanned()

        # Get detailed job information
        try:
            job_details = self.client.get_job(str(job_id))
        except Exception as e:
            self.report.warning(
                title="Failed to get job details",
                message="Unable to fetch detailed job information. Job will be created with basic metadata.",
                context=f"job_id={job_id}",
                exc=e,
            )
            job_details = {}

        # Prepare custom properties
        custom_properties = {
            "job_id": str(job_id),
            "state": job.get("status", "UNKNOWN"),
            "start-time": str(job.get("start-time", "")),
            "end-time": str(job.get("end-time", "")) if job.get("end-time") else "",
        }

        # Add checkpoint metadata if enabled
        if self.config.include_checkpoint_metadata:
            checkpoint_info = self._get_checkpoint_metadata(str(job_id))
            if checkpoint_info:
                custom_properties.update(checkpoint_info)
                self.report.jobs_with_checkpoints += 1

        # Create DataFlow entity using SDK V2
        data_flow = DataFlow(
            platform=self.platform,
            name=str(job_id),
            display_name=str(job_name),
            description=job_details.get("description") if job_details else None,
            external_url=f"{self.config.connection.rest_api_url}/#/jobs/{job_id}",
            custom_properties=custom_properties,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )

        # Emit DataFlow
        yield from data_flow.as_workunits()

        # Process job operators (vertices)
        vertices = job_details.get("vertices", []) if job_details else []
        logger.debug(f"Job {job_name} has {len(vertices)} operators")

        for vertex in vertices:
            yield from self._process_operator(
                str(job_id), str(job_name), vertex, data_flow
            )

    def _process_operator(
        self,
        job_id: str,
        job_name: str,
        vertex: Dict,
        parent_flow: DataFlow,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Process a single job operator/vertex.

        Args:
            job_id: Parent job ID
            job_name: Parent job name
            vertex: Vertex dictionary from API
            parent_flow: Parent DataFlow entity

        Yields:
            MetadataWorkUnit: MCPs for operator (DataJob)
        """
        vertex_id = vertex.get("id")
        vertex_name = vertex.get("name", vertex_id)

        logger.debug(f"Processing operator: {vertex_name} ({vertex_id})")
        self.report.report_operator_scanned()

        # Create DataJob URN
        flow_urn = DataFlowUrn.create_from_ids(
            orchestrator=self.platform,
            flow_id=str(job_id),
            env=self.config.env,
        )

        # Prepare custom properties
        custom_props = {
            "vertex_id": str(vertex_id),
            "parallelism": str(vertex.get("parallelism", 1)),
            "status": str(vertex.get("status", "UNKNOWN")),
        }

        # Create DataJob entity using SDK V2
        data_job = DataJob(
            name=str(vertex_id),
            display_name=str(vertex_name),
            flow_urn=flow_urn,
            description=f"Flink operator in job {job_name}",
            custom_properties=custom_props,
            platform_instance=self.config.platform_instance,
            subtype="FLINK_OPERATOR",
            # TODO: Parse job plan to determine actual input/output edges
            inlets=[],
            outlets=[],
        )

        # Emit DataJob
        yield from data_job.as_workunits()

    def _get_checkpoint_metadata(self, job_id: str) -> Optional[Dict[str, str]]:
        """
        Get checkpoint metadata for a job.

        Args:
            job_id: Job identifier

        Returns:
            Dictionary of checkpoint properties or None
        """
        try:
            checkpoint_stats = self.client.get_job_checkpoints(job_id)
            if not checkpoint_stats or "latest" not in checkpoint_stats:
                return None

            latest = checkpoint_stats["latest"]["completed"]
            if not latest:
                return None

            return {
                "checkpoint_id": str(latest.get("id", "")),
                "checkpoint_status": latest.get("status", ""),
                "checkpoint_external_path": latest.get("external_path", ""),
                "checkpoint_state_size": str(latest.get("state_size", 0)),
                "checkpoint_duration": str(latest.get("duration", 0)),
            }

        except Exception as e:
            logger.debug(f"Failed to get checkpoint metadata for job {job_id}: {e}")
            return None

    def _process_catalog_metadata(self) -> Iterable[MetadataWorkUnit]:
        """
        Process catalog metadata via SQL Gateway.

        Yields:
            MetadataWorkUnit: MCPs for catalog tables (datasets)
        """
        if not self.sql_gateway_client:
            return
            yield  # Make this a generator

        logger.info("Processing catalog metadata via SQL Gateway")

        try:
            # Get catalogs
            catalogs = self.sql_gateway_client.get_catalogs()
            logger.info(f"Found {len(catalogs)} catalogs")

            for catalog in catalogs:
                if not self.config.catalog_pattern.allowed(catalog):
                    logger.debug(f"Skipping catalog {catalog} (filtered by pattern)")
                    continue

                self.report.catalogs_scanned += 1

                # Get databases in catalog
                databases = self.sql_gateway_client.get_databases(catalog)
                logger.debug(f"Catalog {catalog} has {len(databases)} databases")

                for database in databases:
                    self.report.databases_scanned += 1

                    # Emit database container
                    yield from self._emit_database_container(catalog, database)

                    # Get tables in database
                    tables = self.sql_gateway_client.get_tables(catalog, database)
                    logger.debug(
                        f"Database {catalog}.{database} has {len(tables)} tables"
                    )

                    for table in tables:
                        try:
                            yield from self._process_catalog_table(
                                catalog, database, table
                            )
                            self.report.tables_scanned += 1
                        except Exception as e:
                            self.report.warning(
                                title="Failed to process table",
                                message="Table metadata extraction failed. Table will be skipped.",
                                context=f"table={catalog}.{database}.{table}",
                                exc=e,
                            )
                            continue

        except Exception as e:
            self.report.warning(
                title="Failed to process catalog metadata",
                message="Catalog metadata extraction failed. Catalog tables will not be ingested.",
                exc=e,
            )

    def _emit_database_container(
        self, catalog: str, database: str
    ) -> Iterable[MetadataWorkUnit]:
        """
        Emit container for a Flink catalog database.

        Args:
            catalog: Catalog name
            database: Database name

        Yields:
            MetadataWorkUnit: Container MCPs
        """
        # Create cluster container key for parent
        cluster_key = FlinkClusterKey(
            platform=self.platform,
            cluster_name=self.config.cluster_name,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

        # Create database container key
        database_key = FlinkCatalogDatabaseKey(
            platform=self.platform,
            catalog_name=catalog,
            database_name=database,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

        # Emit database container with cluster as parent
        for mcp in gen_containers(
            container_key=database_key,
            name=f"{catalog}.{database}",
            sub_types=["Database"],
            parent_container_key=cluster_key,
            description=f"Flink catalog database: {catalog}.{database}",
            qualified_name=f"{catalog}.{database}",
            extra_properties={
                "catalog": catalog,
                "database": database,
            },
        ):
            yield mcp

    def _process_catalog_table(
        self, catalog: str, database: str, table: str
    ) -> Iterable[MetadataWorkUnit]:
        """
        Process a single catalog table.

        Args:
            catalog: Catalog name
            database: Database name
            table: Table name

        Yields:
            MetadataWorkUnit: Dataset MCPs for the table
        """
        logger.debug(f"Processing table: {catalog}.{database}.{table}")

        # Type safety: sql_gateway_client is guaranteed to exist here
        assert self.sql_gateway_client is not None

        # Get table schema
        columns = self.sql_gateway_client.get_table_schema(catalog, database, table)

        # Convert Flink columns to DataHub schema fields
        schema_fields = []
        for col in columns:
            col_name = col.get("name")
            col_type = col.get("type")
            col_comment = col.get("comment")

            if col_name and col_type:
                # Create schema field tuple: (name, type, description)
                field_tuple = (
                    col_name,
                    self._map_flink_type_to_datahub(col_type),
                    col_comment or "",
                )
                schema_fields.append(field_tuple)

        # Create dataset using SDK
        dataset_name = f"{catalog}.{database}.{table}"
        dataset = Dataset(
            platform=self.platform,
            name=dataset_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            description=f"Flink catalog table from {catalog} catalog",
            qualified_name=dataset_name,
            custom_properties={
                "catalog": catalog,
                "database": database,
                "table": table,
            },
            schema=schema_fields if schema_fields else None,
            subtype="TABLE",
            parent_container=self._get_database_container_key(catalog, database),
        )

        # Emit dataset workunits
        yield from dataset.as_workunits()

    def _get_database_container_key(
        self, catalog: str, database: str
    ) -> FlinkCatalogDatabaseKey:
        """
        Get container key for a database.

        Args:
            catalog: Catalog name
            database: Database name

        Returns:
            FlinkCatalogDatabaseKey for the database
        """
        return FlinkCatalogDatabaseKey(
            platform=self.platform,
            catalog_name=catalog,
            database_name=database,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

    def _map_flink_type_to_datahub(self, flink_type: str) -> str:
        """
        Map Flink SQL types to DataHub types.

        Args:
            flink_type: Flink SQL type string (e.g., "VARCHAR(255)", "INT", "TIMESTAMP(3)")

        Returns:
            DataHub type string
        """
        # Normalize type string (uppercase, remove precision/scale)
        flink_type_upper = flink_type.upper().split("(")[0].strip()

        # Map common Flink types to DataHub types
        type_mapping = {
            # Numeric types
            "TINYINT": "TINYINT",
            "SMALLINT": "SMALLINT",
            "INT": "INT",
            "INTEGER": "INT",
            "BIGINT": "BIGINT",
            "FLOAT": "FLOAT",
            "DOUBLE": "DOUBLE",
            "DECIMAL": "DECIMAL",
            # String types
            "CHAR": "CHAR",
            "VARCHAR": "VARCHAR",
            "STRING": "STRING",
            # Binary types
            "BINARY": "BINARY",
            "VARBINARY": "VARBINARY",
            "BYTES": "BYTES",
            # Date/Time types
            "DATE": "DATE",
            "TIME": "TIME",
            "TIMESTAMP": "TIMESTAMP",
            "TIMESTAMP_LTZ": "TIMESTAMP",
            # Boolean
            "BOOLEAN": "BOOLEAN",
            # Array/Map/Row (complex types)
            "ARRAY": "ARRAY",
            "MAP": "MAP",
            "ROW": "STRUCT",
            "MULTISET": "ARRAY",
        }

        # Return mapped type or original if not in mapping
        return type_mapping.get(flink_type_upper, flink_type)

    def get_report(self) -> FlinkSourceReport:
        """Return the ingestion report."""
        return self.report

    def close(self) -> None:
        """Close API clients."""
        if hasattr(self, "client"):
            self.client.close()
        if hasattr(self, "sql_gateway_client") and self.sql_gateway_client:
            self.sql_gateway_client.close()
        super().close()

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        """
        Test connection to Flink cluster.

        Args:
            config_dict: Configuration dictionary

        Returns:
            TestConnectionReport with connectivity results
        """
        test_report = TestConnectionReport()

        try:
            config = FlinkSourceConfig.model_validate(config_dict)
            client = FlinkJobManagerClient(config.connection)

            # Test basic connectivity
            if client.test_connection():
                test_report.basic_connectivity = CapabilityReport(capable=True)
            else:
                test_report.basic_connectivity = CapabilityReport(
                    capable=False,
                    failure_reason="Failed to connect to Flink JobManager API",
                )
                return test_report

            # Test job listing capability
            try:
                jobs = client.get_jobs()
                if test_report.capability_report is None:
                    test_report.capability_report = {}
                test_report.capability_report[SourceCapability.DESCRIPTIONS] = (
                    CapabilityReport(capable=True)
                )
                logger.info(f"Found {len(jobs)} jobs in cluster")
            except Exception as e:
                if test_report.capability_report is None:
                    test_report.capability_report = {}
                test_report.capability_report[SourceCapability.DESCRIPTIONS] = (
                    CapabilityReport(
                        capable=False,
                        failure_reason=f"Failed to list jobs: {e}",
                    )
                )

            # Test SQL Gateway connectivity if configured
            if config.include_catalog_metadata and config.connection.sql_gateway_url:
                try:
                    sql_client = FlinkSQLGatewayClient(config.connection)
                    if sql_client.test_connection():
                        logger.info("SQL Gateway connection successful")
                    else:
                        logger.warning("SQL Gateway connection failed")
                    sql_client.close()
                except Exception as e:
                    logger.warning(f"SQL Gateway test failed: {e}")

        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False,
                failure_reason=f"Connection test failed: {e}",
            )

        return test_report
