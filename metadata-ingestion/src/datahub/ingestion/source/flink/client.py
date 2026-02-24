"""
Flink REST API clients.

This module provides clients for interacting with Apache Flink's REST APIs:
- FlinkJobManagerClient: JobManager REST API for job metadata
- FlinkSQLGatewayClient: SQL Gateway REST API for catalog metadata
"""

import logging
from typing import Any, Dict, Iterator, List, Optional, cast
from urllib.parse import urljoin

import requests  # type: ignore[import-untyped]
from requests.adapters import HTTPAdapter  # type: ignore[import-untyped]
from urllib3.util.retry import Retry

from datahub.ingestion.source.flink.config import FlinkConnectionConfig

logger = logging.getLogger(__name__)


class FlinkJobManagerClient:
    """
    Client for Flink JobManager REST API.

    Handles:
    - Job metadata retrieval
    - Checkpoint statistics
    - Job execution graphs
    - Exception tracking
    - Authentication
    - Request retry logic
    - Error handling

    API Documentation:
    https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/rest_api/
    """

    def __init__(self, config: FlinkConnectionConfig):
        """
        Initialize JobManager API client.

        Args:
            config: Connection configuration with API URL and auth settings
        """
        self.config = config
        self.base_url = config.rest_api_url.rstrip("/")

        # Setup session with retry logic
        self.session = requests.Session()
        retry_strategy = Retry(
            total=config.max_retries,
            backoff_factor=1,  # Exponential backoff: 1, 2, 4 seconds
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # Setup authentication
        self._setup_auth()

        # Connection test flag
        self._connection_tested = False

    def _setup_auth(self) -> None:
        """Configure authentication headers."""
        # Note: Flink REST API typically doesn't require authentication
        # for local development, but can support basic auth in production
        if self.config.username and self.config.password:
            self.session.auth = (
                self.config.username,
                self.config.password.get_secret_value(),
            )
            logger.info("Configured basic authentication for Flink REST API")
        else:
            logger.debug("No authentication configured (local/unauthenticated mode)")

    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Make API request with error handling.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint path
            params: Query parameters
            json: JSON body

        Returns:
            Response data as dictionary

        Raises:
            requests.exceptions.HTTPError: For HTTP errors
            requests.exceptions.Timeout: For timeouts
            requests.exceptions.RequestException: For other request failures
        """
        url = urljoin(self.base_url + "/", endpoint.lstrip("/"))

        try:
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                json=json,
                timeout=self.config.timeout_seconds,
                verify=self.config.verify_ssl,
            )
            response.raise_for_status()
            return cast(Dict[str, Any], response.json())

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(f"Resource not found: {url}")
                return {}
            elif e.response.status_code == 403:
                logger.error(f"Permission denied: {url}")
                raise PermissionError(f"Permission denied for {url}") from e
            else:
                logger.error(f"HTTP error {e.response.status_code}: {url}")
                raise
        except requests.exceptions.Timeout:
            logger.error(f"Request timeout: {url}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {url}: {e}")
            raise

    def test_connection(self) -> bool:
        """
        Test connectivity to Flink JobManager API.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Test with /overview endpoint which provides cluster overview
            response = self._request("GET", "/overview")
            if response and "slots-total" in response:
                logger.info("Successfully connected to Flink JobManager API")
                self._connection_tested = True
                return True
            else:
                logger.warning("Connected but unexpected response format")
                return False
        except Exception as e:
            logger.error(f"Failed to connect to Flink JobManager: {e}")
            return False

    def get_cluster_overview(self) -> Dict[str, Any]:
        """
        Get cluster overview information.

        Returns:
            Dictionary with cluster metadata (version, taskmanagers, slots, etc.)
        """
        return self._request("GET", "/overview")

    def get_jobs(self) -> List[Dict[str, Any]]:
        """
        Get list of all jobs.

        Returns:
            List of job dictionaries with id, name, status, start-time
        """
        response = self._request("GET", "/jobs")
        return cast(List[Dict[str, Any]], response.get("jobs", []))

    def get_job(self, job_id: str) -> Dict[str, Any]:
        """
        Get detailed information for a specific job.

        Args:
            job_id: Unique job identifier

        Returns:
            Job details including name, state, timestamps, execution plan
        """
        return self._request("GET", f"/jobs/{job_id}")

    def get_job_config(self, job_id: str) -> Dict[str, Any]:
        """
        Get job configuration.

        Args:
            job_id: Unique job identifier

        Returns:
            Job configuration including execution settings
        """
        return self._request("GET", f"/jobs/{job_id}/config")

    def get_job_plan(self, job_id: str) -> Dict[str, Any]:
        """
        Get job execution plan (dataflow graph).

        Args:
            job_id: Unique job identifier

        Returns:
            Execution plan with nodes and edges representing operators
        """
        return self._request("GET", f"/jobs/{job_id}/plan")

    def get_job_vertices(self, job_id: str) -> List[Dict[str, Any]]:
        """
        Get list of job vertices (operators).

        Args:
            job_id: Unique job identifier

        Returns:
            List of vertex dictionaries with id, name, parallelism, status
        """
        job_details = self.get_job(job_id)
        return cast(List[Dict[str, Any]], job_details.get("vertices", []))

    def get_job_vertex(self, job_id: str, vertex_id: str) -> Dict[str, Any]:
        """
        Get detailed information for a specific job vertex.

        Args:
            job_id: Unique job identifier
            vertex_id: Unique vertex (operator) identifier

        Returns:
            Vertex details including metrics and subtasks
        """
        return self._request("GET", f"/jobs/{job_id}/vertices/{vertex_id}")

    def get_job_checkpoints(self, job_id: str) -> Dict[str, Any]:
        """
        Get checkpoint statistics for a job.

        Args:
            job_id: Unique job identifier

        Returns:
            Checkpoint statistics including latest checkpoint, counts, history
        """
        try:
            return self._request("GET", f"/jobs/{job_id}/checkpoints")
        except Exception as e:
            logger.warning(f"Failed to fetch checkpoints for job {job_id}: {e}")
            return {}

    def get_job_checkpoint_config(self, job_id: str) -> Dict[str, Any]:
        """
        Get checkpoint configuration for a job.

        Args:
            job_id: Unique job identifier

        Returns:
            Checkpoint configuration (interval, timeout, externalized)
        """
        try:
            return self._request("GET", f"/jobs/{job_id}/checkpoints/config")
        except Exception as e:
            logger.warning(f"Failed to fetch checkpoint config for job {job_id}: {e}")
            return {}

    def get_job_exceptions(self, job_id: str) -> Dict[str, Any]:
        """
        Get job exceptions and failures.

        Args:
            job_id: Unique job identifier

        Returns:
            Exception information including root exceptions and truncated flag
        """
        try:
            return self._request("GET", f"/jobs/{job_id}/exceptions")
        except Exception as e:
            logger.warning(f"Failed to fetch exceptions for job {job_id}: {e}")
            return {}

    def close(self) -> None:
        """Close the HTTP session."""
        self.session.close()


class FlinkSQLGatewayClient:
    """
    Client for Flink SQL Gateway REST API.

    Handles:
    - Catalog listing
    - Database enumeration
    - Table metadata extraction
    - SQL session management
    - Authentication
    - Request retry logic

    API Documentation:
    https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql-gateway/rest/
    """

    def __init__(self, config: FlinkConnectionConfig):
        """
        Initialize SQL Gateway API client.

        Args:
            config: Connection configuration with SQL Gateway URL
        """
        if not config.sql_gateway_url:
            raise ValueError(
                "sql_gateway_url must be configured for SQL Gateway client"
            )

        self.config = config
        self.base_url = config.sql_gateway_url.rstrip("/")

        # Setup session with retry logic
        self.session = requests.Session()
        retry_strategy = Retry(
            total=config.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # Setup authentication (same as JobManager)
        if config.username and config.password:
            self.session.auth = (
                config.username,
                config.password.get_secret_value(),
            )

        # Session management
        self._session_handle: Optional[str] = None

    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Make API request with error handling."""
        url = urljoin(self.base_url + "/", endpoint.lstrip("/"))

        try:
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                json=json,
                timeout=self.config.timeout_seconds,
                verify=self.config.verify_ssl,
            )
            response.raise_for_status()
            return cast(Dict[str, Any], response.json())

        except requests.exceptions.RequestException as e:
            logger.error(f"SQL Gateway request failed: {url}: {e}")
            raise

    def open_session(self) -> str:
        """
        Open a SQL Gateway session.

        Returns:
            Session handle string

        Raises:
            Exception: If session creation fails
        """
        if self._session_handle:
            logger.debug("Reusing existing session")
            return self._session_handle

        try:
            response = self._request(
                "POST",
                "/sessions",
                json={"properties": {}},
            )
            self._session_handle = response["sessionHandle"]
            logger.info(f"Opened SQL Gateway session: {self._session_handle}")
            return self._session_handle
        except Exception as e:
            logger.error(f"Failed to open SQL Gateway session: {e}")
            raise

    def close_session(self) -> None:
        """Close the current SQL Gateway session."""
        if not self._session_handle:
            return

        try:
            self._request("DELETE", f"/sessions/{self._session_handle}")
            logger.info(f"Closed SQL Gateway session: {self._session_handle}")
            self._session_handle = None
        except Exception as e:
            logger.warning(f"Failed to close SQL Gateway session: {e}")

    def execute_statement(
        self, sql: str, session_handle: Optional[str] = None
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        Execute SQL statement and yield results.

        Args:
            sql: SQL query to execute
            session_handle: Optional session handle (uses default if None)

        Yields:
            Batches of result rows

        Raises:
            Exception: If statement execution fails
        """
        if not session_handle:
            session_handle = self._session_handle or self.open_session()

        try:
            # Submit statement
            response = self._request(
                "POST",
                f"/sessions/{session_handle}/statements",
                json={"statement": sql},
            )
            operation_handle = response["operationHandle"]
            logger.debug(f"Submitted statement, operation: {operation_handle}")

            # Fetch results (with polling for operation completion)
            result_token = 0
            while True:
                result_response = self._request(
                    "GET",
                    f"/sessions/{session_handle}/operations/{operation_handle}/result/{result_token}",
                )
                logger.debug(f"Result response: {result_response}")

                # Check if operation is still executing
                result_type = result_response.get("resultType")
                if result_type == "NOT_READY":
                    # Operation not ready yet, poll status until complete
                    import time

                    max_wait = 30  # 30 seconds max wait
                    waited = 0.0
                    while waited < max_wait:
                        time.sleep(0.5)
                        waited += 0.5
                        status_response = self._request(
                            "GET",
                            f"/sessions/{session_handle}/operations/{operation_handle}/status",
                        )
                        status = status_response.get("status")
                        logger.debug(f"Operation status: {status}")
                        if status in ["FINISHED", "ERROR", "CANCELED"]:
                            break
                    # Retry fetching results
                    result_response = self._request(
                        "GET",
                        f"/sessions/{session_handle}/operations/{operation_handle}/result/{result_token}",
                    )

                results = result_response.get("results", {})
                data = results.get("data", [])
                logger.debug(f"Data from result/{result_token}: {data}")

                if data:
                    yield data

                # Check if more results available
                # Note: nextResultUri may be present but empty, so check both conditions
                next_result_uri = result_response.get("nextResultUri")
                if not next_result_uri or not data:
                    # Stop if no more URIs or if current page had no data
                    break

                result_token += 1

        except Exception as e:
            logger.error(f"Failed to execute SQL statement '{sql}': {e}")
            raise

    def get_catalogs(self) -> List[str]:
        """
        Get list of available catalogs.

        Returns:
            List of catalog names
        """
        try:
            results = list(self.execute_statement("SHOW CATALOGS"))
            logger.debug(f"Raw results from SHOW CATALOGS: {results}")
            catalogs = []
            for batch in results:
                logger.debug(f"Processing batch: {batch}")
                for row in batch:
                    logger.debug(f"Processing row: {row}, type: {type(row)}")
                    # Row format: {'kind': 'INSERT', 'fields': ['default_catalog']}
                    if isinstance(row, dict) and "fields" in row:
                        catalog_name = row["fields"][0]
                        catalogs.append(catalog_name)
                        logger.debug(f"Added catalog: {catalog_name}")
            logger.info(f"Found {len(catalogs)} catalog(s): {catalogs}")
            return catalogs
        except Exception as e:
            logger.error(f"Failed to list catalogs: {e}")
            return []

    def get_databases(self, catalog: str) -> List[str]:
        """
        Get list of databases in a catalog.

        Args:
            catalog: Catalog name

        Returns:
            List of database names
        """
        try:
            # Switch to catalog first
            list(self.execute_statement(f"USE CATALOG `{catalog}`"))

            # List databases
            results = list(self.execute_statement("SHOW DATABASES"))
            databases = []
            for batch in results:
                for row in batch:
                    # Row format: {'kind': 'INSERT', 'fields': ['database_name']}
                    if isinstance(row, dict) and "fields" in row:
                        database_name = row["fields"][0]
                        databases.append(database_name)
            return databases
        except Exception as e:
            logger.error(f"Failed to list databases for catalog {catalog}: {e}")
            return []

    def get_tables(self, catalog: str, database: str) -> List[str]:
        """
        Get list of tables in a database.

        Args:
            catalog: Catalog name
            database: Database name

        Returns:
            List of table names
        """
        try:
            # Switch to catalog and database
            list(self.execute_statement(f"USE CATALOG `{catalog}`"))
            list(self.execute_statement(f"USE `{database}`"))

            # List tables
            results = list(self.execute_statement("SHOW TABLES"))
            tables = []
            for batch in results:
                for row in batch:
                    # Row format: {'kind': 'INSERT', 'fields': ['table_name']}
                    if isinstance(row, dict) and "fields" in row:
                        table_name = row["fields"][0]
                        tables.append(table_name)
            return tables
        except Exception as e:
            logger.error(f"Failed to list tables for {catalog}.{database}: {e}")
            return []

    def get_table_schema(
        self, catalog: str, database: str, table: str
    ) -> List[Dict[str, Any]]:
        """
        Get table schema (columns and types).

        Args:
            catalog: Catalog name
            database: Database name
            table: Table name

        Returns:
            List of column dictionaries with name, type, nullable, comment
        """
        try:
            full_table_name = f"`{catalog}`.`{database}`.`{table}`"
            results = list(self.execute_statement(f"DESCRIBE {full_table_name}"))

            columns = []
            for batch in results:
                for row in batch:
                    # DESCRIBE returns: {'kind': 'INSERT', 'fields': ['col_name', 'data_type', 'nullable', ...]}
                    if isinstance(row, dict) and "fields" in row:
                        fields = row["fields"]
                        columns.append(
                            {
                                "name": fields[0] if len(fields) > 0 else None,
                                "type": fields[1] if len(fields) > 1 else None,
                                "nullable": fields[2] if len(fields) > 2 else True,
                                "comment": fields[3] if len(fields) > 3 else None,
                            }
                        )
            return columns
        except Exception as e:
            logger.error(f"Failed to describe table {catalog}.{database}.{table}: {e}")
            return []

    def test_connection(self) -> bool:
        """
        Test connectivity to SQL Gateway API.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            session_handle = self.open_session()
            if session_handle:
                # Try a simple query
                list(self.execute_statement("SHOW CATALOGS"))
                logger.info("Successfully connected to Flink SQL Gateway")
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to connect to Flink SQL Gateway: {e}")
            return False
        finally:
            self.close_session()

    def close(self) -> None:
        """Close session and HTTP connection."""
        self.close_session()
        self.session.close()
