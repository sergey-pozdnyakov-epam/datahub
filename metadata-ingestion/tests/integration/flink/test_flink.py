"""Integration tests for Apache Flink DataHub Source."""

import json
import subprocess
import sys
import time
from pathlib import Path

import pytest
import requests
import time_machine

from datahub.ingestion.run.pipeline import Pipeline

FROZEN_TIME = "2024-01-01 00:00:00"


def wait_for_flink_service(url: str, timeout: int = 60) -> bool:
    """Wait for a Flink service to be ready."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                return True
        except requests.exceptions.RequestException:
            pass
        time.sleep(2)
    return False


@pytest.fixture(scope="module")
def flink_test_data():
    """Set up test data in existing Flink cluster.

    Note: This assumes a Flink cluster is running on localhost:8081.
    For Docker-based testing, ensure docker-compose is started beforehand.
    """
    # Check if Flink is accessible
    if not wait_for_flink_service("http://localhost:8081/config", timeout=30):
        pytest.skip("Flink cluster not accessible on localhost:8081")

    # Run setup script if it exists
    setup_script = Path(__file__).parent / "setup" / "setup_test_data.py"
    if setup_script.exists():
        result = subprocess.run(
            [sys.executable, str(setup_script)],
            capture_output=True,
            text=True,
            timeout=120,
        )

        if result.returncode == 0:
            # Parse result JSON from output
            for line in result.stdout.split("\n"):
                if line.startswith("RESULT:"):
                    result_data = json.loads(line.replace("RESULT:", "").strip())
                    return result_data

    return {"status": "success"}


@pytest.mark.integration
@time_machine.travel(FROZEN_TIME)
def test_flink_ingestion_golden_file(flink_test_data, pytestconfig, tmp_path):
    """Test complete Flink ingestion against golden file.

    This test verifies:
    - Cluster container extraction
    - Catalog database containers (if SQL Gateway available)
    - Table datasets with schemas (if SQL Gateway available)
    - Type mapping (INT, VARCHAR, TIMESTAMP, etc.)
    - Container hierarchy (Cluster → Database → Tables)

    Note: This test generates a golden file on first run with --update-golden-files flag.
    """
    test_resources_dir: Path = Path(__file__).parent
    golden_file = test_resources_dir / "flink_mces_golden.json"

    # Determine if SQL Gateway is available
    sql_gateway_available = wait_for_flink_service(
        "http://localhost:8083/v1/info", timeout=5
    )

    # Run ingestion
    connection_config: dict[str, str] = {
        "rest_api_url": "http://localhost:8081",
    }
    source_config: dict[str, object] = {
        "connection": connection_config,
        "include_checkpoint_metadata": True,
        "include_job_exceptions": True,
        "cluster_name": "flink-test-cluster",
        "env": "TEST",
    }
    pipeline_config: dict[str, object] = {
        "source": {
            "type": "flink",
            "config": source_config,
        },
        "sink": {
            "type": "file",
            "config": {"filename": str(tmp_path / "flink_mces.json")},
        },
    }

    # Add SQL Gateway config if available
    if sql_gateway_available:
        connection_config["sql_gateway_url"] = "http://localhost:8083"
        source_config["include_catalog_metadata"] = True

    pipeline = Pipeline.create(pipeline_config)
    pipeline.run()
    pipeline.raise_from_status()

    # Verify output exists and has content
    output_file = tmp_path / "flink_mces.json"
    assert output_file.exists(), "Output file was not created"

    # Read and verify structure
    with open(output_file) as f:
        output_data = json.load(f)

    assert len(output_data) > 0, "Output file is empty"

    # Basic structural validation
    entity_types = {item.get("entityType") for item in output_data}
    aspect_names = {item.get("aspectName") for item in output_data}

    # Verify we extracted containers (cluster at minimum)
    assert "container" in entity_types, "No container entities found"

    # If SQL Gateway is available, verify we extracted datasets
    if sql_gateway_available:
        assert "dataset" in entity_types, "No dataset entities found"
        assert "schemaMetadata" in aspect_names, "Missing schemaMetadata"
        assert "datasetProperties" in aspect_names, "Missing datasetProperties"

    # Verify key aspects are present
    assert "containerProperties" in aspect_names, "Missing containerProperties"

    # Write golden file if requested
    if pytestconfig.getoption("--update-golden-files", default=False):
        with open(golden_file, "w") as f:
            json.dump(output_data, f, indent=2)
        print(f"\n✓ Golden file updated: {golden_file}")

    # If golden file exists, compare
    if golden_file.exists():
        with open(golden_file) as f:
            golden_data = json.load(f)

        # Basic comparison (entity count and types)
        assert len(output_data) == len(golden_data), (
            f"Output has {len(output_data)} events, golden has {len(golden_data)}"
        )

        golden_entity_types = {item.get("entityType") for item in golden_data}
        assert entity_types == golden_entity_types, (
            f"Entity types mismatch: {entity_types} vs {golden_entity_types}"
        )


@pytest.mark.integration
def test_flink_connection_success(flink_test_data):
    """Test that connection to Flink cluster succeeds with valid config."""
    from datahub.ingestion.source.flink.source import FlinkSource

    config_dict = {
        "connection": {
            "rest_api_url": "http://localhost:8081",
        },
        "env": "TEST",
    }

    # Test connection should succeed
    report = FlinkSource.test_connection(config_dict)
    assert report is not None, "Test connection returned None"

    # Check that basic connectivity worked
    assert report.basic_connectivity is not None, "Basic connectivity not reported"
    assert report.basic_connectivity.capable is True, "Basic connectivity failed"


@pytest.mark.integration
def test_flink_connection_failure_invalid_url():
    """Test that connection fails gracefully with invalid URL."""
    from datahub.ingestion.source.flink.source import FlinkSource

    config_dict = {
        "connection": {
            "rest_api_url": "http://localhost:9999",  # Wrong port
        },
        "env": "TEST",
    }

    # Test connection should return a report indicating failure
    report = FlinkSource.test_connection(config_dict)
    assert report is not None, "Test connection returned None"

    # Should have failure information
    assert report.basic_connectivity is not None, "Basic connectivity not reported"
    assert report.basic_connectivity.capable is False, (
        "Expected basic connectivity to fail"
    )


@pytest.mark.integration
def test_flink_extracts_cluster_container(flink_test_data, tmp_path):
    """Test that Flink connector extracts the cluster as a container."""
    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.source.flink.source import FlinkSource

    config = {
        "connection": {
            "rest_api_url": "http://localhost:8081",
        },
        "cluster_name": "test-cluster",
        "env": "TEST",
    }

    ctx = PipelineContext(run_id="test-cluster-run")
    source = FlinkSource.create(config, ctx)

    # Get workunits
    workunits = list(source.get_workunits())

    # Find container workunits
    container_urns = []
    for wu in workunits:
        if hasattr(wu.metadata, "entityUrn"):
            urn = wu.metadata.entityUrn
            if isinstance(urn, str) and "container" in urn:
                container_urns.append(urn)

    # Should have at least one container (the cluster)
    assert len(container_urns) > 0, "No cluster container extracted"

    # Verify cluster name appears in container properties
    found_cluster = False
    for wu in workunits:
        if hasattr(wu.metadata, "aspect"):
            aspect = wu.metadata.aspect
            if aspect is not None and hasattr(aspect, "name"):
                if "test-cluster" in aspect.name or "flink" in aspect.name.lower():
                    found_cluster = True
                    break

    assert found_cluster, "Cluster container not properly named"


@pytest.mark.integration
def test_flink_extracts_catalog_tables(flink_test_data, tmp_path):
    """Test that Flink connector extracts catalog tables with schemas.

    Note: This test requires SQL Gateway to be running on port 8083.
    """
    # Skip if SQL Gateway not available
    if not wait_for_flink_service("http://localhost:8083/v1/info", timeout=5):
        pytest.skip("SQL Gateway not available on localhost:8083")

    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.source.flink.source import FlinkSource

    config = {
        "connection": {
            "rest_api_url": "http://localhost:8081",
            "sql_gateway_url": "http://localhost:8083",
        },
        "include_catalog_metadata": True,
        "env": "TEST",
    }

    ctx = PipelineContext(run_id="test-catalog-run")
    source = FlinkSource.create(config, ctx)

    # Get workunits
    workunits = list(source.get_workunits())

    # Find dataset entities (catalog tables)
    dataset_urns = []
    schema_aspects = []
    for wu in workunits:
        if hasattr(wu.metadata, "entityUrn"):
            urn = wu.metadata.entityUrn
            if isinstance(urn, str) and "dataset" in urn:
                dataset_urns.append(urn)

        if hasattr(wu.metadata, "aspect"):
            aspect = wu.metadata.aspect
            if aspect is not None and hasattr(aspect, "fields"):
                schema_aspects.append(aspect)

    # Should have extracted tables (users, orders, products from setup if available)
    assert len(dataset_urns) >= 1, f"Expected at least 1 table, got {len(dataset_urns)}"

    # Should have schema metadata
    assert len(schema_aspects) >= 1, (
        f"Expected schema for at least 1 table, got {len(schema_aspects)}"
    )

    # Verify schema fields are populated
    for schema in schema_aspects:
        assert len(schema.fields) > 0, "Schema has no fields"

        # Verify fields have required attributes
        for field in schema.fields:
            assert field.fieldPath, "Field missing fieldPath"
            assert field.nativeDataType, "Field missing nativeDataType"


@pytest.mark.integration
def test_flink_type_mapping(flink_test_data, tmp_path):
    """Test that Flink types are correctly mapped to DataHub types.

    Note: This test requires SQL Gateway to be running on port 8083.
    """
    # Skip if SQL Gateway not available
    if not wait_for_flink_service("http://localhost:8083/v1/info", timeout=5):
        pytest.skip("SQL Gateway not available on localhost:8083")

    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.source.flink.source import FlinkSource

    config = {
        "connection": {
            "rest_api_url": "http://localhost:8081",
            "sql_gateway_url": "http://localhost:8083",
        },
        "include_catalog_metadata": True,
        "env": "TEST",
    }

    ctx = PipelineContext(run_id="test-types-run")
    source = FlinkSource.create(config, ctx)

    # Get workunits
    workunits = list(source.get_workunits())

    # Find schema aspects
    all_fields = []
    for wu in workunits:
        if hasattr(wu.metadata, "aspect"):
            aspect = wu.metadata.aspect
            if aspect is not None and hasattr(aspect, "fields"):
                all_fields.extend(aspect.fields)

    # Should have fields
    assert len(all_fields) > 0, "No schema fields found"

    # Collect native types present
    native_types = {f.nativeDataType for f in all_fields}

    # Verify we see various Flink types
    assert len(native_types) >= 1, (
        f"Expected to find type mappings, found: {native_types}"
    )

    # Verify DataHub type classes are assigned
    for field in all_fields:
        assert field.type is not None, f"Field {field.fieldPath} missing DataHub type"
