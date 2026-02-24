# Apache Flink

## Overview

The Apache Flink source connector extracts metadata from Apache Flink clusters via the JobManager REST API. This connector provides comprehensive visibility into Flink streaming jobs, operators, and operational metadata.

**Extracted Metadata:**

- **Jobs** (as DataFlow entities) - Streaming jobs with status, timing, and configuration
- **Job Operators/Vertices** (as DataJob entities) - Individual operators within jobs with parallelism and status
- **Cluster Information** - Flink version, TaskManager count, available slots
- **Checkpoint Metadata** - Checkpoint state, size, duration, and external paths
- **Job Exceptions** - Failure tracking and error information
- **Catalog Metadata** (optional) - Tables and schemas from Flink catalogs via SQL Gateway

**Key Capabilities:**

- Platform instance support for multi-cluster environments
- Automatic descriptions for all entity types
- Stateful ingestion with deletion detection for stopped/deleted jobs
- Test connection validation
- Configurable filtering for jobs and catalogs

## Prerequisites

- **Flink Cluster**: Running Apache Flink cluster (version 1.13+)
- **JobManager REST API**: Accessible at default port 8081 or custom endpoint
- **Network Access**: DataHub ingestion host must reach JobManager REST endpoint
- **Optional - SQL Gateway**: For catalog integration (default port 8083)
- **Authentication**: Basic auth credentials (if cluster security is enabled)

## Quickstart

### Installation

```bash
pip install 'acryl-datahub[flink]'
```

### Basic Recipe

```yaml
source:
  type: flink
  config:
    # JobManager REST API endpoint
    connection:
      rest_api_url: "http://localhost:8081"

    # Cluster name for container hierarchy
    cluster_name: "production-flink"

    # Environment (PROD, DEV, QA, etc.)
    env: "PROD"

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

### Recipe with Catalog Integration

```yaml
source:
  type: flink
  config:
    connection:
      rest_api_url: "http://localhost:8081"
      # SQL Gateway endpoint for catalog metadata
      sql_gateway_url: "http://localhost:8083"

    cluster_name: "production-flink"
    env: "PROD"

    # Enable catalog metadata extraction
    include_catalog_metadata: true

    # Filter catalogs (optional)
    catalog_pattern:
      deny:
        - "default_catalog" # Exclude in-memory catalog

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

### Recipe with Authentication

```yaml
source:
  type: flink
  config:
    connection:
      rest_api_url: "https://flink.company.com:8081"
      username: "${FLINK_USERNAME}"
      password: "${FLINK_PASSWORD}"
      timeout_seconds: 60
      verify_ssl: true

    cluster_name: "production-flink"
    env: "PROD"

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

## Configuration Options

### Connection Configuration

| Option            | Type   | Required | Default | Description                                                                                                        |
| ----------------- | ------ | -------- | ------- | ------------------------------------------------------------------------------------------------------------------ |
| `rest_api_url`    | string | ✅ Yes   | -       | JobManager REST API endpoint (e.g., `http://localhost:8081`)                                                       |
| `sql_gateway_url` | string | No       | None    | SQL Gateway REST API endpoint (e.g., `http://localhost:8083`). Required when `include_catalog_metadata` is enabled |
| `username`        | string | No       | None    | Username for HTTP basic authentication                                                                             |
| `password`        | string | No       | None    | Password for HTTP basic authentication                                                                             |
| `timeout_seconds` | int    | No       | 30      | HTTP request timeout in seconds                                                                                    |
| `max_retries`     | int    | No       | 3       | Maximum number of retry attempts for failed requests                                                               |
| `verify_ssl`      | bool   | No       | True    | Verify SSL certificates for HTTPS connections                                                                      |

### Source Configuration

| Option              | Type   | Required | Default   | Description                                                                                          |
| ------------------- | ------ | -------- | --------- | ---------------------------------------------------------------------------------------------------- |
| `cluster_name`      | string | No       | "default" | Human-readable cluster name for container identification. Use when ingesting multiple Flink clusters |
| `env`               | string | No       | "PROD"    | Environment label (PROD, DEV, QA, etc.)                                                              |
| `platform_instance` | string | No       | None      | Platform instance identifier for multi-cluster setups                                                |

### Feature Flags

| Option                        | Type | Required | Default | Description                                                                               |
| ----------------------------- | ---- | -------- | ------- | ----------------------------------------------------------------------------------------- |
| `include_job_lineage`         | bool | No       | True    | **Note**: Lineage extraction not yet implemented. Reserved for future release             |
| `include_catalog_metadata`    | bool | No       | False   | Extract catalog metadata via SQL Gateway. Requires `sql_gateway_url` in connection config |
| `include_checkpoint_metadata` | bool | No       | True    | Include checkpoint/savepoint metadata as job custom properties                            |
| `include_job_exceptions`      | bool | No       | True    | Track job exceptions and failures                                                         |

### Filtering Options

| Option            | Type             | Required | Default   | Description                                 |
| ----------------- | ---------------- | -------- | --------- | ------------------------------------------- |
| `job_pattern`     | AllowDenyPattern | No       | Allow all | Regex patterns to filter Flink jobs by name |
| `catalog_pattern` | AllowDenyPattern | No       | Allow all | Regex patterns to filter catalog databases  |

### Stateful Ingestion

| Option                                     | Type | Required | Default | Description                                            |
| ------------------------------------------ | ---- | -------- | ------- | ------------------------------------------------------ |
| `stateful_ingestion.enabled`               | bool | No       | False   | Enable stateful ingestion for deletion detection       |
| `stateful_ingestion.remove_stale_metadata` | bool | No       | True    | Automatically remove stopped/deleted jobs from DataHub |

## Configuration Examples

### Filter Jobs by Pattern

```yaml
source:
  type: flink
  config:
    connection:
      rest_api_url: "http://localhost:8081"

    cluster_name: "production-flink"

    # Include only production jobs
    job_pattern:
      allow:
        - "^prod_.*"
      deny:
        - ".*_test$"
        - ".*_experimental$"
```

### Stateful Ingestion with Deletion Detection

```yaml
source:
  type: flink
  config:
    connection:
      rest_api_url: "http://localhost:8081"

    cluster_name: "production-flink"

    # Enable deletion detection
    stateful_ingestion:
      enabled: true
      remove_stale_metadata: true
```

### Multi-Cluster Setup

```yaml
# Recipe for Cluster 1
source:
  type: flink
  config:
    connection:
      rest_api_url: "http://flink-cluster1:8081"
    cluster_name: "flink-prod-us-east"
    platform_instance: "prod-us-east"
    env: "PROD"

---
# Recipe for Cluster 2
source:
  type: flink
  config:
    connection:
      rest_api_url: "http://flink-cluster2:8081"
    cluster_name: "flink-prod-eu-west"
    platform_instance: "prod-eu-west"
    env: "PROD"
```

## Extracted Metadata

### Entity Types

The Flink connector creates the following DataHub entities:

1. **Container** (Flink Cluster)

   - Represents the entire Flink cluster
   - Contains cluster metadata: version, TaskManager count, slot availability
   - Parent container for all jobs

2. **DataFlow** (Flink Job)

   - Represents a Flink streaming job
   - Includes: job ID, name, status, start/end time
   - Custom properties: checkpoint metadata, job configuration
   - External URL links to Flink UI job detail page

3. **DataJob** (Job Operator/Vertex)

   - Represents individual operators within a job
   - Includes: operator ID, name, parallelism, status
   - Parent: associated DataFlow (job)

4. **Dataset** (Catalog Table) - _Optional_
   - Catalog tables from Flink catalogs (when `include_catalog_metadata` enabled)
   - Includes: table schema, column names, types, comments
   - Parent: Catalog Database container

### Custom Properties

**Job (DataFlow) Properties:**

- `job_id` - Flink job identifier
- `state` - Job status (RUNNING, FINISHED, FAILED, CANCELED)
- `start-time` - Job start timestamp
- `end-time` - Job end timestamp (if completed)

**Job (DataFlow) Properties (when checkpoints enabled):**

- `checkpoint_id` - Latest checkpoint ID
- `checkpoint_status` - Checkpoint status
- `checkpoint_external_path` - External storage path
- `checkpoint_state_size` - State size in bytes
- `checkpoint_duration` - Checkpoint duration in milliseconds

**Operator (DataJob) Properties:**

- `vertex_id` - Operator vertex identifier
- `parallelism` - Operator parallelism level
- `status` - Operator status

### Concept Mapping

| Source Concept      | DataHub Entity | URN Format                                                                      | SubType          | Notes                  |
| ------------------- | -------------- | ------------------------------------------------------------------------------- | ---------------- | ---------------------- |
| Flink Cluster       | Container      | `urn:li:container:{guid}`                                                       | "Flink Cluster"  | Top-level container    |
| Flink Job           | DataFlow       | `urn:li:dataFlow:(flink,{jobId},{env})`                                         | -                | Streaming job/pipeline |
| Job Operator/Vertex | DataJob        | `urn:li:dataJob:({flowUrn},{vertexId})`                                         | "FLINK_OPERATOR" | Individual operator    |
| Catalog Database    | Container      | `urn:li:container:{guid}`                                                       | "Database"       | Flink catalog database |
| Catalog Table       | Dataset        | `urn:li:dataset:(urn:li:dataPlatform:flink,{catalog}.{database}.{table},{env})` | "TABLE"          | Catalog table          |

### Entity Hierarchy

```
Flink Cluster (Container)
├── Job 1 (DataFlow)
│   ├── Source Operator (DataJob)
│   ├── Transform Operator (DataJob)
│   └── Sink Operator (DataJob)
├── Job 2 (DataFlow)
│   └── ...
└── Catalog Database (Container) [Optional]
    ├── Table 1 (Dataset)
    └── Table 2 (Dataset)
```

## Known Limitations

### Lineage

- **Lineage extraction not yet implemented**: The `include_job_lineage` option is reserved for a future release. Currently, no upstream/downstream lineage is extracted from job configurations.
- **Future support planned**: Will infer lineage from Flink connector configurations (Kafka sources, JDBC sinks, etc.)

### Catalog Integration

- **In-Memory Catalogs**: Flink's default in-memory catalog is session-specific and ephemeral. Tables created via SQL Gateway sessions may not persist across sessions.
- **Recommendation**: Use persistent catalogs (Hive Metastore, JDBC Catalog) for production catalog metadata extraction.
- **Empty Catalogs**: If SQL Gateway catalogs have no tables, no dataset entities will be created (expected behavior).

### Operational Constraints

- **Running Jobs Only**: Only currently running, recently completed, or failed jobs visible via REST API are extracted. Very old completed jobs may be purged from JobManager history.
- **Checkpoint Availability**: Checkpoint metadata only available for jobs that have completed at least one checkpoint.
- **API Limitations**: Flink REST API does not expose detailed job graph edges. Operator lineage within jobs is not currently extracted.

### Performance Considerations

- **Large Clusters**: Clusters with hundreds of jobs may experience slower ingestion times. Consider using `job_pattern` filters to reduce scope.
- **SQL Gateway**: Catalog metadata extraction requires additional API calls (one per catalog/database/table). Disable `include_catalog_metadata` if not needed.

## Troubleshooting

### Connection Issues

**Problem**: `Failed to connect to Flink JobManager API`

**Solutions**:

1. Verify JobManager REST API is accessible:
   ```bash
   curl http://localhost:8081/v1/overview
   ```
2. Check network connectivity from DataHub ingestion host
3. Verify `rest_api_url` is correct in recipe
4. If using authentication, confirm credentials are valid

**Problem**: `SQL Gateway connection failed`

**Solutions**:

1. Verify SQL Gateway is running and accessible:
   ```bash
   curl http://localhost:8083/v1/info
   ```
2. Ensure `sql_gateway_url` is configured when `include_catalog_metadata: true`
3. Check SQL Gateway logs for startup errors

### Authentication Issues

**Problem**: `401 Unauthorized` or `403 Forbidden`

**Solutions**:

1. Verify `username` and `password` are correct
2. Confirm Flink cluster has authentication enabled (if expecting auth)
3. Check Flink security configuration in `flink-conf.yaml`

### Missing Jobs

**Problem**: Jobs visible in Flink UI but not ingested to DataHub

**Solutions**:

1. Check `job_pattern` filters - may be excluding jobs unintentionally:
   ```yaml
   job_pattern:
     allow:
       - ".*" # Allow all jobs
   ```
2. Verify job is in a state visible to REST API (RUNNING, FINISHED, FAILED)
3. Check ingestion logs for errors or warnings about specific jobs
4. Confirm job is not filtered by default patterns

### Missing Catalog Metadata

**Problem**: Catalog tables not appearing in DataHub

**Solutions**:

1. Verify `include_catalog_metadata: true` in recipe
2. Confirm `sql_gateway_url` is configured and accessible
3. Check if catalog actually has tables:
   ```bash
   # Create SQL Gateway session and query
   curl -X POST http://localhost:8083/v1/sessions
   # Use session to run: SHOW CATALOGS; SHOW DATABASES; SHOW TABLES;
   ```
4. Review `catalog_pattern` filters - may be excluding catalogs
5. **Important**: In-memory catalogs may be empty. Use persistent catalogs (Hive, JDBC) for production

### Checkpoint Metadata Missing

**Problem**: Checkpoint properties not appearing on jobs

**Solutions**:

1. Verify `include_checkpoint_metadata: true` (default)
2. Confirm job has completed at least one checkpoint
3. Check Flink UI → Job → Checkpoints tab to verify checkpoints exist
4. Review ingestion logs for checkpoint retrieval errors

### Performance Issues

**Problem**: Ingestion taking too long

**Solutions**:

1. **Filter jobs**: Reduce scope with `job_pattern`:
   ```yaml
   job_pattern:
     allow:
       - "^prod_critical_.*" # Only critical jobs
   ```
2. **Disable catalog metadata**: Set `include_catalog_metadata: false` if not needed
3. **Increase timeout**: For large clusters, increase `timeout_seconds`:
   ```yaml
   connection:
     timeout_seconds: 120 # Increase from default 30
   ```
4. **Disable checkpoints**: Set `include_checkpoint_metadata: false` to reduce API calls

### Stateful Ingestion Issues

**Problem**: Stopped jobs not removed from DataHub

**Solutions**:

1. Verify stateful ingestion is enabled:
   ```yaml
   stateful_ingestion:
     enabled: true
     remove_stale_metadata: true
   ```
2. Ensure ingestion runs regularly (stopped jobs detected on subsequent runs)
3. Check DataHub version supports stateful ingestion (DataHub 0.8.0+)

## Advanced Configuration

### Custom SSL Certificates

For self-signed certificates in development:

```yaml
source:
  type: flink
  config:
    connection:
      rest_api_url: "https://flink-dev:8081"
      verify_ssl: false # Only for development/testing
      username: "admin"
      password: "${FLINK_PASSWORD}"
```

**Warning**: Never disable SSL verification in production environments.

### Timeout and Retry Configuration

For unreliable networks or large clusters:

```yaml
source:
  type: flink
  config:
    connection:
      rest_api_url: "http://flink-cluster:8081"
      timeout_seconds: 120 # Increase timeout
      max_retries: 5 # More retry attempts
```

### Complete Production Recipe

```yaml
source:
  type: flink
  config:
    # Connection
    connection:
      rest_api_url: "https://flink-prod.company.com:8081"
      sql_gateway_url: "https://flink-sql-gateway.company.com:8083"
      username: "${FLINK_API_USER}"
      password: "${FLINK_API_PASSWORD}"
      timeout_seconds: 60
      max_retries: 3
      verify_ssl: true

    # Cluster identification
    cluster_name: "production-flink-us-east"
    platform_instance: "prod-us-east"
    env: "PROD"

    # Feature flags
    include_catalog_metadata: true
    include_checkpoint_metadata: true
    include_job_exceptions: true

    # Filtering
    job_pattern:
      allow:
        - "^prod_.*"
      deny:
        - ".*_test$"
        - ".*_canary$"

    catalog_pattern:
      allow:
        - "hive_catalog"
        - "jdbc_catalog"
      deny:
        - "default_catalog" # Exclude in-memory catalog

    # Stateful ingestion
    stateful_ingestion:
      enabled: true
      remove_stale_metadata: true

sink:
  type: datahub-rest
  config:
    server: "http://datahub-gms.company.com:8080"
    token: "${DATAHUB_TOKEN}"
```

## Testing the Connector

### Quick Test with Docker

```bash
# Start Flink cluster (JobManager + TaskManager)
docker run -d --name flink-jobmanager \
  -p 8081:8081 \
  flink:1.18 jobmanager

docker run -d --name flink-taskmanager \
  --link flink-jobmanager:jobmanager \
  flink:1.18 taskmanager

# Test REST API connectivity
curl http://localhost:8081/v1/overview

# Test DataHub connection
datahub check metadata-service

# Run ingestion
datahub ingest -c flink_recipe.yml
```

### Test Connection

```bash
# Test connection without running full ingestion
python -c "
from datahub.ingestion.source.flink.source import FlinkSource
from datahub.configuration.common import PipelineConfig

config = {
    'connection': {
        'rest_api_url': 'http://localhost:8081'
    },
    'cluster_name': 'test-cluster'
}

report = FlinkSource.test_connection(config)
print(f'Connection successful: {report.basic_connectivity.capable}')
"
```

## FAQ

**Q: Does this connector support Flink on Kubernetes?**

A: Yes, as long as the JobManager REST API is accessible. Use the Kubernetes service endpoint or ingress URL as `rest_api_url`.

**Q: Can I extract lineage from Flink jobs?**

A: Lineage extraction is planned for a future release but not yet implemented. The `include_job_lineage` option is reserved for this feature.

**Q: What Flink versions are supported?**

A: Flink 1.13+ is recommended. Older versions may work if they expose compatible REST API endpoints.

**Q: How often should I run ingestion?**

A: For active clusters, run every 15-60 minutes to capture job lifecycle changes. Enable stateful ingestion to automatically remove stopped jobs.

**Q: Does this extract Flink SQL query history?**

A: No, only job metadata and catalog tables are extracted. Query history is not available via REST APIs.

**Q: Can I extract Table API / SQL job lineage?**

A: Not currently. Future releases may parse Table API job plans to infer lineage from SQL queries.

**Q: Why are my catalog tables empty?**

A: In-memory catalogs are session-specific and may be empty. Use persistent catalogs (Hive Metastore, JDBC Catalog) with proper configuration in Flink.

## Resources

- **Apache Flink Documentation**: https://nightlies.apache.org/flink/flink-docs-stable/
- **JobManager REST API**: https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/rest_api/
- **SQL Gateway REST API**: https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql-gateway/rest/
- **DataHub Slack**: Join #ingestion-help for questions
- **GitHub Issues**: Report bugs at https://github.com/datahub-project/datahub/issues
