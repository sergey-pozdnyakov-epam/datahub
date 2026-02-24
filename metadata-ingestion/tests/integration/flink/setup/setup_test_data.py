#!/usr/bin/env python3
"""Setup script for Flink integration test data.

Creates test jobs and catalog tables in the Flink test environment.
"""

import json
import sys
import time
from typing import Any, Dict, Optional

import requests


class FlinkTestSetup:
    """Helper class to set up Flink test environment."""

    def __init__(
        self,
        jobmanager_url: str = "http://localhost:8082",
        sql_gateway_url: str = "http://localhost:8084",
    ):
        self.jobmanager_url = jobmanager_url
        self.sql_gateway_url = sql_gateway_url
        self.session_handle: Optional[str] = None

    def wait_for_flink(self, timeout: int = 60) -> bool:
        """Wait for Flink cluster to be ready."""
        print("Waiting for Flink cluster to be ready...")
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = requests.get(f"{self.jobmanager_url}/config", timeout=5)
                if response.status_code == 200:
                    print("✓ Flink cluster is ready")
                    return True
            except requests.exceptions.RequestException:
                pass
            time.sleep(2)
        print("✗ Flink cluster failed to start")
        return False

    def wait_for_sql_gateway(self, timeout: int = 60) -> bool:
        """Wait for SQL Gateway to be ready."""
        print("Waiting for SQL Gateway to be ready...")
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                # Try to open a session to verify SQL Gateway is working
                response = requests.post(
                    f"{self.sql_gateway_url}/v1/sessions",
                    json={"sessionName": "test_check"},
                    timeout=5,
                )
                if response.status_code == 200:
                    # Close the test session
                    session_handle = response.json()["sessionHandle"]
                    requests.delete(
                        f"{self.sql_gateway_url}/v1/sessions/{session_handle}"
                    )
                    print("✓ SQL Gateway is ready")
                    return True
            except requests.exceptions.RequestException:
                pass
            time.sleep(2)
        print("✗ SQL Gateway failed to start")
        return False

    def open_sql_session(self) -> str:
        """Open a SQL Gateway session."""
        print("Opening SQL Gateway session...")
        response = requests.post(
            f"{self.sql_gateway_url}/v1/sessions",
            json={"sessionName": "test_setup_session"},
            timeout=10,
        )
        response.raise_for_status()
        self.session_handle = response.json()["sessionHandle"]
        print(f"✓ Session opened: {self.session_handle}")
        return self.session_handle

    def execute_sql(self, statement: str) -> Dict[str, Any]:
        """Execute SQL statement via SQL Gateway."""
        if not self.session_handle:
            raise ValueError("No active session. Call open_sql_session() first.")

        print(f"Executing SQL: {statement[:100]}...")
        response = requests.post(
            f"{self.sql_gateway_url}/v1/sessions/{self.session_handle}/statements",
            json={"statement": statement},
            timeout=30,
        )
        response.raise_for_status()
        operation_handle = response.json()["operationHandle"]

        # Wait for statement to complete
        max_wait = 30
        start_time = time.time()
        while time.time() - start_time < max_wait:
            status_response = requests.get(
                f"{self.sql_gateway_url}/v1/sessions/{self.session_handle}/operations/{operation_handle}/status",
                timeout=10,
            )
            status_response.raise_for_status()
            status = status_response.json()["status"]

            if status in ("FINISHED", "ERROR"):
                if status == "ERROR":
                    print("✗ SQL execution failed")
                else:
                    print("✓ SQL executed successfully")
                break
            time.sleep(1)

        return status_response.json()

    def create_catalog_tables(self):
        """Create test tables in the Flink catalog."""
        print("\n=== Creating catalog tables ===")

        # Create a test table with various data types
        create_users_table = """
        CREATE TABLE users (
            user_id INT,
            username VARCHAR(255),
            email VARCHAR(255),
            created_at TIMESTAMP(3),
            is_active BOOLEAN,
            age TINYINT,
            balance DECIMAL(10, 2),
            metadata MAP<STRING, STRING>,
            tags ARRAY<STRING>,
            PRIMARY KEY (user_id) NOT ENFORCED
        )
        """
        self.execute_sql(create_users_table)

        # Create another table
        create_orders_table = """
        CREATE TABLE orders (
            order_id BIGINT,
            user_id INT,
            product_name STRING,
            quantity INT,
            price DOUBLE,
            order_time TIMESTAMP(3),
            status VARCHAR(50),
            PRIMARY KEY (order_id) NOT ENFORCED
        )
        """
        self.execute_sql(create_orders_table)

        # Create a third table for variety
        create_products_table = """
        CREATE TABLE products (
            product_id INT,
            product_name STRING,
            category VARCHAR(100),
            price DECIMAL(8, 2),
            stock_quantity INT,
            description STRING,
            created_date DATE,
            PRIMARY KEY (product_id) NOT ENFORCED
        )
        """
        self.execute_sql(create_products_table)

        print("✓ Catalog tables created successfully")

    def submit_test_job(
        self, job_name: str, job_jar_path: Optional[str] = None
    ) -> Optional[str]:
        """Submit a test Flink job.

        Since we may not have a jar file readily available, we'll create a simple
        streaming job using SQL statements instead.
        """
        print(f"\n=== Submitting test job: {job_name} ===")

        # Create a simple streaming SQL job
        sql_job = """
        CREATE TEMPORARY TABLE user_clicks (
            user_id INT,
            click_time TIMESTAMP(3),
            url STRING,
            WATERMARK FOR click_time AS click_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'datagen',
            'rows-per-second' = '10',
            'fields.user_id.kind' = 'random',
            'fields.user_id.min' = '1',
            'fields.user_id.max' = '1000',
            'fields.url.length' = '50'
        );

        CREATE TEMPORARY TABLE click_counts (
            user_id INT,
            click_count BIGINT,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3)
        ) WITH (
            'connector' = 'blackhole'
        );

        INSERT INTO click_counts
        SELECT
            user_id,
            COUNT(*) as click_count,
            TUMBLE_START(click_time, INTERVAL '1' MINUTE) as window_start,
            TUMBLE_END(click_time, INTERVAL '1' MINUTE) as window_end
        FROM user_clicks
        GROUP BY user_id, TUMBLE(click_time, INTERVAL '1' MINUTE);
        """

        try:
            # Execute the SQL job (this will submit it as a long-running job)
            _ = self.execute_sql(sql_job)  # Job is submitted, ID not returned
            # The job will be submitted but we can't easily get its ID from SQL execution
            print("✓ SQL job submitted (check Flink UI for job details)")
            return None
        except Exception as e:
            print(f"✗ Failed to submit job: {e}")
            return None

    def close_session(self):
        """Close the SQL Gateway session."""
        if self.session_handle:
            print("\nClosing SQL Gateway session...")
            try:
                requests.delete(
                    f"{self.sql_gateway_url}/v1/sessions/{self.session_handle}",
                    timeout=10,
                )
                print("✓ Session closed")
            except Exception as e:
                print(f"✗ Failed to close session: {e}")
            finally:
                self.session_handle = None

    def get_cluster_info(self) -> Dict[str, Any]:
        """Get cluster information."""
        response = requests.get(f"{self.jobmanager_url}/config", timeout=10)
        response.raise_for_status()
        return response.json()

    def get_jobs(self) -> list:
        """Get list of jobs."""
        response = requests.get(f"{self.jobmanager_url}/jobs", timeout=10)
        response.raise_for_status()
        return response.json().get("jobs", [])


def main():
    """Main setup function."""
    setup = FlinkTestSetup()

    # Wait for services to be ready
    if not setup.wait_for_flink():
        print("ERROR: Flink cluster not ready")
        sys.exit(1)

    if not setup.wait_for_sql_gateway():
        print("ERROR: SQL Gateway not ready")
        sys.exit(1)

    try:
        # Open SQL session
        setup.open_sql_session()

        # Create test catalog tables
        setup.create_catalog_tables()

        # Note: Job submission via SQL is tricky without proper JAR files
        # For now, we'll just create the catalog structure
        # The connector can still extract cluster, session, and catalog metadata

        # Print cluster info
        print("\n=== Cluster Information ===")
        cluster_info = setup.get_cluster_info()
        print(f"Flink version: {cluster_info.get('flink-version', 'unknown')}")

        # Print jobs
        print("\n=== Current Jobs ===")
        jobs = setup.get_jobs()
        print(f"Active jobs: {len(jobs)}")
        for job in jobs:
            print(f"  - {job.get('id')}: {job.get('status')}")

        print("\n✓ Test environment setup complete!")
        print("\nNote: SQL Gateway session is kept open for testing.")
        print(f"Session handle: {setup.session_handle}")

        # Return session handle so tests can reuse it
        return {"session_handle": setup.session_handle, "status": "success"}

    except Exception as e:
        print(f"\n✗ Setup failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
    finally:
        # Don't close session - keep it open for tests to use
        pass


if __name__ == "__main__":
    result = main()
    # Print result as JSON for programmatic access
    print(f"\nRESULT: {json.dumps(result)}")
