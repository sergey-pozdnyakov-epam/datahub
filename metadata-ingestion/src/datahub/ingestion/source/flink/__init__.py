"""Flink DataHub Connector."""

from datahub.ingestion.source.flink.config import FlinkSourceConfig
from datahub.ingestion.source.flink.source import FlinkSource

__all__ = ["FlinkSourceConfig", "FlinkSource"]
