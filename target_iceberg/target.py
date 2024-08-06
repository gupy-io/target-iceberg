"""Iceberg target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_iceberg.sinks import (
    IcebergSink,
)


class TargetIceberg(Target):
    """Sample target for Iceberg."""

    name = "target-iceberg"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "credential",
            th.StringType,
            secret=True,  # Flag config as protected.
            description="Rest catalog user credential",
            required=True,
        ),
        th.Property(
            "catalog_uri",
            th.StringType,
            description="Catalog URI, e.g. https://api.catalog.io/ws/",
            required=True,
        ),
        th.Property(
            "catalog_name",
            th.StringType,
            description="The name of the catalog where data will be written",
            required=True,
        ),
        th.Property(
            "catalog_type",
            th.StringType,
            description="rest or jdbc",
            required=True,
        ),
        th.Property(
            "database",
            th.StringType,
            description="The name of the database where data will be written",
            required=True,
        ),
    ).to_dict()

    default_sink_class = IcebergSink


if __name__ == "__main__":
    TargetIceberg.cli()
