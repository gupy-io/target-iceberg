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
            secret=True,
            description="Rest catalog user credential",
        ),
        th.Property(
            "s3_endpoint",
            th.StringType,
            description="s3 endpoint",
        ),
        th.Property(
            "s3_access_key_id",
            th.StringType,
            secret=True,
            description="AWS access key id",
        ),
        th.Property(
            "s3_secret_access_key",
            th.StringType,
            secret=True,
            description="AWS secret access key id",
        ),
        th.Property(
            "catalog_uri",
            th.StringType,
            description="Catalog URI, e.g. https://api.catalog.io/ws/",
            required=True,
        ),
        th.Property(
            "warehouse",
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
            "namespace",
            th.StringType,
            description="The namespace where data will be written",
            required=True,
        ),
    ).to_dict()

    default_sink_class = IcebergSink


if __name__ == "__main__":
    TargetIceberg.cli()
