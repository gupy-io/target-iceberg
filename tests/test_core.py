"""Tests standard target features using the built-in SDK tests library."""

from __future__ import annotations

from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from singer_sdk.testing import get_target_test_class

from target_iceberg.target import TargetIceberg
from target_iceberg.catalog import get_catalog_config
import uuid
import pytest
import pyarrow as pa


SAMPLE_CONFIG = {
    "catalog_uri": "http://localhost:8181",
    "catalog_name": "demo",
    "catalog_type": "rest",
    "database": uuid.uuid4().hex,
    "s3_endpoint": "http://localhost:9000",
    "s3_access_key_id": "admin",
    "s3_secret_access_key": "password",
}

REQUIRED_TABLES = {
    "test_array_data": pa.schema(
        [
            ("Id", pa.int64()),
            ("id", pa.int64()),
            ("_sdc_started_at", pa.timestamp("us", tz="UTC")),
            ("fruit.element", pa.string()),
            ("fruits", pa.list_(pa.string())),
        ]
    ),
    "test_strings_in_arrays": pa.schema(
        [
            ("id", pa.int64()),
            ("strings", pa.list_(pa.string())),
            ("_sdc_started_at", pa.timestamp("us", tz="UTC")),
        ]
    ),
    "test_strings_in_objects": pa.schema(
        [
            ("id", pa.int64()),
            ("string", pa.list_(pa.string())),
            (
                "info",
                pa.struct([("name", pa.string()), ("value", pa.string())]),
            ),
            ("_sdc_started_at", pa.timestamp("us", tz="UTC")),
        ]
    ),
    "test_optional_attributes": pa.schema(
        [
            ("id", pa.int64()),
            ("optional", pa.string()),
            ("_sdc_started_at", pa.timestamp("us", tz="UTC")),
        ]
    ),
    "record_missing_fields": pa.schema(
        [
            ("id", pa.int64()),
            ("optional", pa.string()),
            ("_sdc_started_at", pa.timestamp("us", tz="UTC")),
        ]
    ),
    "test_duplicate_records": pa.schema(
        [
            ("id", pa.int64()),
            ("_sdc_started_at", pa.timestamp("us", tz="UTC")),
            ("metric", pa.int64()),
        ]
    ),
    "test_strings": pa.schema(
        [
            ("Id", pa.int64()),
            ("id", pa.int64()),
            ("info", pa.string()),
            ("_sdc_started_at", pa.timestamp("us", tz="UTC")),
        ]
    ),
    "test_no_pk": pa.schema(
        [
            ("id", pa.int64()),
            ("metric", pa.int64()),
            ("_sdc_started_at", pa.timestamp("us", tz="UTC")),
        ]
    ),
    "test_schema_updates": pa.schema(
        [
            ("id", pa.int64()),
            ("a1", pa.int64()),
            ("a2", pa.string()),
            ("a3", pa.bool_()),
            ("a4", pa.struct([("id", pa.int64()), ("value", pa.int64())])),
            ("a5", pa.list_(pa.string())),
            ("a6", pa.int64()),
            ("_sdc_started_at", pa.timestamp("us", tz="UTC")),
        ]
    ),
    "test_object_schema_with_properties": pa.schema(
        [
            (
                "object_store",
                pa.struct([("id", pa.int64()), ("metric", pa.int64())]),
            ),
            ("_sdc_started_at", pa.timestamp("us", tz="UTC")),
        ]
    ),
    "test_object_schema_no_properties": pa.schema(
        [
            (
                "object_store",
                pa.struct([("id", pa.int64()), ("metric", pa.int64())]),
            ),
            ("_sdc_started_at", pa.timestamp("us", tz="UTC")),
        ]
    ),
    "TestCamelcase": pa.schema(
        [
            ("Id", pa.string()),
            ("clientName", pa.string()),
            ("_sdc_started_at", pa.timestamp("us", tz="UTC")),
        ]
    ),
}


# Run standard built-in target tests from the SDK:
StandardTargetTests = get_target_test_class(
    target_class=TargetIceberg,
    config=SAMPLE_CONFIG,
)


class TestTargetIceberg(StandardTargetTests):  # type: ignore[misc, valid-type]
    """Standard Target Tests."""

    def _create_required_tables(self) -> None:
        catalog = load_catalog(
            "demo",
            **get_catalog_config(SAMPLE_CONFIG),
        )

        for table, schema in REQUIRED_TABLES.items():
            catalog.create_table(
                f"{SAMPLE_CONFIG['database']}.{table}",
                schema=schema,
            )

    @pytest.fixture(scope="class")
    def resource(self):  # noqa: ANN201
        self._create_required_tables()
