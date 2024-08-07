"""Tests standard target features using the built-in SDK tests library."""

from __future__ import annotations

from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.typedef import Identifier
from singer_sdk.testing import get_target_test_class

from target_iceberg.target import TargetIceberg
from target_iceberg.catalog import get_catalog_config
from tests.utils import get_config
import pytest
import contextlib
import pyarrow as pa

SAMPLE_CONFIG = get_config()


# Run standard built-in target tests from the SDK:
StandardTargetTests = get_target_test_class(
    target_class=TargetIceberg,
    config=SAMPLE_CONFIG,
)


class TestTargetIceberg(StandardTargetTests):  # type: ignore[misc, valid-type]
    """Standard Target Tests."""

    @pytest.fixture(scope="class")
    def resource(self):  # noqa: ANN201
        catalog = load_catalog(
            "demo",
            **get_catalog_config(SAMPLE_CONFIG),
        )
        with contextlib.suppress(NamespaceAlreadyExistsError):
            catalog.create_namespace(SAMPLE_CONFIG["database"])
        catalog.create_table(
            f"{SAMPLE_CONFIG['database']}.test_array_data",
            schema=pa.schema(
                [
                    ("Id", pa.int64()),
                    ("id", pa.int64()),
                    ("_sdc_started_at", pa.timestamp("us", tz="UTC")),
                    ("fruit.element", pa.string()),
                    ("fruits", pa.list_(pa.string())),
                ]
            ),
        )
        catalog.create_table(
            f"{SAMPLE_CONFIG['database']}.test_strings_in_arrays",
            schema=pa.schema(
                [
                    ("id", pa.int64()),
                    ("strings", pa.list_(pa.string())),
                    ("_sdc_started_at", pa.timestamp("us", tz="UTC")),
                ]
            ),
        )
        catalog.create_table(
            f"{SAMPLE_CONFIG['database']}.test_strings_in_objects",
            schema=pa.schema(
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
        )
        catalog.create_table(
            f"{SAMPLE_CONFIG['database']}.test_duplicate_records",
            schema=pa.schema(
                [
                    ("id", pa.int64()),
                    ("_sdc_started_at", pa.timestamp("us", tz="UTC")),
                    ("metric", pa.int64()),
                ]
            ),
        )
        catalog.create_table(
            f"{SAMPLE_CONFIG['database']}.test_strings",
            schema=pa.schema(
                [
                    ("Id", pa.int64()),
                    ("id", pa.int64()),
                    ("info", pa.string()),
                    ("_sdc_started_at", pa.timestamp("us", tz="UTC")),
                ]
            ),
        )
        catalog.create_table(
            f"{SAMPLE_CONFIG['database']}.test_no_pk",
            schema=pa.schema(
                [
                    ("id", pa.int64()),
                    ("metric", pa.int64()),
                    ("_sdc_started_at", pa.timestamp("us", tz="UTC")),
                ]
            ),
        )
        catalog.create_table(
            f"{SAMPLE_CONFIG['database']}.test_optional_attributes",
            schema=pa.schema(
                [
                    ("id", pa.int64()),
                    ("optional", pa.string()),
                    ("_sdc_started_at", pa.timestamp("us", tz="UTC")),
                ]
            ),
        )
        catalog.create_table(
            f"{SAMPLE_CONFIG['database']}.record_missing_fields",
            schema=pa.schema(
                [
                    ("id", pa.int64()),
                    ("optional", pa.string()),
                    ("_sdc_started_at", pa.timestamp("us", tz="UTC")),
                ]
            ),
        )
        catalog.create_table(
            f"{SAMPLE_CONFIG['database']}.test_object_schema_no_properties",
            schema=pa.schema(
                [
                    (
                        "object_store",
                        pa.struct([("id", pa.int64()), ("metric", pa.int64())]),
                    ),
                    ("_sdc_started_at", pa.timestamp("us", tz="UTC")),
                ]
            ),
        )
        catalog.create_table(
            f"{SAMPLE_CONFIG['database']}.test_object_schema_with_properties",
            schema=pa.schema(
                [
                    (
                        "object_store",
                        pa.struct([("id", pa.int64()), ("metric", pa.int64())]),
                    ),
                    ("_sdc_started_at", pa.timestamp("us", tz="UTC")),
                ]
            ),
        )
        catalog.create_table(
            f"{SAMPLE_CONFIG['database']}.test_schema_updates",
            schema=pa.schema(
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
        )
        catalog.create_table(
            (SAMPLE_CONFIG["database"], "test:SpecialChars!in?attributes"),
            schema=pa.schema(
                [
                    ("_id", pa.int64()),
                    ("_sdc_started_at", pa.timestamp("us", tz="UTC")),
                ]
            ),
        )
        catalog.create_table(
            f"{SAMPLE_CONFIG['database']}.TestCamelcase",
            schema=pa.schema(
                [
                    ("Id", pa.string()),
                    ("clientName", pa.string()),
                    ("_sdc_started_at", pa.timestamp("us", tz="UTC")),
                ]
            ),
        )
        return "resource"
