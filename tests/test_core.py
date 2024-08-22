"""Tests standard target features using the built-in SDK tests library."""

from __future__ import annotations
from pathlib import Path

from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform

from pyiceberg.schema import Schema
from pyiceberg.catalog import load_catalog
from pyiceberg.types import (
    NestedField,
    LongType,
    StringType,
    ListType,
    StructType,
    TimestampType
)

from singer_sdk.testing import get_target_test_class
from singer_sdk.testing.templates import TargetFileTestTemplate
from singer_sdk.testing.suites import TestSuite

from target_iceberg.target import TargetIceberg
from target_iceberg.catalog import get_catalog_config
import uuid
import pytest


from . import data_files
from importlib.resources import files


SAMPLE_CONFIG ={
    "catalog_uri": "http://localhost:8181",
    "warehouse": "demo",
    "catalog_type": "rest",
    "database": uuid.uuid4().hex,
    "s3_endpoint": "http://localhost:9000",
    "s3_access_key_id": "admin",
    "s3_secret_access_key": "password",
}

catalog = load_catalog(
    "demo",
    **{
        "uri": "http://127.0.0.1:8181",
        "s3.endpoint": "http://127.0.0.1:9000",
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
    }
)

REQUIRED_TABLES ={
    "test_array_data": Schema(
        NestedField(field_id=2,name="id", field_type=LongType()),
        NestedField(field_id=4,name="fruits", field_type=ListType(element_id=5, element_type=StringType()))
    ),
    "test_strings_in_arrays": Schema(
        NestedField(field_id=1,name="id", field_type=LongType()),
        NestedField(field_id=2,name="strings", field_type=ListType(element_id=3, element_type=StringType()))
    ),
    "test_strings_in_objects": Schema(
        NestedField(field_id=1,name="id", field_type=LongType()),
        NestedField(field_id=2,name="string", field_type=ListType(element_id=3, element_type=StringType())),
        NestedField(
            field_id=4,
            name="info",
            field_type=StructType(
                NestedField(field_id=5,name="name", field_type=StringType()),
                NestedField(field_id=6, name="value", field_type=StringType())
            ),
        ),
    ),
    "test_optional_attributes": Schema(
        NestedField(field_id=1,name="id", field_type=LongType()),
        NestedField(field_id=2,name="optional", field_type=StringType())
    ),
    "record_missing_fields": Schema(
        NestedField(field_id=1,name="id", field_type=LongType()),
        NestedField(field_id=2,name="optional", field_type=StringType())
    ),
    "test_duplicate_records": Schema(
        NestedField(field_id=1,name="id", field_type=LongType()),
        NestedField(field_id=2,name="metric", field_type=LongType())
    ),
    "test_strings": Schema(
        NestedField(field_id=1,name="id", field_type=LongType()),
        NestedField(field_id=2,name="string", field_type=StringType()),
        NestedField(field_id=3,name="info", field_type=StringType())
    ),
    "test_no_pk": Schema(
        NestedField(field_id=1,name="id", field_type=LongType()),
        NestedField(field_id=2,name="metric", field_type=LongType())
    ),
    "test_schema_updates": Schema(
        NestedField(field_id=1,name="id", field_type=LongType()),
        NestedField(field_id=2,name="a1", field_type=LongType()),
        NestedField(field_id=3,name="a2", field_type=StringType()),
    ),
    "test_object_schema_with_properties": Schema(
        NestedField(field_id=1, name="object_store", field_type=StructType(
            NestedField(field_id=2, name="id", field_type=LongType()),
            NestedField(field_id=3, name="metric", field_type=LongType())
        ))
    ),
    "test_object_schema_no_properties": Schema(
        NestedField(field_id=1, name="object_store", field_type=StructType(
            NestedField(field_id=2, name="id", field_type=LongType()),
            NestedField(field_id=3, name="metric", field_type=LongType())
        ))
    ),
    "TestCamelcase": Schema(
        NestedField(field_id=1, name="Id", field_type=StringType()),
        NestedField(field_id=2, name="clientName", field_type=StringType())
    ),
    "data_partitioning": Schema(
        NestedField(field_id=1, name="id", field_type=StringType()),
        NestedField(field_id=2, name="created_at", field_type=TimestampType())
    ),
}

class TargetPartitioningTest(TargetFileTestTemplate):
    """Test Target handles data partitioning correctly."""

    name = "data_partitioning"

    @property
    def singer_filepath(self) -> Path:
        return files(data_files) / "data_partitioning.singer"

    def test(self) -> None:
        """Run partitioning test."""
        self.runner.sync_all()

        table = catalog.load_table(f"{SAMPLE_CONFIG['database']}.data_partitioning")
        number_of_partitions = len(table.inspect.partitions())

        """In data_partitioning.singer, we currently have only three distinct fields available for partitioning"""
        assert number_of_partitions == 3, f"Expected partitions 3, but got {number_of_partitions}"


# Run standard built-in target tests from the SDK:
StandardTargetTests = get_target_test_class(
    target_class=TargetIceberg,
    config=SAMPLE_CONFIG,
    custom_suites=[
        TestSuite(
            kind="target",
            tests=[TargetPartitioningTest]
        )
    ]
)


class TestTargetIceberg(StandardTargetTests):  # type: ignore[misc, valid-type]
    """Standard Target Tests."""

    @pytest.fixture(scope="class")
    def resource(self):  # noqa: ANN201

        catalog =load_catalog(
            "demo",
            **get_catalog_config(SAMPLE_CONFIG),
        )

        for table, schema in REQUIRED_TABLES.items():

            table_creation_config = {
                "identifier": f"{SAMPLE_CONFIG['database']}.{table}",
                "schema": schema,
            }

            if table == "data_partitioning":
                partition_spec = PartitionSpec(
                    PartitionField(source_id=2, field_id=6, transform=DayTransform(), name="created_at_day")
                )
                table_creation_config["partition_spec"] = partition_spec

            catalog.create_table(**table_creation_config)
