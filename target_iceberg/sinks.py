"""Iceberg target sink class, which handles writing streams."""

from __future__ import annotations

import logging

import pyarrow as pa
from pyiceberg.catalog import load_catalog

from singer_sdk import Target
from singer_sdk.sinks import BatchSink

from target_iceberg.catalog import get_catalog_config

logger = logging.getLogger("iceberg_sink")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

BATCH_SIZE = 10000

class IcebergSink(BatchSink):
    """Iceberg target sink class."""

    batch_max_size = BATCH_SIZE

    def __init__(
        self,
        target: Target,
        stream_name: str,
        schema: dict,
        key_properties: list[str] | None,
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written.

        Args:
            context: Stream partition or context dictionary.
        """
        catalog = load_catalog(
            self.config.get("catalog_name"), **get_catalog_config(self.config)
        )

        if context.get("records"):

            if table := catalog.load_table(f"{self.config['database']}.{self.stream_name}"):

                logger.info(
                    "Appending to table", extra={"table_name": self.stream_name}
                )

                schema = table.schema()

                records: pa.Table = pa.Table.from_pylist(context["records"])

                records = records.cast(schema.as_arrow())
                table.append(records)

            else:
                msg = f"Table {self.stream_name} should exist in database"
                raise ValueError(msg)
