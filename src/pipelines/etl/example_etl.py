from __future__ import annotations

from typing import Any

from sqlalchemy import text

from src.core.base_pipeline import BasePipeline
from src.core.registry import register_pipeline


@register_pipeline("example_etl")
class ExampleETL(BasePipeline[list[dict[str, Any]], list[dict[str, Any]]]):
    """Demonstrates an ETL pipeline that reads from one table and writes to another.

    The ``params`` dict comes from ``pipelines.yaml``::

        params:
          source_table: "raw_jobs"
          target_table: "processed_jobs"
          batch_size: 500
    """

    def extract(self) -> list[dict[str, Any]]:
        source = self.params["source_table"]
        batch_size = self.params.get("batch_size", 1000)
        session = self.session_factory()
        try:
            rows = (
                session.execute(
                    text(
                        f"SELECT * FROM {source} "  # noqa: S608
                        "WHERE processed = false LIMIT :limit"
                    ),
                    {"limit": batch_size},
                )
                .mappings()
                .all()
            )
            self.log.info("extracted_rows", count=len(rows))
            return [dict(r) for r in rows]
        finally:
            session.close()

    def transform(self, raw_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        # TODO: implement your business logic — normalize, deduplicate, enrich
        return raw_data

    def load(self, clean_data: list[dict[str, Any]]) -> None:
        if not clean_data:
            self.log.info("no_data_to_load")
            return

        target = self.params["target_table"]
        session = self.session_factory()
        try:
            for record in clean_data:
                session.execute(
                    text(f"INSERT INTO {target} (data) VALUES (:data)"),  # noqa: S608
                    {"data": str(record)},
                )
            session.commit()
            self.log.info("loaded_to_target", table=target, count=len(clean_data))
        finally:
            session.close()
