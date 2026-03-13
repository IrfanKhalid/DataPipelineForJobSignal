from __future__ import annotations

from typing import Any

from sqlalchemy import bindparam, text

from src.core.base_pipeline import BasePipeline
from src.core.registry import register_pipeline
from src.db.connection import get_session


@register_pipeline("job_processing")
class JobProcessingPipeline(BasePipeline[list[dict[str, Any]], list[dict[str, Any]]]):
    """Fetches unprocessed rows from JobDetails, deduplicates by
    (jobs_ids, apply_url), and inserts unique jobs into ProcessingJobs.

    YAML params::

        params:
          batch_size: 1000
    """

    def extract(self) -> list[dict[str, Any]]:
        """Fetch all JobDetails rows where IsProcessed = false."""
        batch_size = self.params.get("batch_size", 1000)
        with get_session(self.session_factory) as session:
            rows = (
                session.execute(
                    text(
                        'SELECT "Id", "Title", "Location", '
                        '"Description", "ApplyUrl", "Responsibilities", "Achievements", "Requirements", "Compensation" '
                        'FROM "JobDetails" '
                        'WHERE "IsProcessed" = false '
                        "LIMIT :limit"
                    ),
                    {"limit": batch_size},
                )
                .mappings()
                .all()
            )
            self.log.info("extracted_unprocessed_jobs", count=len(rows))
            return [dict(r) for r in rows]

    @staticmethod
    def _normalize_text(value: Any, *, lowercase: bool = True) -> str:
        """Trim and normalize whitespace for text values."""
        normalized = " ".join(str(value or "").split())
        if lowercase:
            normalized = normalized.lower()
        return normalized.strip()

    @classmethod
    def _merge_description(cls, row: dict[str, Any]) -> str:
        """Merge description-related columns into a single lowercase,
        trimmed string."""
        parts = [
            cls._normalize_text(row.get("Description")),
            cls._normalize_text(row.get("Responsibilities")),
            cls._normalize_text(row.get("Achievements")),
            cls._normalize_text(row.get("Requirements")),
            cls._normalize_text(row.get("Compensation")),
        ]
        return " ".join(part for part in parts if part)

    @classmethod
    def _normalize_record_for_insert(cls, record: dict[str, Any]) -> dict[str, Any]:
        """Return a normalized ProcessingJobs payload for insert checks and writes."""
        return {
            "jobs_ids": cls._normalize_text(record.get("jobs_ids"), lowercase=False),
            "title": cls._normalize_text(record.get("title")),
            "location": cls._normalize_text(record.get("location")),
            "description": cls._normalize_text(record.get("description")),
            "apply_url": cls._normalize_text(record.get("apply_url"), lowercase=False),
        }

    @staticmethod
    def _resolve_processing_jobs_target(session: Any) -> dict[str, str]:
        """Resolve the actual target table/column names present in the database."""
        if session.execute(text("SELECT to_regclass('public.\"ProcessingJobs\"')")).scalar():
            return {
                "table": '"ProcessingJobs"',
                "id": '"Id"',
                "jobs_ids": '"JobsIds"',
                "title": '"Title"',
                "location": '"Location"',
                "description": '"Description"',
                "apply_url": '"ApplyUrl"',
            }

        if session.execute(text("SELECT to_regclass('public.processing_jobs')")).scalar():
            return {
                "table": "processing_jobs",
                "id": "id",
                "jobs_ids": "jobs_ids",
                "title": "title",
                "location": "location",
                "description": "description",
                "apply_url": "apply_url",
            }

        raise RuntimeError("No ProcessingJobs target table found in the database")

    def transform(self, raw_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Deduplicate by (jobs_ids, apply_url), keeping first occurrence.
        Merges Description, Responsibilities, Achievements, Requirements,
        and Compensation into a single description field."""
        if not raw_data:
            return []

        unique_by_key: dict[tuple[str, str], dict[str, Any]] = {}

        for row in raw_data:
            source_id = self._normalize_text(row.get("Id"), lowercase=False)
            record = self._normalize_record_for_insert(
                {
                    "jobs_ids": source_id,
                    "title": row.get("Title"),
                    "location": row.get("Location"),
                    "description": self._merge_description(row),
                    "apply_url": row.get("ApplyUrl"),
                }
            )
            key = (record["jobs_ids"], record["apply_url"])
            existing = unique_by_key.get(key)
            if existing is None:
                unique_by_key[key] = {**record, "source_ids": [source_id]}
            else:
                existing.setdefault("source_ids", []).append(source_id)

        unique = list(unique_by_key.values())

        self.log.info(
            "deduplicated_jobs",
            raw_count=len(raw_data),
            unique_count=len(unique),
        )
        return unique

    def load(self, clean_data: list[dict[str, Any]]) -> None:
        """Insert unique jobs into ProcessingJobs and mark source rows
        as processed, all within a single transaction."""
        if not clean_data:
            self.log.info("no_data_to_load")
            return

        with get_session(self.session_factory) as session:
            target = self._resolve_processing_jobs_target(session)
            inserted = 0
            source_ids: list[str] = []
            for raw_record in clean_data:
                source_ids.extend(raw_record.get("source_ids", []))
                record = self._normalize_record_for_insert(raw_record)
                # Skip if this (jobs_ids, apply_url) already exists
                exists = session.execute(
                    text(
                        f"SELECT 1 FROM {target['table']} "
                        f"WHERE {target['jobs_ids']} = :jobs_ids AND {target['apply_url']} = :apply_url "
                        "LIMIT 1"
                    ),
                    {
                        "jobs_ids": record["jobs_ids"],
                        "apply_url": record["apply_url"],
                    },
                ).first()

                if exists:
                    continue

                session.execute(
                    text(
                        f"INSERT INTO {target['table']} "
                        f"({target['id']}, {target['jobs_ids']}, {target['title']}, {target['location']}, {target['description']}, {target['apply_url']}) "
                        "VALUES ("
                        "gen_random_uuid(), "
                        ":jobs_ids, :title, :location, :description, :apply_url)"
                    ),
                    record,
                )
                inserted += 1

            # Mark only successfully extracted source rows as processed
            unique_source_ids = sorted({source_id for source_id in source_ids if source_id})
            if unique_source_ids:
                session.execute(
                    text(
                        'UPDATE "JobDetails" '
                        'SET "IsProcessed" = true '
                        'WHERE "Id" IN :source_ids'
                    ).bindparams(bindparam("source_ids", expanding=True)),
                    {"source_ids": unique_source_ids},
                )

            self.log.info(
                "load_completed",
                inserted=inserted,
                skipped_duplicates=len(clean_data) - inserted,
            )
