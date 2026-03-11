from __future__ import annotations

from typing import Any

import httpx

from src.core.base_pipeline import BasePipeline
from src.core.registry import register_pipeline


@register_pipeline("example_scraper")
class ExampleScraper(BasePipeline[list[dict[str, Any]], list[dict[str, Any]]]):
    """Demonstrates a scraping pipeline.

    Replace the extract/transform/load logic with your actual scraping code.
    The ``params`` dict comes from ``pipelines.yaml``::

        params:
          target_url: "https://example.com/jobs"
          max_pages: 10
    """

    def extract(self) -> list[dict[str, Any]]:
        url = self.params["target_url"]
        self.log.info("fetching_page", url=url)
        response = httpx.get(url, timeout=30)
        response.raise_for_status()
        # TODO: replace with real HTML parsing (e.g. BeautifulSoup)
        return [{"title": "Example Job", "company": "Acme Corp", "source": url}]

    def transform(self, raw_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {**record, "title": record["title"].strip().lower()}
            for record in raw_data
        ]

    def load(self, clean_data: list[dict[str, Any]]) -> None:
        session = self.session_factory()
        try:
            # TODO: replace with actual ORM insert or bulk operation
            for record in clean_data:
                self.log.info("would_insert_record", record=record)
            session.commit()
            self.log.info("loaded_records", count=len(clean_data))
        finally:
            session.close()
