from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import bindparam, text

from src.core.base_pipeline import BasePipeline
from src.core.registry import register_pipeline
from src.db.connection import get_session


@register_pipeline("job_feature_engineering")
class JobFeatureEngineeringPipeline(
    BasePipeline[list[dict[str, Any]], list[dict[str, Any]]]
):
    """Engineers feature columns from processed job descriptions.

    Output fields:
    - required_years
    - skills
    - tools
    - cloud_demand
    - ai_demand
    - salary
    - has_ai
    - has_cloud
    - keywords
    """

    SKILL_KEYWORDS = (
    "python",
    "sql",
    "machine learning",
    "deep learning",
    "nlp",
    "data analysis",
    "data engineering",
    "etl",
    "statistics",
    "pandas",
    "numpy",
    "scikit-learn",
    "tensorflow",
    "pytorch",
    "spark",
    "communication",
    "problem solving",
    "java",
    "c#",
    "c++",
    "c",
    "javascript",
    "typescript",
    "go",
    "rust",
    "nosql",
    "data structures",
    "algorithms",
    "kotlin",
    "swift",
    "ruby",
    "php",
    "dart",
    "r",
    "scala",
    "perl",
    "elixir",
    "haskell",
    "clojure",
    "groovy",
    "lua",
    "solidity",
    "graphql",
    "shell scripting",
    "powershell",
    "bash",
    "html",
    "css",
    "sass",
    "scss",
    "xml",
    "json",
    "yaml",
    )

    TOOL_KEYWORDS = (
    "docker",
    "kubernetes",
    "airflow",
    "git",
    "jira",
    "tableau",
    "power bi",
    "excel",
    "looker",
    "databricks",
    "snowflake",
    "postgresql",
    "mysql",
    "redshift",
    "bigquery",
    "terraform",
    "jenkins",
    "spark",
    "hadoop",
    "github actions",
    "gitlab ci",
    "circleci",
    "argo",
    "flux",
    "ansible",
    "puppet",
    "chef",
    "packer",
    "vagrant",
    "prometheus",
    "grafana",
    "datadog",
    "splunk",
    "elastic stack",
    "jaeger",
    "opentelemetry",
    "new relic",
    "dynatrace",
    "pagerduty",
    "opsgenie",
    "rootly",
    "sonarqube",
    "snyk",
    "trivy",
    "vault",
    "jfrog",
    "npm",
    "maven",
    "gradle",
    "pip",
    )

    CLOUD_KEYWORDS = (
    "aws",
    "azure",
    "gcp",
    "google cloud",
    "amazon web services",
    "cloud",
    "ec2",
    "s3",
    "lambda",
    "redshift",
    "bigquery",
    "oracle cloud",
    "ibm cloud",
    "alibaba cloud",
    "cloudfoundry",
    "heroku",
    "vercel",
    "netlify",
    "iaas",
    "paas",
    "saas",
    "serverless",
    "functions",
    "blob storage",
    "cloud storage",
    "vpc",
    "cdn",
    "cloudfront",
    "cloudflare",
    "openshift",
    "rancher",
    "crossplane",
    )

    AI_KEYWORDS = (
    "ai",
    "artificial intelligence",
    "machine learning",
    "deep learning",
    "generative ai",
    "genai",
    "llm",
    "large language model",
    "nlp",
    "computer vision",
    "rag",
    "prompt engineering",
    "langchain",
    "tensorflow",
    "pytorch",
    "transformers",
    "huggingface",
    "agentic ai",
    "ai agents",
    "multi-agent systems",
    "world models",
    "physics-based ai",
    "retrieval-augmented generation",
    "llamaindex",
    "vector databases",
    "pinecone",
    "weaviate",
    "ollama",
    "vllm",
    "triton",
    "mlflow",
    "kubeflow",
    "wandb",
    "dvc",
    "pandas",
    "numpy",
    "scikit-learn",
    "time series analysis",
    "genetic algorithms",
    "reinforcement learning",
    "quantum machine learning",
    "edge ai",
    )

    def extract(self) -> list[dict[str, Any]]:
        """Fetch unprocessed rows from ProcessingJobs."""
        batch_size = self.params.get("batch_size", 1000)
        with get_session(self.session_factory) as session:
            source = self._resolve_processing_jobs_source(session)
            rows = (
                session.execute(
                    text(
                        f"SELECT p.{source['content_hash']} AS content_hash, "
                        f"p.{source['title']} AS title, "
                        f"p.{source['location']} AS location, "
                        f"p.{source['description']} AS description, "
                        f"p.{source['apply_url']} AS apply_url "
                        f"FROM {source['table']} p "
                        f"WHERE p.{source['is_processed']} = false "
                        "LIMIT :limit"
                    ),
                    {"limit": batch_size},
                )
                .mappings()
                .all()
            )
            self.log.info("feature_rows_extracted", count=len(rows))
            return [dict(r) for r in rows]

    @staticmethod
    def _normalize_text(value: Any, *, lowercase: bool = True) -> str:
        normalized = " ".join(str(value or "").split())
        if lowercase:
            normalized = normalized.lower()
        return normalized.strip()

    @staticmethod
    def _resolve_processing_jobs_source(session: Any) -> dict[str, str]:
        if session.execute(text("SELECT to_regclass('public.\"ProcessingJobs\"')")).scalar():
            return {
                "table": '"ProcessingJobs"',
                "content_hash": '"ContentHash"',
                "is_processed": '"IsProcessed"',
                "title": '"Title"',
                "location": '"Location"',
                "description": '"Description"',
                "apply_url": '"ApplyUrl"',
            }

        if session.execute(text("SELECT to_regclass('public.processing_jobs')")).scalar():
            return {
                "table": "processing_jobs",
                "content_hash": "content_hash",
                "is_processed": "is_processed",
                "title": "title",
                "location": "location",
                "description": "description",
                "apply_url": "apply_url",
            }

        raise RuntimeError("No ProcessingJobs source table found in the database")

    @staticmethod
    def _ensure_job_features_target(session: Any) -> dict[str, str]:
        if session.execute(text("SELECT to_regclass('public.\"JobFeatures\"')")).scalar():
            return {
                "table": '"JobFeatures"',
                "content_hash": '"ContentHash"',
                "required_years": '"RequiredYears"',
                "skills": '"Skills"',
                "tools": '"Tools"',
                "cloud_demand": '"CloudDemand"',
                "ai_demand": '"AiDemand"',
                "salary": '"Salary"',
                "has_ai": '"HasAi"',
                "has_cloud": '"HasCloud"',
                "keywords": '"Keywords"',
                "executed_at": '"ExecutedAt"',
            }

        if session.execute(text("SELECT to_regclass('public.job_features')")).scalar():
            return {
                "table": "job_features",
                "content_hash": "content_hash",
                "required_years": "required_years",
                "skills": "skills",
                "tools": "tools",
                "cloud_demand": "cloud_demand",
                "ai_demand": "ai_demand",
                "salary": "salary",
                "has_ai": "has_ai",
                "has_cloud": "has_cloud",
                "keywords": "keywords",
                "executed_at": "executed_at",
            }

        session.execute(
            text(
                "CREATE TABLE IF NOT EXISTS job_features ("
                "content_hash VARCHAR(255) PRIMARY KEY, "
                "required_years INTEGER NULL, "
                "skills TEXT NULL, "
                "tools TEXT NULL, "
                "cloud_demand INTEGER NOT NULL DEFAULT 0, "
                "ai_demand INTEGER NOT NULL DEFAULT 0, "
                "salary TEXT NULL, "
                "has_ai BOOLEAN NOT NULL DEFAULT FALSE, "
                "has_cloud BOOLEAN NOT NULL DEFAULT FALSE, "
                "keywords TEXT NULL, "
                "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()"
                ")"
            )
        )
        return {
            "table": "job_features",
            "content_hash": "content_hash",
            "required_years": "required_years",
            "skills": "skills",
            "tools": "tools",
            "cloud_demand": "cloud_demand",
            "ai_demand": "ai_demand",
            "salary": "salary",
            "has_ai": "has_ai",
            "has_cloud": "has_cloud",
            "keywords": "keywords",
            "executed_at": "executed_at",
        }

    @classmethod
    def _extract_required_years(cls, text_value: str) -> int | None:
        patterns = (
            r"(\d+)\s*\+?\s*(?:years?|yrs?)\s+of\s+experience",
            r"(\d+)\s*(?:-|to)\s*(\d+)\s*(?:years?|yrs?)",
            r"(?:minimum|min\.?|at least)\s*(\d+)\s*(?:years?|yrs?)",
            r"(\d+)\s*\+?\s*(?:years?|yrs?)",
        )
        for pattern in patterns:
            match = re.search(pattern, text_value, flags=re.IGNORECASE)
            if not match:
                continue
            value = match.group(1)
            return int(value) if value.isdigit() else None
        return None

    @staticmethod
    def _extract_salary(text_value: str) -> str | None:
        patterns = (
            r"([$€£]\s?\d[\d,]*(?:\.\d+)?\s*(?:-|to)\s*[$€£]?\s?\d[\d,]*(?:\.\d+)?(?:\s*(?:per year|per annum|annually|/year))?)",
            r"([$€£]\s?\d[\d,]*(?:\.\d+)?(?:k|K)?(?:\s*(?:per year|per annum|annually|/year|/hour))?)",
            r"(\d+(?:\.\d+)?\s*(?:lpa|lac|lakhs?|crore|cr|k)\s*(?:-|to)?\s*\d*(?:\.\d+)?\s*(?:lpa|lac|lakhs?|crore|cr|k)?)",
        )
        for pattern in patterns:
            match = re.search(pattern, text_value, flags=re.IGNORECASE)
            if match:
                return " ".join(match.group(1).split())
        return None

    @classmethod
    def _find_keywords(cls, text_value: str, keywords: tuple[str, ...]) -> list[str]:
        if not keywords:
            return []

        cache = getattr(cls, "_keyword_pattern_cache", None)
        if cache is None:
            cache = {}
            setattr(cls, "_keyword_pattern_cache", cache)

        compiled_entry = cache.get(keywords)
        if compiled_entry is None:
            group_name_to_keyword: dict[str, str] = {}
            pattern_parts: list[str] = []
            for index, keyword in enumerate(keywords):
                group_name = f"keyword_{index}"
                group_name_to_keyword[group_name] = keyword
                pattern_parts.append(
                    rf"(?P<{group_name}>(?<!\w){re.escape(keyword)}(?!\w))"
                )

            compiled_entry = (
                re.compile("|".join(pattern_parts), flags=re.IGNORECASE),
                group_name_to_keyword,
            )
            cache[keywords] = compiled_entry

        pattern, group_name_to_keyword = compiled_entry
        found_keywords = {
            group_name_to_keyword[match.lastgroup]
            for match in pattern.finditer(text_value)
            if match.lastgroup is not None
        }
        return [keyword for keyword in keywords if keyword in found_keywords]
    def transform(self, raw_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Derive engineered features from normalized job text."""
        if not raw_data:
            return []

        clean_data: list[dict[str, Any]] = []
        for row in raw_data:
            description = self._normalize_text(row.get("description"))
            title = self._normalize_text(row.get("title"))
            location = self._normalize_text(row.get("location"), lowercase=False)
            apply_url = self._normalize_text(row.get("apply_url"), lowercase=False)
            content_hash = self._normalize_text(
                row.get("content_hash"),
                lowercase=False,
            )

            combined_text = " ".join(
                part for part in [title, location.lower(), description, apply_url.lower()] if part
            )
            skills = self._find_keywords(combined_text, self.SKILL_KEYWORDS)
            tools = self._find_keywords(combined_text, self.TOOL_KEYWORDS)
            cloud_hits = self._find_keywords(combined_text, self.CLOUD_KEYWORDS)
            ai_hits = self._find_keywords(combined_text, self.AI_KEYWORDS)
            all_keywords = list(dict.fromkeys(skills + tools + cloud_hits + ai_hits))

            clean_data.append(
                {
                    "content_hash": content_hash,
                    "required_years": self._extract_required_years(combined_text),
                    "skills": ", ".join(skills),
                    "tools": ", ".join(tools),
                    "cloud_demand": len(cloud_hits),
                    "ai_demand": len(ai_hits),
                    "salary": self._extract_salary(combined_text),
                    "has_ai": bool(ai_hits),
                    "has_cloud": bool(cloud_hits),
                    "keywords": ", ".join(all_keywords) if all_keywords else None,
                    "executed_at": datetime.now(timezone.utc),
                }
            )

        self.log.info("feature_rows_engineered", count=len(clean_data))
        return clean_data

    def load(self, clean_data: list[dict[str, Any]]) -> None:
        """Upsert engineered features into the job_features table."""
        if not clean_data:
            self.log.info("no_feature_data_to_load")
            return

        with get_session(self.session_factory) as session:
            source = self._resolve_processing_jobs_source(session)
            target = self._ensure_job_features_target(session)
            upsert_stmt = text(
                f"INSERT INTO {target['table']} ("
                f"{target['content_hash']}, {target['required_years']}, {target['skills']}, {target['tools']}, "
                f"{target['cloud_demand']}, {target['ai_demand']}, {target['salary']}, {target['has_ai']}, {target['has_cloud']}, {target['keywords']}, {target['executed_at']}"
                ") VALUES ("
                ":content_hash, :required_years, :skills, :tools, :cloud_demand, :ai_demand, :salary, :has_ai, :has_cloud, :keywords, :executed_at"
                ") "
                f"ON CONFLICT ({target['content_hash']}) DO UPDATE SET "
                f"{target['required_years']} = EXCLUDED.{target['required_years']}, "
                f"{target['skills']} = EXCLUDED.{target['skills']}, "
                f"{target['tools']} = EXCLUDED.{target['tools']}, "
                f"{target['cloud_demand']} = EXCLUDED.{target['cloud_demand']}, "
                f"{target['ai_demand']} = EXCLUDED.{target['ai_demand']}, "
                f"{target['salary']} = EXCLUDED.{target['salary']}, "
                f"{target['has_ai']} = EXCLUDED.{target['has_ai']}, "
                f"{target['has_cloud']} = EXCLUDED.{target['has_cloud']}, "
                f"{target['keywords']} = EXCLUDED.{target['keywords']}, "
                f"{target['executed_at']} = EXCLUDED.{target['executed_at']}"
            )
            session.execute(upsert_stmt, clean_data)
            processed_hashes = [record["content_hash"] for record in clean_data]
            upserted = len(clean_data)

            unique_hashes = sorted({value for value in processed_hashes if value})
            if unique_hashes:
                session.execute(
                    text(
                        f"UPDATE {source['table']} "
                        f"SET {source['is_processed']} = true "
                        f"WHERE {source['content_hash']} IN :content_hashes"
                    ).bindparams(bindparam("content_hashes", expanding=True)),
                    {"content_hashes": unique_hashes},
                )

            self.log.info("feature_load_completed", upserted=upserted)