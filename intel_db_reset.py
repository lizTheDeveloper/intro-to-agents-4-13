"""
Reset all hiring-intel + news vector tables (PostgreSQL). Scope: only `intel_*` tables in this project.
"""

from __future__ import annotations

import logging

from db_connection import connect as _connect
from langfuse_tracing import trace_postgres_write

logger = logging.getLogger("intro_agents.intel_db_reset")


def truncate_all_intel_tables() -> dict[str, str]:
    """
    Remove every row from intel_* tables. Keeps schema, extensions, and indexes.
    """
    sql = """
    TRUNCATE TABLE
      intel_schema_change_queue,
      intel_company_news_chunk,
      intel_company_news_article,
      intel_claim_citation,
      intel_claim,
      intel_citation,
      intel_lead_primary_source,
      intel_interview_prep,
      intel_executive_motion,
      intel_signal,
      intel_lead_related_company,
      intel_lead,
      intel_funding_round,
      intel_target_profile,
      intel_bundle,
      intel_company
    RESTART IDENTITY CASCADE;
    """
    with trace_postgres_write("truncate_all_intel_tables"):
        with _connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)
    logger.warning("Truncated all intel_* tables (full reset).")
    return {"ok": "truncated", "scope": "intel_*"}


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    from logging_config import configure_logging
    configure_logging()
    print(truncate_all_intel_tables())
