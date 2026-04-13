#!/usr/bin/env python3
"""
Orchestrator: hiring research (Groq + tools) → news ingest (Tavily + MLX) per discovered
company → intel analyst brief.

Data **accumulates** across runs. Each run creates a new bundle. Companies, funding rounds,
and news articles are shared and updated; leads and claims are scoped to their bundle.

Use --reset only when you explicitly need a clean slate (e.g. after schema changes or to
discard test data).

Example (accrual — the normal mode):
  python run_intel_pipeline.py \\
    --search "US B2B SaaS firms that raised Series B in 2024-2026 and may need a CFO" \\
    --report reports/latest_intel.md

Example (clean start):
  python run_intel_pipeline.py --reset \\
    --search "EdTech companies with CTO openings" \\
    --report reports/latest_intel.md
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

import hiring_intel_store
import intel_db_reset
import intel_schema_queue_store
import prompting
from langfuse_tracing import pipeline_bundle_context
from news_ingestion_agent import ingest_company_news

logger = logging.getLogger("intro_agents.run_intel_pipeline")


def _research_prompt(bundle_id: int, search_focus: str, preferred_company_slug: str | None) -> str:
    slug_hint = ""
    if preferred_company_slug:
        slug_hint = (
            f"\nOptional anchor: for your strongest hypothesis involving a known firm, you may use "
            f'intel_company_upsert with company_id exactly "{preferred_company_slug}" for that company only.\n'
        )
    return f"""You are the executive hiring signals research agent (agent 1). The user does **not** know which
companies matter yet — you must **discover** them from the open web (funding, scale, exec churn, board pressure,
category dynamics). Roles may **not** be posted; infer where a CFO/COO/CTO/etc. is likely needed soon.

Fixed bundle_id for ALL intel writes: {bundle_id}.
Research focus: {search_focus}
{slug_hint}
Company identity rules:
- For every company you take seriously, call intel_company_upsert with a **stable lowercase slug** `company_id`
  (ASCII, hyphens ok), e.g. `acme-analytics` from "Acme Analytics, Inc." and legal_name_best_effort from sources.
- Every intel_lead_create must use an existing company slug as primary_company_id — only companies you upserted.
- **lead_id format**: Always prefix with `b{{bundle_id}}-`, e.g. `b{bundle_id}-acme-cfo-hypothesis`.
  This prevents collisions with leads from earlier research runs.
- **claim_uuid format**: Always prefix with `b{{bundle_id}}-`, e.g. `b{bundle_id}-acme-series-b-claim`.

Data accumulates across runs. Companies and funding rounds are shared; leads and claims are scoped to your bundle.

Workflow:
1. intel_initialize_database once (idempotent).
2. intel_target_profile_put for this bundle_id (role_family, domains, stages from the focus).
3. web_search, web_extract, web_research as needed to find **candidates** and evidence.
4. intel_company_upsert for each distinct discovered company.
5. intel_funding_round_upsert when funding is credible.
6. intel_lead_create for each strong **pre-posting** hiring hypothesis (bundle_id + primary company slug).
7. intel_lead_link_funding_context, intel_signal_add, intel_claim_add (with citations), intel_lead_add_primary_sources,
   intel_executive_motion_add, intel_interview_prep_put when you have signal.

Prefer signals over job boards. Finish with bullet list of `lead_id` values you stored and a short summary."""


def _ingest_news_for_bundle(
    bundle_id: int,
    max_articles: int,
    search_depth: str,
    max_results: int,
    max_companies: int,
    extra_company_slug: str | None,
    extra_display_name: str | None,
) -> list[dict[str, object]]:
    """Run Tavily+MLX news ingest for each company linked to this bundle's leads."""
    if not os.environ.get("TAVILY_API_KEY"):
        logger.error("TAVILY_API_KEY required for news step.")
        sys.exit(1)
    rows = hiring_intel_store.companies_for_bundle_news(bundle_id)
    targets: list[tuple[str, str]] = []
    seen: set[str] = set()
    for row in rows:
        slug = row.get("company_id")
        if not isinstance(slug, str) or not slug.strip():
            continue
        name = row.get("legal_name_best_effort")
        display = name if isinstance(name, str) and name.strip() else slug
        if slug not in seen:
            seen.add(slug)
            targets.append((slug, display))
    if extra_company_slug:
        display = extra_display_name or extra_company_slug
        if extra_company_slug not in seen:
            hiring_intel_store.company_upsert(
                extra_company_slug,
                display,
                one_liner="Pipeline user-specified company (optional anchor).",
            )
            targets.insert(0, (extra_company_slug, display))
            seen.add(extra_company_slug)
    if not targets:
        logger.warning("No companies linked to bundle %s — skipping news (researcher may not have stored leads).", bundle_id)
        return []
    targets = targets[:max(1, max_companies)]
    summaries: list[dict[str, object]] = []
    for company_slug, display_name in targets:
        logger.info("News ingest: %s (%s)", company_slug, display_name)
        try:
            summary = ingest_company_news(
                company_slug=company_slug,
                display_name=display_name,
                max_articles=max_articles,
                search_depth=search_depth,
                max_results=max_results,
            )
            summaries.append({"company_slug": company_slug, **summary})
        except Exception as exc:
            logger.exception("News ingest failed for %s", company_slug)
            summaries.append({"company_slug": company_slug, "error": str(exc)})
    return summaries


def main() -> None:
    load_dotenv()
    from logging_config import configure_logging
    configure_logging(include_timestamps=True)
    parser = argparse.ArgumentParser(
        description="Run research → news → analyst pipeline. Data accumulates across runs."
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Truncate all intel_* tables before running (use sparingly — data accumulates by default).",
    )
    parser.add_argument("--search", required=True, help="Natural-language research focus for agent 1")
    parser.add_argument(
        "--company-slug",
        default=None,
        help="Optional: extra company slug to upsert+prioritize for news (when you already know one name).",
    )
    parser.add_argument(
        "--display-name",
        default=None,
        help="Display name for optional --company-slug (Tavily news query).",
    )
    parser.add_argument("--research-model", default=os.environ.get("PIPELINE_RESEARCH_MODEL", "qwen/qwen3-32b"))
    parser.add_argument("--max-articles", type=int, default=8)
    parser.add_argument(
        "--max-news-companies",
        type=int,
        default=int(os.environ.get("PIPELINE_MAX_NEWS_COMPANIES", "15")),
        help="Cap how many distinct companies get a Tavily news pass (cost control).",
    )
    parser.add_argument("--report", type=str, default="reports/pipeline_intel.md", help="Analyst markdown output path")
    parser.add_argument("--skip-news", action="store_true")
    parser.add_argument("--skip-analyst", action="store_true")
    parser.add_argument("--skip-research", action="store_true", help="Only news + analyst; bundle_id must exist")
    parser.add_argument("--bundle-id", type=int, default=None, help="When --skip-research, use this bundle id")
    args = parser.parse_args()

    if not (os.environ.get("DATABASE_URL") or os.environ.get("HIRING_INTEL_DATABASE_URL")):
        logger.error("DATABASE_URL not set.")
        sys.exit(1)

    if args.reset:
        intel_db_reset.truncate_all_intel_tables()

    bundle_id: int
    if args.skip_research:
        if args.bundle_id is None:
            logger.error("--skip-research requires --bundle-id")
            sys.exit(2)
        bundle_id = int(args.bundle_id)
    else:
        if not os.environ.get("GROQ_API_KEY"):
            logger.error("GROQ_API_KEY required for research step.")
            sys.exit(1)
        hiring_intel_store.initialize_database()
        from datetime import datetime, timezone
        bundle_row = hiring_intel_store.bundle_create(
            generated_at_utc=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            agent_name="pipeline-run",
            search_focus=args.search,
            limitations=["automated_pipeline"],
        )
        bundle_id = int(bundle_row["bundle_id"])
        logger.info("Created bundle_id=%s", bundle_id)

    with pipeline_bundle_context(
        bundle_id,
        search_focus=args.search,
        skip_research=args.skip_research,
    ):
        if not args.skip_research:
            prompting.reset_conversation()
            research_prompt = _research_prompt(bundle_id, args.search, args.company_slug)
            logger.info("Starting research agent (Groq)...")
            research_output = prompting.prompt(research_prompt, model=args.research_model)
            logger.info("Research agent finished (%d chars)", len(research_output or ""))
            Path("reports").mkdir(parents=True, exist_ok=True)
            Path("reports/research_transcript.txt").write_text(research_output or "", encoding="utf-8")

        if not args.skip_news:
            news_summaries = _ingest_news_for_bundle(
                bundle_id=bundle_id,
                max_articles=args.max_articles,
                search_depth=os.environ.get("AGENT_TAVILY_SEARCH_DEPTH", "advanced"),
                max_results=12,
                max_companies=args.max_news_companies,
                extra_company_slug=args.company_slug,
                extra_display_name=args.display_name,
            )
            Path("reports").mkdir(parents=True, exist_ok=True)
            Path("reports/news_ingest_summary.json").write_text(
                json.dumps(news_summaries, indent=2, default=str),
                encoding="utf-8",
            )
            if news_summaries:
                logger.info("News ingest finished for %d company/companies", len(news_summaries))

        if not args.skip_analyst:
            if not os.environ.get("GROQ_API_KEY"):
                logger.error("GROQ_API_KEY required for analyst step.")
                sys.exit(1)
            report_path = Path(args.report)
            report_path.parent.mkdir(parents=True, exist_ok=True)
            analyst_cmd = [
                sys.executable,
                str(Path(__file__).resolve().parent / "intel_analyst_agent.py"),
                "--bundle-id",
                str(bundle_id),
                "--output",
                str(report_path),
            ]
            logger.info("Running analyst: %s", " ".join(analyst_cmd))
            subprocess.run(analyst_cmd, check=True)
            logger.info("Wrote %s", report_path)
        else:
            logger.warning("Skipping analyst.")

        queue_result = intel_schema_queue_store.export_pending_schema_requests()
        logger.info("Schema change queue export: %s", queue_result)

    print(
        json.dumps(
            {"bundle_id": bundle_id, "report": args.report, "schema_queue_export": queue_result},
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
