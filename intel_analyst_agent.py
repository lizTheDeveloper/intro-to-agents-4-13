#!/usr/bin/env python3
"""
Intel analyst agent: after hiring research + news ingestion, pull Postgres + pgvector context
and produce a synthesized executive brief (Groq/OpenAI-compatible API by default).

Example:
  python intel_analyst_agent.py --bundle-id 1 --output reports/bundle_1.md
  python intel_analyst_agent.py --bundle-id 1 --no-llm --output dossier.json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from langfuse.openai import OpenAI

from intel_analyst_gather import compact_intel_dossier, gather_bundle_intel
from langfuse_tracing import trace_agent_session

logger = logging.getLogger("intro_agents.intel_analyst_agent")

_SYSTEM_PROMPT = """You are a senior executive intelligence analyst.
You receive structured JSON: hiring hypotheses (signals, claims with citations, scores),
target role profile, and recent/semantic news excerpts per company.
Write a clear, decision-ready brief for an executive job seeker.
Use markdown with sections: Executive summary; Company-by-company (hypothesis, evidence, news alignment, risks);
Cross-cutting themes; Contradictions or gaps; Suggested next actions (outreach, diligence, people to verify).
Be explicit when evidence is weak or missing. Do not invent facts beyond the JSON."""


def _groq_client() -> OpenAI:
    return OpenAI(
        api_key=os.environ.get("GROQ_API_KEY"),
        base_url="https://api.groq.com/openai/v1",
    )


def synthesize_brief(
    dossier_compact: dict[str, Any],
    model: str,
    max_completion_tokens: int,
    bundle_id: int,
) -> str:
    client = _groq_client()
    payload = json.dumps(dossier_compact, ensure_ascii=False, default=str)
    with trace_agent_session("intel_analyst_agent", bundle_id=bundle_id, model=model):
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": "Synthesize the following dossier JSON into the requested markdown brief.\n\n"
                    + payload,
                },
            ],
            max_tokens=max_completion_tokens,
            temperature=0.35,
            name="intel_analyst_brief",
            metadata={"agent": "intel_analyst", "bundle_id": bundle_id},
        )
    message = response.choices[0].message
    return (message.content or "").strip()


def main() -> None:
    load_dotenv()
    from logging_config import configure_logging
    configure_logging()
    parser = argparse.ArgumentParser(description="Synthesize hiring intel + news for one or more research bundles.")
    bundle_group = parser.add_mutually_exclusive_group(required=True)
    bundle_group.add_argument("--bundle-id", type=int, help="Analyze a single bundle")
    bundle_group.add_argument(
        "--bundle-ids", type=str,
        help="Comma-separated bundle IDs to synthesize across (e.g. 1,2,3)",
    )
    bundle_group.add_argument(
        "--latest-bundles", type=int, metavar="N",
        help="Synthesize across the N most recent bundles",
    )
    parser.add_argument("--output", type=str, default="-", help="Write markdown or JSON path, or - for stdout")
    parser.add_argument("--no-llm", action="store_true", help="Only write compact JSON dossier (no Groq call)")
    parser.add_argument(
        "--model",
        default=os.environ.get("INTEL_ANALYST_MODEL", "qwen/qwen3-32b"),
        help="Groq chat model id for synthesis",
    )
    parser.add_argument("--max-tokens", type=int, default=int(os.environ.get("INTEL_ANALYST_MAX_TOKENS", "8192")))
    parser.add_argument("--min-priority", type=float, default=None, help="Filter leads by overall_priority >=")
    parser.add_argument("--lead-limit", type=int, default=40)
    parser.add_argument("--news-per-company", type=int, default=8)
    parser.add_argument(
        "--no-semantic-news",
        action="store_true",
        help="Skip MLX embeddings; use only recency-ordered news headlines/snippets",
    )
    args = parser.parse_args()
    if not (os.environ.get("DATABASE_URL") or os.environ.get("HIRING_INTEL_DATABASE_URL")):
        logger.error("DATABASE_URL is not set.")
        sys.exit(1)

    import hiring_intel_store

    if args.bundle_id is not None:
        target_bundle_ids = [args.bundle_id]
    elif args.bundle_ids is not None:
        target_bundle_ids = [int(bid.strip()) for bid in args.bundle_ids.split(",") if bid.strip()]
    else:
        listing = hiring_intel_store.bundles_list(limit=args.latest_bundles)
        target_bundle_ids = [row["bundle_id"] for row in listing["bundles"]]
        if not target_bundle_ids:
            logger.error("No bundles found in the database.")
            sys.exit(2)
        logger.info("Using latest %d bundle(s): %s", len(target_bundle_ids), target_bundle_ids)

    bundle_arg = target_bundle_ids[0] if len(target_bundle_ids) == 1 else target_bundle_ids
    dossier = gather_bundle_intel(
        bundle_arg,
        lead_limit=args.lead_limit,
        minimum_overall_priority=args.min_priority,
        news_per_company=args.news_per_company,
        use_semantic_news=not args.no_semantic_news,
    )
    if dossier.get("error"):
        logger.error("%s", dossier)
        sys.exit(2)
    compact = compact_intel_dossier(dossier)
    primary_bundle_id = target_bundle_ids[0]
    if args.no_llm:
        body = json.dumps(compact, indent=2, ensure_ascii=False, default=str)
    else:
        if not os.environ.get("GROQ_API_KEY"):
            logger.error("GROQ_API_KEY is not set (required unless --no-llm).")
            sys.exit(1)
        body = synthesize_brief(
            compact,
            model=args.model,
            max_completion_tokens=args.max_tokens,
            bundle_id=primary_bundle_id,
        )
    if args.output == "-":
        print(body)
    else:
        path = Path(args.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body, encoding="utf-8")
        logger.info("Wrote %s", path)


if __name__ == "__main__":
    main()
