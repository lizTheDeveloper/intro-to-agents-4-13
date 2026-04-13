"""
Collect hiring-intel leads (Postgres) and company news (pgvector / recency) into one dossier for synthesis.
"""

from __future__ import annotations

import copy
import logging
import os
from typing import Any, Optional

import hiring_intel_store
import news_vector_store

logger = logging.getLogger("intro_agents.intel_analyst_gather")


def _default_news_query() -> str:
    return os.environ.get(
        "INTEL_ANALYST_NEWS_QUERY",
        "Executive leadership, CFO CEO hiring, funding rounds, acquisitions, layoffs, strategy, and regulatory news.",
    )


def gather_bundle_intel(
    bundle_id: int | list[int],
    lead_limit: int = 50,
    minimum_overall_priority: Optional[float] = None,
    news_per_company: int = 8,
    use_semantic_news: bool = True,
    news_query: Optional[str] = None,
) -> dict[str, Any]:
    """
    Load bundle metadata, target profile, full lead records, and per-company news context.

    bundle_id can be a single int or a list of ints to synthesize across multiple bundles
    (accumulated data). When use_semantic_news is True, MLX embeddings are used (falls back
    to recency on failure).
    """
    bundle_ids = [bundle_id] if isinstance(bundle_id, int) else list(bundle_id)
    query = news_query or _default_news_query()

    bundles_detail: list[dict[str, Any]] = []
    for bid in bundle_ids:
        detail = hiring_intel_store.bundle_get_detail(bid)
        if detail.get("error"):
            logger.warning("Skipping bundle %d: %s", bid, detail)
            continue
        bundles_detail.append(detail)
    if not bundles_detail:
        return {"error": "no_valid_bundles", "bundle_ids": bundle_ids}

    full_leads: list[dict[str, Any]] = []
    seen_lead_ids: set[str] = set()
    for bid in bundle_ids:
        listing = hiring_intel_store.leads_list(
            bid,
            minimum_overall_priority=minimum_overall_priority,
            limit=lead_limit,
        )
        for row in listing["leads"]:
            lead_id = row["lead_id"]
            if lead_id in seen_lead_ids:
                continue
            seen_lead_ids.add(lead_id)
            payload = hiring_intel_store.lead_get_full(lead_id)
            if payload.get("error"):
                logger.warning("Skipping lead %s: %s", lead_id, payload)
                continue
            full_leads.append(payload)
    slugs: list[str] = []
    seen: set[str] = set()
    for lead_payload in full_leads:
        slug = lead_payload.get("primary_company_slug")
        if isinstance(slug, str) and slug and slug not in seen:
            seen.add(slug)
            slugs.append(slug)
    news_by_company: dict[str, Any] = {}
    for company_slug in slugs:
        if use_semantic_news:
            try:
                from mlx_qwen_embedder import embed_texts

                vector = embed_texts([query], normalize=True)[0]
                semantic = news_vector_store.news_semantic_search(
                    company_slug,
                    query,
                    vector,
                    limit=news_per_company,
                )
                if semantic.get("error"):
                    logger.info("Semantic news miss for %s, using recency", company_slug)
                    news_by_company[company_slug] = news_vector_store.news_recent_articles(
                        company_slug, limit=news_per_company
                    )
                else:
                    semantic["mode"] = "semantic"
                    news_by_company[company_slug] = semantic
            except Exception as exc:
                logger.warning("MLX semantic news failed for %s (%s); using recency", company_slug, exc)
                news_by_company[company_slug] = news_vector_store.news_recent_articles(
                    company_slug, limit=news_per_company
                )
        else:
            news_by_company[company_slug] = news_vector_store.news_recent_articles(
                company_slug, limit=news_per_company
            )
    all_leads_index = [
        {"lead_id": lead.get("lead_id"), "hypothesis_statement": lead.get("hypothesis_statement"),
         "overall_priority": lead.get("overall_priority"), "company_id": lead.get("primary_company_slug")}
        for lead in full_leads
    ]
    return {
        "bundle": bundles_detail[0] if len(bundles_detail) == 1 else None,
        "bundles": bundles_detail,
        "bundle_ids": bundle_ids,
        "leads_index": all_leads_index,
        "leads_full": full_leads,
        "news_by_company": news_by_company,
        "news_query_used": query,
    }


def _trim_citation(citation: dict[str, Any], quote_max: int) -> dict[str, Any]:
    trimmed = dict(citation)
    quote = trimmed.get("quote")
    if isinstance(quote, str) and len(quote) > quote_max:
        trimmed["quote"] = quote[:quote_max] + "…"
    return trimmed


def _compact_lead(
    lead_payload: dict[str, Any],
    max_claims: int,
    max_citations_per_claim: int,
    quote_max: int,
) -> dict[str, Any]:
    node = copy.deepcopy(lead_payload)
    claims = node.get("claims")
    if isinstance(claims, list):
        slim_claims = []
        for claim in claims[:max_claims]:
            if not isinstance(claim, dict):
                continue
            claim_copy = dict(claim)
            citations = claim_copy.get("citations")
            if isinstance(citations, list):
                claim_copy["citations"] = [
                    _trim_citation(c, quote_max) for c in citations[:max_citations_per_claim] if isinstance(c, dict)
                ]
            slim_claims.append(claim_copy)
        node["claims"] = slim_claims
    node.pop("funding_rounds_for_primary_company", None)
    return node


def compact_intel_dossier(
    dossier: dict[str, Any],
    max_claims_per_lead: int = 10,
    max_citations_per_claim: int = 2,
    quote_max_chars: int = 180,
    max_news_chunks: int = 6,
) -> dict[str, Any]:
    """Shrink dossier for LLM context: trim claims, drop heavy funding listings, cap news rows."""
    if dossier.get("error"):
        return dossier
    compact = copy.deepcopy(dossier)
    leads_full = compact.get("leads_full")
    if isinstance(leads_full, list):
        compact["leads_full"] = [
            _compact_lead(lead, max_claims_per_lead, max_citations_per_claim, quote_max_chars)
            for lead in leads_full
            if isinstance(lead, dict)
        ]
    news_map = compact.get("news_by_company")
    if isinstance(news_map, dict):
        for company_slug, block in news_map.items():
            if not isinstance(block, dict):
                continue
            rows = block.get("results")
            if isinstance(rows, list):
                block["results"] = rows[:max_news_chunks]
            news_map[company_slug] = block
    return compact
