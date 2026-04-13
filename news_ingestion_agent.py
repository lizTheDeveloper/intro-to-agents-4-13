#!/usr/bin/env python3
"""
Standalone agent: search the web for recent news about a company, extract text,
chunk, embed with MLX (Qwen3-Embedding 0.6B class), and store in PostgreSQL + pgvector.

Requires: DATABASE_URL, TAVILY_API_KEY, Apple Silicon + mlx / mlx-lm for embeddings.
The company row must already exist in intel_company (e.g. created via hiring intel tools).

Example:
  python news_ingestion_agent.py --company-slug acme --display-name "Acme Inc" --max-articles 6
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from typing import Any, Optional

from dotenv import load_dotenv
from tavily import TavilyClient

from langfuse_tracing import trace_agent_session, trace_retriever_step
from mlx_qwen_embedder import EMBEDDING_DIMENSION, embed_texts
from news_text_chunking import chunk_text
from news_vector_store import news_article_upsert, news_chunks_insert

logger = logging.getLogger("intro_agents.news_ingestion_agent")


def _compact_extract(payload: dict[str, Any]) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    results = payload.get("results")
    if not isinstance(results, list) or not results:
        return None
    first = results[0]
    if not isinstance(first, dict):
        return None
    for key in ("raw_content", "content", "markdown"):
        raw = first.get(key)
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
    return None


def ingest_company_news(
    company_slug: str,
    display_name: str,
    max_articles: int,
    search_depth: str,
    max_results: int,
) -> dict[str, Any]:
    client = TavilyClient(os.environ.get("TAVILY_API_KEY"))
    query = (
        f'"{display_name}" OR {company_slug} '
        f"(news OR funding OR acquisition OR CEO OR CFO OR layoff OR partnership)"
    )
    logger.info("Searching: %s", query)
    with trace_agent_session(
        "news_ingestion_agent",
        company_slug=company_slug,
        display_name=display_name,
    ):
        return _ingest_company_news_body(
            client,
            company_slug=company_slug,
            display_name=display_name,
            max_articles=max_articles,
            search_depth=search_depth,
            max_results=max_results,
            query=query,
        )


def _ingest_company_news_body(
    client: TavilyClient,
    *,
    company_slug: str,
    display_name: str,
    max_articles: int,
    search_depth: str,
    max_results: int,
    query: str,
) -> dict[str, Any]:
    with trace_retriever_step("tavily.search", query=query, topic="news"):
        search = client.search(
            query=query,
            search_depth=search_depth,
            max_results=max_results,
            include_raw_content=False,
            topic="news",
        )
    results = search.get("results") if isinstance(search, dict) else []
    if not isinstance(results, list):
        results = []
    articles_processed = 0
    chunks_total = 0
    for item in results[:max_articles]:
        if not isinstance(item, dict):
            continue
        url = item.get("url")
        if not url:
            continue
        title = item.get("title")
        snippet = item.get("content") or item.get("snippet")
        body: Optional[str] = None
        try:
            with trace_retriever_step("tavily.extract", url=url):
                extracted = client.extract(url)
            body = _compact_extract(extracted if isinstance(extracted, dict) else {})
        except Exception as extract_error:
            logger.warning("Extract failed for %s: %s", url, extract_error)
        combined = "\n\n".join(
            part for part in (title, snippet, body) if isinstance(part, str) and part.strip()
        )
        if not combined.strip():
            continue
        chunks = chunk_text(combined)
        if not chunks:
            continue
        vectors = embed_texts(chunks, normalize=True)
        if vectors.shape[1] != EMBEDDING_DIMENSION:
            raise RuntimeError(
                f"Unexpected embedding dim {vectors.shape[1]} (expected {EMBEDDING_DIMENSION})"
            )
        article_row = news_article_upsert(
            company_slug=company_slug,
            canonical_url=url,
            title=title if isinstance(title, str) else None,
            snippet=snippet if isinstance(snippet, str) else None,
            body_text=body,
            ingest_source="tavily_search_extract",
            search_query=query,
            raw_metadata={"search_item": item},
        )
        if article_row.get("error"):
            logger.error("Article upsert failed: %s", article_row)
            continue
        article_id = int(article_row["article_id"])
        insert_result = news_chunks_insert(article_id, chunks, vectors)
        if insert_result.get("error"):
            logger.error("Chunk insert failed: %s", insert_result)
            continue
        articles_processed += 1
        chunks_total += int(insert_result.get("chunks_written", 0))
        logger.info("Stored article id=%s url=%s chunks=%s", article_id, url, insert_result.get("chunks_written"))
    return {
        "company_slug": company_slug,
        "articles_processed": articles_processed,
        "chunks_total": chunks_total,
        "search_results_seen": len(results),
    }


def main() -> None:
    load_dotenv()
    from logging_config import configure_logging
    configure_logging()
    parser = argparse.ArgumentParser(description="Ingest company news into PostgreSQL + pgvector.")
    parser.add_argument("--company-slug", required=True, help="intel_company.company_id slug")
    parser.add_argument(
        "--display-name",
        default=None,
        help="Human-readable company name for search queries (defaults to slug)",
    )
    parser.add_argument("--max-articles", type=int, default=8)
    parser.add_argument("--max-results", type=int, default=12, help="Tavily search result cap before trimming")
    parser.add_argument(
        "--search-depth",
        default=os.environ.get("AGENT_TAVILY_SEARCH_DEPTH", "advanced"),
        help="Tavily search_depth (basic or advanced)",
    )
    parser.add_argument(
        "--init-schema",
        action="store_true",
        help="Run hiring intel DDL (includes news + pgvector) before ingesting",
    )
    args = parser.parse_args()
    display = args.display_name or args.company_slug
    if not os.environ.get("TAVILY_API_KEY"):
        logger.error("TAVILY_API_KEY is not set.")
        sys.exit(1)
    if not (os.environ.get("DATABASE_URL") or os.environ.get("HIRING_INTEL_DATABASE_URL")):
        logger.error("DATABASE_URL is not set.")
        sys.exit(1)
    if args.init_schema:
        import hiring_intel_store

        init_result = hiring_intel_store.initialize_database()
        logger.info("Schema init: %s", init_result)
    summary = ingest_company_news(
        company_slug=args.company_slug,
        display_name=display,
        max_articles=args.max_articles,
        search_depth=args.search_depth,
        max_results=args.max_results,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
