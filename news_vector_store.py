"""
Store and retrieve company news chunks with pgvector (PostgreSQL).
"""

from __future__ import annotations

import json
import logging
from typing import Any, Optional

import numpy as np

from db_connection import connect as _connect_base
from langfuse_tracing import trace_postgres_write

logger = logging.getLogger("intro_agents.news_vector_store")


def _connect():
    return _connect_base(register_vector_ext=True)


def _company_internal_id(connection, company_slug: str) -> Optional[int]:
    with connection.cursor() as cursor:
        cursor.execute("SELECT id FROM intel_company WHERE company_id = %s", (company_slug,))
        row = cursor.fetchone()
        return int(row["id"]) if row else None


def news_article_upsert(
    company_slug: str,
    canonical_url: str,
    title: Optional[str],
    snippet: Optional[str],
    body_text: Optional[str],
    ingest_source: str,
    search_query: Optional[str],
    raw_metadata: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    with trace_postgres_write(
        "news_article_upsert",
        company_slug=company_slug,
        canonical_url=canonical_url,
    ):
        with _connect() as connection:
            company_pk = _company_internal_id(connection, company_slug)
            if company_pk is None:
                logger.warning("news_article_upsert: unknown company_slug=%s", company_slug)
                return {"error": "unknown_company_slug", "company_slug": company_slug}
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO intel_company_news_article (
                      company_id, canonical_url, title, snippet, body_text,
                      ingest_source, search_query, raw_metadata_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (company_id, canonical_url) DO UPDATE SET
                      title = EXCLUDED.title,
                      snippet = EXCLUDED.snippet,
                      body_text = EXCLUDED.body_text,
                      ingest_source = EXCLUDED.ingest_source,
                      search_query = EXCLUDED.search_query,
                      raw_metadata_json = EXCLUDED.raw_metadata_json
                    RETURNING id
                    """,
                    (
                        company_pk,
                        canonical_url,
                        title,
                        snippet,
                        body_text,
                        ingest_source,
                        search_query,
                        json.dumps(raw_metadata, ensure_ascii=False) if raw_metadata is not None else None,
                    ),
                )
                row = cursor.fetchone()
                article_id = int(row["id"])
                cursor.execute(
                    "DELETE FROM intel_company_news_chunk WHERE article_id = %s",
                    (article_id,),
                )
    logger.info("Upserted article id=%d company=%s url=%s", article_id, company_slug, canonical_url)
    return {"article_id": article_id, "company_slug": company_slug}


def news_chunks_insert(
    article_id: int,
    chunk_texts: list[str],
    embeddings: np.ndarray,
) -> dict[str, Any]:
    if len(chunk_texts) != len(embeddings):
        logger.error(
            "Chunk/embedding count mismatch for article_id=%d: chunks=%d embeddings=%d",
            article_id, len(chunk_texts), len(embeddings),
        )
        return {
            "error": "chunk_embedding_count_mismatch",
            "chunks": len(chunk_texts),
            "embeddings": len(embeddings),
        }
    with trace_postgres_write(
        "news_chunks_insert",
        article_id=article_id,
        chunk_count=len(chunk_texts),
    ):
        with _connect() as connection:
            with connection.cursor() as cursor:
                for index, (chunk_body, vector) in enumerate(zip(chunk_texts, embeddings, strict=True)):
                    cursor.execute(
                        """
                        INSERT INTO intel_company_news_chunk (article_id, chunk_index, content, embedding)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (article_id, index, chunk_body, vector.tolist()),
                    )
    logger.info("Inserted %d chunks for article_id=%d", len(chunk_texts), article_id)
    return {"ok": True, "article_id": article_id, "chunks_written": len(chunk_texts)}


def news_semantic_search(
    company_slug: str,
    query: str,
    query_embedding: np.ndarray,
    limit: int = 8,
) -> dict[str, Any]:
    limit = max(1, min(int(limit), 50))
    vector = query_embedding.astype(np.float32).reshape(-1)
    with _connect() as connection:
        company_pk = _company_internal_id(connection, company_slug)
        if company_pk is None:
            logger.warning("news_semantic_search: unknown company_slug=%s", company_slug)
            return {"error": "unknown_company_slug", "company_slug": company_slug}
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT c.id AS chunk_id, c.chunk_index, c.content,
                       a.id AS article_id, a.title, a.canonical_url, a.snippet,
                       c.embedding <=> %s::vector AS distance
                FROM intel_company_news_chunk c
                JOIN intel_company_news_article a ON a.id = c.article_id
                WHERE a.company_id = %s
                ORDER BY c.embedding <=> %s::vector
                LIMIT %s
                """,
                (vector.tolist(), company_pk, vector.tolist(), limit),
            )
            rows = cursor.fetchall()
    results = [dict(row) for row in rows]
    logger.info("Semantic search for %s returned %d chunks", company_slug, len(results))
    return {
        "company_slug": company_slug,
        "query": query,
        "results": results,
    }


def news_recent_articles(company_slug: str, limit: int = 12) -> dict[str, Any]:
    """Latest ingested news rows for a company (no embeddings required)."""
    limit = max(1, min(int(limit), 50))
    with _connect() as connection:
        company_pk = _company_internal_id(connection, company_slug)
        if company_pk is None:
            return {"error": "unknown_company_slug", "company_slug": company_slug}
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT a.id AS article_id, a.canonical_url, a.title, a.snippet,
                       a.fetched_at_utc, a.ingest_source
                FROM intel_company_news_article a
                WHERE a.company_id = %s
                ORDER BY a.fetched_at_utc DESC NULLS LAST, a.id DESC
                LIMIT %s
                """,
                (company_pk, limit),
            )
            rows = cursor.fetchall()
    return {
        "company_slug": company_slug,
        "mode": "recent",
        "results": [dict(row) for row in rows],
    }
