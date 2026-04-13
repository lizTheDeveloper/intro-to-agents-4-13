"""Integration tests for news tables + pgvector (no MLX required)."""

from __future__ import annotations

import os
import unittest
import uuid

import numpy as np
from dotenv import load_dotenv


def _database_url_configured() -> bool:
    load_dotenv()
    return bool(os.environ.get("DATABASE_URL") or os.environ.get("HIRING_INTEL_DATABASE_URL"))


def _random_unit_embedding() -> np.ndarray:
    vector = np.random.randn(1024).astype(np.float32)
    vector /= np.linalg.norm(vector) + 1e-9
    return vector


@unittest.skipUnless(_database_url_configured(), "DATABASE_URL not set")
class NewsVectorStoreTests(unittest.TestCase):
    def setUp(self):
        load_dotenv()
        import importlib

        import hiring_intel_store
        import news_vector_store

        importlib.reload(hiring_intel_store)
        importlib.reload(news_vector_store)
        self.hiring_intel_store = hiring_intel_store
        self.news_vector_store = news_vector_store
        self._suffix = uuid.uuid4().hex[:12]
        self.company_slug = f"news-test-co-{self._suffix}"

    def test_article_chunk_roundtrip_and_search(self):
        store = self.hiring_intel_store
        news = self.news_vector_store
        self.assertEqual(store.initialize_database().get("ok"), True)
        store.company_upsert(self.company_slug, "News Test Co")
        url = f"https://example.com/news/{self._suffix}"
        article = news.news_article_upsert(
            company_slug=self.company_slug,
            canonical_url=url,
            title="Headline",
            snippet="Short",
            body_text="Long body " * 50,
            ingest_source="test",
            search_query="unit test",
            raw_metadata={"k": 1},
        )
        self.assertNotIn("error", article)
        article_id = int(article["article_id"])
        chunk_texts = ["alpha chunk about hiring", "beta chunk about funding"]
        embeddings = np.stack([_random_unit_embedding(), _random_unit_embedding()])
        inserted = news.news_chunks_insert(article_id, chunk_texts, embeddings)
        self.assertEqual(inserted.get("ok"), True)
        query_vec = embeddings[0]
        hits = news.news_semantic_search(
            self.company_slug,
            query="hiring",
            query_embedding=query_vec,
            limit=5,
        )
        self.assertNotIn("error", hits)
        self.assertGreaterEqual(len(hits["results"]), 1)


class NewsChunkingTests(unittest.TestCase):
    def test_chunk_overlap(self):
        from news_text_chunking import chunk_text

        text = "para one.\n\n" + ("word " * 400)
        chunks = chunk_text(text, max_chars=200, overlap_chars=40)
        self.assertGreater(len(chunks), 1)
        self.assertTrue(all(len(part) <= 200 for part in chunks))


if __name__ == "__main__":
    unittest.main()
