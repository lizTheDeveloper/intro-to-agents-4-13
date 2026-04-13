"""Integration tests for intel analyst gather (no Groq)."""

from __future__ import annotations

import os
import unittest
import uuid

import numpy as np
from dotenv import load_dotenv

from intel_analyst_gather import compact_intel_dossier, gather_bundle_intel


def _database_url_configured() -> bool:
    load_dotenv()
    return bool(os.environ.get("DATABASE_URL") or os.environ.get("HIRING_INTEL_DATABASE_URL"))


@unittest.skipUnless(_database_url_configured(), "DATABASE_URL not set")
class IntelAnalystGatherTests(unittest.TestCase):
    def setUp(self):
        load_dotenv()
        import importlib

        import hiring_intel_store
        import news_vector_store

        importlib.reload(hiring_intel_store)
        importlib.reload(news_vector_store)
        self.hiring_intel_store = hiring_intel_store
        self.news_vector_store = news_vector_store
        self._suffix = uuid.uuid4().hex[:10]

    def test_gather_and_compact_bundle(self):
        his = self.hiring_intel_store
        nvs = self.news_vector_store
        self.assertEqual(his.initialize_database().get("ok"), True)
        slug = f"analyst-co-{self._suffix}"
        bundle_id = his.bundle_create("2026-04-13T15:01:00Z", "analyst-test")["bundle_id"]
        his.target_profile_put(bundle_id, role_family=["cfo"], domains=["saas"])
        his.company_upsert(slug, "Analyst Test Co")
        his.funding_round_upsert(slug, f"seed-{self._suffix}", round_label="seed", amount_value=2e6)
        his.lead_create(bundle_id, f"lead-{self._suffix}", "CFO gap post-seed", slug, overall_priority=80.0)
        discovered = his.companies_for_bundle_news(bundle_id)
        self.assertEqual(len(discovered), 1)
        self.assertEqual(discovered[0]["company_id"], slug)
        article = nvs.news_article_upsert(
            company_slug=slug,
            canonical_url=f"https://example.com/a/{self._suffix}",
            title="Press",
            snippet="Snip",
            body_text="Body " * 20,
            ingest_source="test",
            search_query="test",
        )
        article_id = int(article["article_id"])
        emb = np.random.randn(2, 1024).astype(np.float32)
        emb /= np.linalg.norm(emb, axis=1, keepdims=True) + 1e-9
        nvs.news_chunks_insert(article_id, ["chunk one text", "chunk two text"], emb)

        dossier = gather_bundle_intel(
            bundle_id,
            use_semantic_news=False,
            news_per_company=5,
        )
        self.assertNotIn("error", dossier)
        self.assertEqual(dossier["bundle"]["bundle_id"], bundle_id)
        self.assertEqual(len(dossier["leads_full"]), 1)
        self.assertIn(slug, dossier["news_by_company"])
        compact = compact_intel_dossier(dossier)
        self.assertNotIn("funding_rounds_for_primary_company", compact["leads_full"][0])


if __name__ == "__main__":
    unittest.main()
