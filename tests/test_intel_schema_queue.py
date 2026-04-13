"""Schema change queue (Postgres + markdown export)."""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from dotenv import load_dotenv


def _database_url_configured() -> bool:
    load_dotenv()
    return bool(os.environ.get("DATABASE_URL") or os.environ.get("HIRING_INTEL_DATABASE_URL"))


@unittest.skipUnless(_database_url_configured(), "DATABASE_URL not set")
class IntelSchemaQueueTests(unittest.TestCase):
    def setUp(self):
        load_dotenv()
        import importlib

        import hiring_intel_store
        import intel_schema_queue_store

        importlib.reload(hiring_intel_store)
        importlib.reload(intel_schema_queue_store)
        self.hiring_intel_store = hiring_intel_store
        self.intel_schema_queue_store = intel_schema_queue_store

    def test_submit_export_and_status(self):
        his = self.hiring_intel_store
        sqs = self.intel_schema_queue_store
        self.assertEqual(his.initialize_database().get("ok"), True)
        bundle_id = his.bundle_create("2026-04-13T20:00:00Z", "schema-queue-test")["bundle_id"]
        submit = sqs.schema_queue_submit(
            source_agent="unittest",
            request_title="Add column foo",
            request_description="Track editorial tone on news chunks",
            request_kind="missing_column",
            related_table="intel_company_news_chunk",
            related_column="editorial_tone",
            bundle_id=bundle_id,
            proposed_ddl="ALTER TABLE intel_company_news_chunk ADD COLUMN editorial_tone TEXT;",
        )
        self.assertIn("request_id", submit)
        listed = sqs.schema_queue_list(status="pending", limit=10)
        self.assertGreaterEqual(len(listed["requests"]), 1)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "queue.md"
            exported = sqs.export_pending_schema_requests(markdown_path=path)
            self.assertEqual(exported.get("exported"), 1)
            text = path.read_text(encoding="utf-8")
            self.assertIn("REQ-", text)
            self.assertIn("editorial_tone", text)
        listed_after = sqs.schema_queue_list(status="exported", limit=5)
        self.assertTrue(any(r["id"] == submit["request_id"] for r in listed_after["requests"]))


if __name__ == "__main__":
    unittest.main()
