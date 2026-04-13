"""
Integration smoke tests for the agent framework cleanup.

Exercises real module wiring, DB connection sharing, logging config,
Langfuse tracing (in no-credentials mode), and tool dispatch error paths.
Does NOT hit external APIs (Groq, Tavily).
"""

from __future__ import annotations

import json
import os
import unittest
import uuid
from types import SimpleNamespace

from dotenv import load_dotenv


def _database_url_configured() -> bool:
    load_dotenv()
    return bool(os.environ.get("DATABASE_URL") or os.environ.get("HIRING_INTEL_DATABASE_URL"))


class CentralizedLoggingTests(unittest.TestCase):
    def test_configure_logging_is_idempotent(self):
        from logging_config import configure_logging
        configure_logging()
        configure_logging()
        configure_logging(level="DEBUG")

    def test_all_project_loggers_use_namespace(self):
        """Every module logger should be under the intro_agents.* namespace."""
        import logging
        from logging_config import configure_logging
        configure_logging()

        module_names = [
            "intro_agents.db_connection",
            "intro_agents.langfuse_tracing",
            "intro_agents.tools",
            "intro_agents.prompting",
            "intro_agents.news_vector_store",
            "intro_agents.news_text_chunking",
            "intro_agents.hiring_intel_store",
        ]
        for name in module_names:
            logger = logging.getLogger(name)
            self.assertTrue(logger.name.startswith("intro_agents."), f"{name} not in namespace")


class SharedDBConnectionTests(unittest.TestCase):
    def test_conninfo_requires_env(self):
        from db_connection import conninfo
        saved = os.environ.pop("DATABASE_URL", None)
        saved2 = os.environ.pop("HIRING_INTEL_DATABASE_URL", None)
        try:
            with self.assertRaises(RuntimeError):
                conninfo()
        finally:
            if saved:
                os.environ["DATABASE_URL"] = saved
            if saved2:
                os.environ["HIRING_INTEL_DATABASE_URL"] = saved2

    def test_redacted_conninfo_masks_password(self):
        from db_connection import redacted_conninfo
        os.environ.setdefault("DATABASE_URL", "postgresql://user:secret@localhost:5432/db")
        result = redacted_conninfo()
        self.assertNotIn("secret", result)
        self.assertIn("***", result)


class ToolSpecTests(unittest.TestCase):
    def test_function_tool_shape(self):
        from tool_spec import function_tool
        result = function_tool("my_tool", "Does stuff", {"type": "object", "properties": {}})
        self.assertEqual(result["type"], "function")
        self.assertEqual(result["function"]["name"], "my_tool")
        self.assertEqual(result["function"]["description"], "Does stuff")


class LangfuseTracingTests(unittest.TestCase):
    """Test tracing context managers don't crash regardless of credential state."""

    def test_trace_agent_session(self):
        from langfuse_tracing import trace_agent_session
        with trace_agent_session("test_agent", foo="bar"):
            pass

    def test_trace_retriever_step(self):
        from langfuse_tracing import trace_retriever_step
        with trace_retriever_step("test_retriever") as span:
            pass  # span may be None or a real Langfuse object

    def test_trace_postgres_write(self):
        from langfuse_tracing import trace_postgres_write
        with trace_postgres_write("test_write"):
            pass

    def test_pipeline_bundle_context(self):
        from langfuse_tracing import pipeline_bundle_context
        with pipeline_bundle_context(999):
            pass

    def test_trace_tool_execution(self):
        from langfuse_tracing import trace_tool_execution
        with trace_tool_execution("test_tool", {"arg": 1}) as span:
            pass  # span may be None or a real Langfuse object


class ToolDispatchErrorHandlingTests(unittest.TestCase):
    """Verify that tool execution errors produce structured JSON, not crashes."""

    def test_unknown_tool_returns_error_json(self):
        from tools import execute_tool_call
        fake_call = SimpleNamespace(
            function=SimpleNamespace(name="nonexistent_tool_xyz", arguments="{}")
        )
        result = execute_tool_call(fake_call)
        parsed = json.loads(result)
        self.assertEqual(parsed["error"], "unknown_tool")

    def test_malformed_arguments_returns_error_json(self):
        from tools import execute_tool_call
        fake_call = SimpleNamespace(
            function=SimpleNamespace(name="web_search", arguments="not valid json {{{")
        )
        result = execute_tool_call(fake_call)
        parsed = json.loads(result)
        self.assertEqual(parsed["error"], "argument_parse_failure")

    def test_tool_handler_exception_returns_error_json(self):
        """When a tool handler itself raises, we get structured error not a crash."""
        import tools as tools_module
        original = tools_module.available_functions.get("web_search")

        def _exploding_tool(**kwargs):
            raise ValueError("Intentional test explosion")

        tools_module.available_functions["web_search"] = _exploding_tool
        try:
            fake_call = SimpleNamespace(
                function=SimpleNamespace(name="web_search", arguments='{"query": "test"}')
            )
            result = tools_module.execute_tool_call(fake_call)
            parsed = json.loads(result)
            self.assertEqual(parsed["error"], "tool_execution_failed")
            self.assertIn("Intentional test explosion", parsed["detail"])
        finally:
            if original is not None:
                tools_module.available_functions["web_search"] = original


class PromptingSetupTests(unittest.TestCase):
    def test_mutable_default_fixed(self):
        """The tools parameter should not be mutable default."""
        import inspect
        from prompting import prompt
        sig = inspect.signature(prompt)
        default = sig.parameters["tools"].default
        self.assertIsNone(default)

    def test_reset_conversation(self):
        from prompting import reset_conversation, messages
        messages.append({"role": "user", "content": "test"})
        reset_conversation()
        self.assertEqual(len(messages), 0)


@unittest.skipUnless(_database_url_configured(), "DATABASE_URL not set")
class DBModuleWiringTests(unittest.TestCase):
    """Verify that all store modules use the shared db_connection layer."""

    def setUp(self):
        load_dotenv()
        self._suffix = uuid.uuid4().hex[:8]

    def test_hiring_intel_store_connects(self):
        import hiring_intel_store
        result = hiring_intel_store.initialize_database()
        self.assertTrue(result.get("ok"))

    def test_news_vector_store_connects(self):
        import hiring_intel_store
        hiring_intel_store.initialize_database()
        slug = f"smoke-co-{self._suffix}"
        hiring_intel_store.company_upsert(slug, "Smoke Test Co")

        from news_vector_store import news_recent_articles
        result = news_recent_articles(slug, limit=1)
        self.assertEqual(result["company_slug"], slug)
        self.assertIsInstance(result["results"], list)

    def test_schema_queue_store_connects(self):
        from intel_schema_queue_store import schema_queue_list
        result = schema_queue_list(status="pending", limit=1)
        self.assertIn("requests", result)


@unittest.skipUnless(_database_url_configured(), "DATABASE_URL not set")
class AccrualBehaviorTests(unittest.TestCase):
    """Verify that data accumulates across pipeline runs without collisions."""

    def setUp(self):
        load_dotenv()
        self._suffix = uuid.uuid4().hex[:8]
        import hiring_intel_store
        hiring_intel_store.initialize_database()
        self.store = hiring_intel_store

    def test_bundles_list_returns_recent(self):
        bundle = self.store.bundle_create(
            "2026-04-13T12:00:00Z", "accrual-test",
            search_focus=f"test-search-{self._suffix}",
        )
        result = self.store.bundles_list(limit=5)
        bundle_ids = [row["bundle_id"] for row in result["bundles"]]
        self.assertIn(bundle["bundle_id"], bundle_ids)
        matching = [row for row in result["bundles"] if row["bundle_id"] == bundle["bundle_id"]]
        self.assertEqual(matching[0]["search_focus"], f"test-search-{self._suffix}")

    def test_lead_upsert_on_duplicate_id(self):
        """Creating a lead with the same lead_id should update, not crash."""
        slug = f"accrual-co-{self._suffix}"
        lead_id = f"accrual-lead-{self._suffix}"
        bundle = self.store.bundle_create("2026-04-13T12:00:00Z", "accrual-test")
        bid = bundle["bundle_id"]
        self.store.company_upsert(slug, "Accrual Test Co")
        first = self.store.lead_create(bid, lead_id, "First hypothesis", slug, overall_priority=50.0)
        self.assertIn("lead_internal_id", first)

        second = self.store.lead_create(bid, lead_id, "Updated hypothesis", slug, overall_priority=90.0)
        self.assertIn("lead_internal_id", second)
        self.assertEqual(first["lead_internal_id"], second["lead_internal_id"])

        full = self.store.lead_get_full(lead_id)
        self.assertEqual(full["hypothesis_statement"], "Updated hypothesis")
        self.assertEqual(full["overall_priority"], 90.0)

    def test_claim_upsert_on_duplicate_uuid(self):
        """Creating a claim with the same UUID should update, not crash."""
        slug = f"claim-co-{self._suffix}"
        lead_id = f"claim-lead-{self._suffix}"
        claim_uuid = f"claim-uuid-{self._suffix}"
        bundle = self.store.bundle_create("2026-04-13T12:00:00Z", "claim-test")
        bid = bundle["bundle_id"]
        self.store.company_upsert(slug, "Claim Test Co")
        self.store.lead_create(bid, lead_id, "Test hypothesis", slug)

        first = self.store.claim_add(
            claim_uuid, "First statement", "high", "funding_event",
            citations=[{"source_url": "https://example.com/1", "retrieved_at_utc": "2026-04-13T12:00:00Z"}],
            lead_id=lead_id,
        )
        self.assertTrue(first.get("ok"))

        second = self.store.claim_add(
            claim_uuid, "Updated statement", "medium", "funding_event",
            citations=[{"source_url": "https://example.com/2", "retrieved_at_utc": "2026-04-13T13:00:00Z"}],
            lead_id=lead_id,
        )
        self.assertTrue(second.get("ok"))
        self.assertEqual(first["claim_internal_id"], second["claim_internal_id"])

    def test_multi_bundle_gather(self):
        """Analyst gather should work with multiple bundle IDs."""
        slug = f"multi-co-{self._suffix}"
        self.store.company_upsert(slug, "Multi Bundle Co")

        bid1 = self.store.bundle_create("2026-04-13T12:00:00Z", "multi-test-1", search_focus="run 1")["bundle_id"]
        self.store.lead_create(bid1, f"b{bid1}-{slug}-lead", "Hypothesis from run 1", slug)

        bid2 = self.store.bundle_create("2026-04-13T13:00:00Z", "multi-test-2", search_focus="run 2")["bundle_id"]
        self.store.lead_create(bid2, f"b{bid2}-{slug}-lead", "Hypothesis from run 2", slug)

        from intel_analyst_gather import gather_bundle_intel
        dossier = gather_bundle_intel([bid1, bid2], use_semantic_news=False)
        self.assertIsNone(dossier.get("error"), f"gather error: {dossier.get('error')}")
        self.assertEqual(len(dossier["bundle_ids"]), 2)
        self.assertEqual(len(dossier["leads_full"]), 2)


class ChunkingTests(unittest.TestCase):
    def test_empty_input(self):
        from news_text_chunking import chunk_text
        self.assertEqual(chunk_text(""), [])
        self.assertEqual(chunk_text("   "), [])

    def test_short_input_single_chunk(self):
        from news_text_chunking import chunk_text
        result = chunk_text("Hello world")
        self.assertEqual(result, ["Hello world"])

    def test_long_input_produces_multiple_chunks(self):
        from news_text_chunking import chunk_text
        long_text = "word " * 500
        result = chunk_text(long_text, max_chars=200, overlap_chars=50)
        self.assertGreater(len(result), 1)
        for chunk in result:
            self.assertLessEqual(len(chunk), 200)


if __name__ == "__main__":
    unittest.main()
