"""
Integration tests for hiring intel persistence (PostgreSQL) and tools wiring.
"""

from __future__ import annotations

import json
import os
import unittest
import uuid
from types import SimpleNamespace
from unittest import mock

from dotenv import load_dotenv


def _database_url_configured() -> bool:
    load_dotenv()
    return bool(os.environ.get("DATABASE_URL") or os.environ.get("HIRING_INTEL_DATABASE_URL"))


@unittest.skipUnless(_database_url_configured(), "DATABASE_URL or HIRING_INTEL_DATABASE_URL not set")
class HiringIntelPostgresIntegrationTests(unittest.TestCase):
    def setUp(self):
        load_dotenv()
        import importlib

        import hiring_intel_store

        importlib.reload(hiring_intel_store)
        self.hiring_intel_store = hiring_intel_store
        self._suffix = uuid.uuid4().hex[:12]

    def test_store_full_workflow(self):
        store = self.hiring_intel_store
        self.assertEqual(store.initialize_database().get("ok"), True)
        company_slug = f"unittest-co-{self._suffix}"
        lead_slug = f"unittest-lead-{self._suffix}"
        claim_uuid = f"unittest-claim-{self._suffix}"

        bundle = store.bundle_create("2026-04-13T12:00:00Z", "integration-test")
        bundle_id = bundle["bundle_id"]
        store.target_profile_put(
            bundle_id,
            role_family=["cfo"],
            domains=["fintech"],
        )
        store.company_upsert(company_slug, "Acme Inc", website_url="https://acme.example")
        store.funding_round_upsert(
            company_slug,
            f"series-b-{self._suffix}",
            round_label="series_b",
            amount_value=40_000_000,
            amount_currency="USD",
        )
        lead = store.lead_create(
            bundle_id,
            lead_slug,
            "Post-Series B scale implies CFO bench build",
            company_slug,
            overall_priority=72.0,
        )
        self.assertIn("lead_internal_id", lead)
        self.assertEqual(
            store.lead_link_funding_context(lead_slug, company_slug, f"series-b-{self._suffix}")["ok"],
            True,
        )
        signal = store.signal_add(
            lead_slug,
            "exec_team_incomplete",
            "strong",
            "Press quotes use-of-proceeds for GTM and finance systems",
        )
        self.assertIn("signal_internal_id", signal)
        claim = store.claim_add(
            claim_uuid,
            "Company announced Series B",
            "high",
            "funding_event",
            citations=[
                {
                    "source_url": "https://example.com/pr",
                    "retrieved_at_utc": "2026-04-13T12:05:00Z",
                    "quote": "$40M Series B",
                }
            ],
            lead_id=lead_slug,
        )
        self.assertEqual(claim.get("ok"), True)
        store.lead_add_primary_sources(
            lead_slug,
            citations=[
                {
                    "source_url": "https://example.com/overview",
                    "retrieved_at_utc": "2026-04-13T12:06:00Z",
                }
            ],
        )
        store.executive_motion_add(
            lead_slug,
            "departed",
            person_name="Jane Doe",
            title="Interim CFO",
            stakeholder_category="recent_exec_changes",
        )
        store.interview_prep_put(
            lead_slug,
            company_narrative="B2B payments infra",
            sharp_questions=["What is the cash runway target?"],
        )
        listed = store.leads_list(bundle_id, minimum_overall_priority=50.0)
        self.assertEqual(len(listed["leads"]), 1)
        full = store.lead_get_full(lead_slug)
        self.assertEqual(full.get("error"), None)
        self.assertEqual(full["lead_id"], lead_slug)
        self.assertEqual(len(full["signals"]), 1)
        self.assertEqual(len(full["claims"]), 1)
        self.assertEqual(len(full["claims"][0]["citations"]), 1)
        self.assertEqual(len(full["primary_sources"]), 1)
        self.assertEqual(len(full["executive_motions"]), 1)
        self.assertIsNotNone(full["interview_prep"])

    def test_tools_dispatch_intel_tool(self):
        store = self.hiring_intel_store
        store.initialize_database()
        company_slug = f"unittest-beta-{self._suffix}"
        lead_slug = f"unittest-lead-beta-{self._suffix}"
        bundle_id = store.bundle_create("2026-04-13T12:00:00Z", "tool-routing")["bundle_id"]
        store.company_upsert(company_slug, "Beta Co")
        store.lead_create(bundle_id, lead_slug, "Hypothesis", company_slug)

        import importlib

        import tools as tools_module

        importlib.reload(tools_module)

        tool_call = SimpleNamespace(
            function=SimpleNamespace(
                name="intel_lead_get_full",
                arguments=json.dumps({"lead_id": lead_slug}),
            )
        )
        raw = tools_module.execute_tool_call(tool_call)
        payload = json.loads(raw)
        self.assertEqual(payload["lead_id"], lead_slug)
        self.assertEqual(payload["primary_company_slug"], company_slug)


if __name__ == "__main__":
    unittest.main()
