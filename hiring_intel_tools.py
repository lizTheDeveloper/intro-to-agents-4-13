"""
OpenAI-compatible function tool specs and dispatch table for hiring intel PostgreSQL ops.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import hiring_intel_store as hiring_intel_store
import intel_schema_queue_store as intel_schema_queue_store
from tool_spec import function_tool as _function_tool


def intel_schema_queue_export_pending_tool(markdown_path: Optional[str] = None):
    path = Path(markdown_path) if markdown_path else None
    return intel_schema_queue_store.export_pending_schema_requests(path)


HIRING_INTEL_TOOL_DEFINITIONS = [
    _function_tool(
        "intel_initialize_database",
        "Create or upgrade the PostgreSQL hiring-intel schema (idempotent). Call once per database or after schema changes.",
        {"type": "object", "properties": {}, "required": []},
    ),
    _function_tool(
        "intel_bundle_create",
        "Start a research bundle (one agent run). Returns bundle_id for subsequent writes.",
        {
            "type": "object",
            "properties": {
                "generated_at_utc": {"type": "string", "description": "ISO-8601 UTC timestamp"},
                "agent_name": {"type": "string"},
                "search_focus": {"type": "string", "description": "The research query/focus for this run"},
                "research_window_start": {"type": "string", "description": "ISO date (optional)"},
                "research_window_end": {"type": "string", "description": "ISO date (optional)"},
                "geo_focus": {"type": "array", "items": {"type": "string"}},
                "sector_focus": {"type": "array", "items": {"type": "string"}},
                "data_sources_used": {"type": "array", "items": {"type": "string"}},
                "limitations": {"type": "array", "items": {"type": "string"}},
                "open_questions_global": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["generated_at_utc", "agent_name"],
        },
    ),
    _function_tool(
        "intel_bundles_list",
        "List recent research bundles with their search focus, lead count, and company count. "
        "Use to see accumulated research history across pipeline runs.",
        {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "description": "Max bundles to return (default 20)"},
            },
            "required": [],
        },
    ),
    _function_tool(
        "intel_target_profile_put",
        "Attach or update the target executive profile for a bundle_id.",
        {
            "type": "object",
            "properties": {
                "bundle_id": {"type": "integer"},
                "role_family": {"type": "array", "items": {"type": "string"}},
                "domains": {"type": "array", "items": {"type": "string"}},
                "company_stage_preference": {"type": "array", "items": {"type": "string"}},
                "must_haves": {"type": "array", "items": {"type": "string"}},
                "avoid": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["bundle_id"],
        },
    ),
    _function_tool(
        "intel_company_upsert",
        "Insert or update a company snapshot keyed by stable company_id (slug).",
        {
            "type": "object",
            "properties": {
                "company_id": {"type": "string"},
                "legal_name_best_effort": {"type": "string"},
                "dba_names": {"type": "array", "items": {"type": "string"}},
                "website_url": {"type": "string"},
                "hq_region": {"type": "string"},
                "employee_count_band": {"type": "string"},
                "sector_labels": {"type": "array", "items": {"type": "string"}},
                "business_model": {"type": "string"},
                "one_liner": {"type": "string"},
            },
            "required": ["company_id", "legal_name_best_effort"],
        },
    ),
    _function_tool(
        "intel_funding_round_upsert",
        "Insert or update a funding round for a company (identified by company_id slug).",
        {
            "type": "object",
            "properties": {
                "company_id": {"type": "string"},
                "round_id": {"type": "string", "description": "Stable id for this round within the company"},
                "round_label": {"type": "string"},
                "amount_currency": {"type": "string"},
                "amount_value": {"type": "number"},
                "amount_is_approximate": {"type": "boolean"},
                "announced_on": {"type": "string"},
                "lead_investors": {"type": "array", "items": {"type": "string"}},
                "participating_investors": {"type": "array", "items": {"type": "string"}},
                "stated_use_of_proceeds_keywords": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["company_id", "round_id"],
        },
    ),
    _function_tool(
        "intel_lead_create",
        "Create a hiring hypothesis lead linked to a bundle and primary company (company_id slug must exist).",
        {
            "type": "object",
            "properties": {
                "bundle_id": {"type": "integer"},
                "lead_id": {"type": "string", "description": "Stable external id for this lead"},
                "hypothesis_statement": {"type": "string"},
                "primary_company_id": {"type": "string", "description": "Company slug"},
                "duplicate_of_lead_id": {"type": "string"},
                "months_since_last_major_round": {"type": "number"},
                "funding_stage_inference": {"type": "string"},
                "overall_priority": {"type": "number"},
                "hypothesis_confidence": {"type": "number"},
                "fit_to_target_profile": {"type": "number"},
                "timing_urgency": {"type": "number"},
                "weights_used": {"type": "object"},
                "next_actions": {"type": "object"},
                "open_questions": {"type": "array", "items": {"type": "string"}},
                "posting_links_if_any": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["bundle_id", "lead_id", "hypothesis_statement", "primary_company_id"],
        },
    ),
    _function_tool(
        "intel_lead_update_scores",
        "Patch scoring, weights, next actions, or open questions for a lead_id.",
        {
            "type": "object",
            "properties": {
                "lead_id": {"type": "string"},
                "overall_priority": {"type": "number"},
                "hypothesis_confidence": {"type": "number"},
                "fit_to_target_profile": {"type": "number"},
                "timing_urgency": {"type": "number"},
                "weights_used": {"type": "object"},
                "next_actions": {"type": "object"},
                "open_questions": {"type": "array", "items": {"type": "string"}},
                "posting_links_if_any": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["lead_id"],
        },
    ),
    _function_tool(
        "intel_lead_link_funding_context",
        "Point a lead at its most recent material funding round (company slug + round_id).",
        {
            "type": "object",
            "properties": {
                "lead_id": {"type": "string"},
                "primary_company_id": {"type": "string"},
                "round_id": {"type": "string"},
            },
            "required": ["lead_id", "primary_company_id", "round_id"],
        },
    ),
    _function_tool(
        "intel_lead_add_related_company",
        "Associate an additional company with a lead (peers, acquirers, etc.).",
        {
            "type": "object",
            "properties": {
                "lead_id": {"type": "string"},
                "related_company_id": {"type": "string", "description": "Company slug"},
            },
            "required": ["lead_id", "related_company_id"],
        },
    ),
    _function_tool(
        "intel_signal_add",
        "Record a structured hiring signal for a lead (posting quarantined as weak signal_type).",
        {
            "type": "object",
            "properties": {
                "lead_id": {"type": "string"},
                "signal_type": {"type": "string"},
                "strength": {"type": "string"},
                "rationale": {"type": "string"},
            },
            "required": ["lead_id", "signal_type", "strength", "rationale"],
        },
    ),
    _function_tool(
        "intel_claim_add",
        "Record an evidence claim with one or more citations. Optionally link to company, lead, funding round, or signal_internal_id.",
        {
            "type": "object",
            "properties": {
                "claim_uuid": {"type": "string"},
                "statement": {"type": "string"},
                "confidence": {"type": "string"},
                "claim_type": {"type": "string"},
                "citations": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "source_url": {"type": "string"},
                            "retrieved_at_utc": {"type": "string"},
                            "title": {"type": "string"},
                            "publisher": {"type": "string"},
                            "quote": {"type": "string"},
                            "archived_url": {"type": "string"},
                        },
                        "required": ["source_url", "retrieved_at_utc"],
                    },
                },
                "company_id": {"type": "string"},
                "lead_id": {"type": "string"},
                "funding_company_id": {"type": "string"},
                "funding_round_id": {"type": "string"},
                "signal_internal_id": {"type": "integer"},
            },
            "required": ["claim_uuid", "statement", "confidence", "claim_type", "citations"],
        },
    ),
    _function_tool(
        "intel_lead_add_primary_sources",
        "Attach top-level primary source citations to a lead (evidence bundle).",
        {
            "type": "object",
            "properties": {
                "lead_id": {"type": "string"},
                "citations": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "source_url": {"type": "string"},
                            "retrieved_at_utc": {"type": "string"},
                            "title": {"type": "string"},
                            "publisher": {"type": "string"},
                            "quote": {"type": "string"},
                            "archived_url": {"type": "string"},
                        },
                        "required": ["source_url", "retrieved_at_utc"],
                    },
                },
            },
            "required": ["lead_id", "citations"],
        },
    ),
    _function_tool(
        "intel_executive_motion_add",
        "Record executive arrival/departure/promotion context for a lead.",
        {
            "type": "object",
            "properties": {
                "lead_id": {"type": "string"},
                "motion": {"type": "string"},
                "person_name": {"type": "string"},
                "title": {"type": "string"},
                "effective_date_best_effort": {"type": "string"},
                "stakeholder_category": {"type": "string"},
            },
            "required": ["lead_id", "motion"],
        },
    ),
    _function_tool(
        "intel_interview_prep_put",
        "Upsert interview preparation notes for a lead.",
        {
            "type": "object",
            "properties": {
                "lead_id": {"type": "string"},
                "company_narrative": {"type": "string"},
                "market_context": {"type": "string"},
                "board_priorities": {"type": "array", "items": {"type": "string"}},
                "ninety_day_expectations": {"type": "array", "items": {"type": "string"}},
                "sharp_questions": {"type": "array", "items": {"type": "string"}},
                "risks": {"type": "array", "items": {"type": "string"}},
                "competitive_set": {"type": "array", "items": {"type": "string"}},
                "positioning_angles": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["lead_id"],
        },
    ),
    _function_tool(
        "intel_leads_list",
        "List leads for a bundle, optionally filtered by minimum overall_priority.",
        {
            "type": "object",
            "properties": {
                "bundle_id": {"type": "integer"},
                "minimum_overall_priority": {"type": "number"},
                "limit": {"type": "integer"},
            },
            "required": ["bundle_id"],
        },
    ),
    _function_tool(
        "intel_lead_get_full",
        "Fetch one lead with joined company, funding, signals, claims+citations, motions, interview prep.",
        {
            "type": "object",
            "properties": {"lead_id": {"type": "string"}},
            "required": ["lead_id"],
        },
    ),
    _function_tool(
        "intel_schema_queue_submit",
        "Queue a database schema change (missing column, new table, index, etc.). Does not alter the database; "
        "requests are exported to plans/schema_change_queue.md after pipeline runs for human/CI merge into sql/hiring_intel_schema.sql.",
        {
            "type": "object",
            "properties": {
                "source_agent": {
                    "type": "string",
                    "description": "Which agent or script is filing this (e.g. hiring_research, news_ingestion)",
                },
                "request_title": {"type": "string"},
                "request_description": {"type": "string"},
                "request_kind": {
                    "type": "string",
                    "description": "e.g. missing_column, new_table, new_index, new_constraint, vector_dim, other",
                },
                "related_table": {"type": "string"},
                "related_column": {"type": "string"},
                "bundle_id": {"type": "integer"},
                "proposed_ddl": {"type": "string", "description": "Optional CREATE/ALTER snippet for reviewers"},
            },
            "required": ["source_agent", "request_title", "request_description"],
        },
    ),
    _function_tool(
        "intel_schema_queue_list",
        "List schema change requests by status (default pending) for review.",
        {
            "type": "object",
            "properties": {
                "status": {
                    "type": "string",
                    "description": "pending | exported | dismissed",
                },
                "limit": {"type": "integer"},
            },
            "required": [],
        },
    ),
    _function_tool(
        "intel_schema_queue_export_pending",
        "Flush all pending schema requests to plans/schema_change_queue.md and mark them exported. "
        "Normally run automatically at end of run_intel_pipeline.py; use if you need a mid-run export.",
        {
            "type": "object",
            "properties": {
                "markdown_path": {
                    "type": "string",
                    "description": "Optional path override (default from INTEL_SCHEMA_QUEUE_MD env)",
                },
            },
            "required": [],
        },
    ),
]


HIRING_INTEL_HANDLERS = {
    "intel_initialize_database": hiring_intel_store.initialize_database,
    "intel_bundle_create": hiring_intel_store.bundle_create,
    "intel_bundles_list": hiring_intel_store.bundles_list,
    "intel_target_profile_put": hiring_intel_store.target_profile_put,
    "intel_company_upsert": hiring_intel_store.company_upsert,
    "intel_funding_round_upsert": hiring_intel_store.funding_round_upsert,
    "intel_lead_create": hiring_intel_store.lead_create,
    "intel_lead_update_scores": hiring_intel_store.lead_update_scores,
    "intel_lead_link_funding_context": hiring_intel_store.lead_link_funding_context,
    "intel_lead_add_related_company": hiring_intel_store.lead_add_related_company,
    "intel_signal_add": hiring_intel_store.signal_add,
    "intel_claim_add": hiring_intel_store.claim_add,
    "intel_lead_add_primary_sources": hiring_intel_store.lead_add_primary_sources,
    "intel_executive_motion_add": hiring_intel_store.executive_motion_add,
    "intel_interview_prep_put": hiring_intel_store.interview_prep_put,
    "intel_leads_list": hiring_intel_store.leads_list,
    "intel_lead_get_full": hiring_intel_store.lead_get_full,
    "intel_schema_queue_submit": intel_schema_queue_store.schema_queue_submit,
    "intel_schema_queue_list": intel_schema_queue_store.schema_queue_list,
    "intel_schema_queue_export_pending": intel_schema_queue_export_pending_tool,
}
