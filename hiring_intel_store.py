"""
Structured persistence for executive hiring signal intelligence (PostgreSQL).
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Optional

from db_connection import connect as _connect, redacted_conninfo as _redacted_conninfo
from langfuse_tracing import traced_postgres_function

logger = logging.getLogger("intro_agents.hiring_intel_store")

_SCHEMA_PATH = Path(__file__).resolve().parent / "sql" / "hiring_intel_schema.sql"


def _dump_json(value: Any) -> Optional[str]:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False)


def _load_json(blob: Any) -> Any:
    if blob is None or blob == "":
        return None
    if isinstance(blob, (dict, list)):
        return blob
    if isinstance(blob, str):
        return json.loads(blob)
    return blob


def _schema_statements() -> list[str]:
    raw = _SCHEMA_PATH.read_text(encoding="utf-8")
    lines = []
    for line in raw.splitlines():
        if line.strip().startswith("--"):
            continue
        lines.append(line)
    text = "\n".join(lines)
    statements: list[str] = []
    for chunk in text.split(";"):
        statement = chunk.strip()
        if statement:
            statements.append(statement)
    return statements


@traced_postgres_function
def initialize_database() -> dict[str, Any]:
    """Apply sql/hiring_intel_schema.sql (idempotent)."""
    with _connect() as connection:
        for statement in _schema_statements():
            with connection.cursor() as cursor:
                cursor.execute(statement)
    logger.info("Hiring intel PostgreSQL schema applied (%s)", _redacted_conninfo())
    return {"ok": True, "database_url_redacted": _redacted_conninfo()}


@traced_postgres_function
def bundle_create(
    generated_at_utc: str,
    agent_name: str,
    search_focus: Optional[str] = None,
    research_window_start: Optional[str] = None,
    research_window_end: Optional[str] = None,
    geo_focus: Optional[list[str]] = None,
    sector_focus: Optional[list[str]] = None,
    data_sources_used: Optional[list[str]] = None,
    limitations: Optional[list[str]] = None,
    open_questions_global: Optional[list[str]] = None,
) -> dict[str, Any]:
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO intel_bundle (
                  generated_at_utc, agent_name, search_focus, research_window_start,
                  research_window_end, geo_focus_json, sector_focus_json,
                  data_sources_used_json, limitations_json, open_questions_global_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    generated_at_utc,
                    agent_name,
                    search_focus,
                    research_window_start,
                    research_window_end,
                    _dump_json(geo_focus),
                    _dump_json(sector_focus),
                    _dump_json(data_sources_used),
                    _dump_json(limitations),
                    _dump_json(open_questions_global),
                ),
            )
            row = cursor.fetchone()
            bundle_id = int(row["id"])
    return {"bundle_id": bundle_id}


@traced_postgres_function
def target_profile_put(
    bundle_id: int,
    role_family: Optional[list[str]] = None,
    domains: Optional[list[str]] = None,
    company_stage_preference: Optional[list[str]] = None,
    must_haves: Optional[list[str]] = None,
    avoid: Optional[list[str]] = None,
) -> dict[str, Any]:
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO intel_target_profile (
                  bundle_id, role_family_json, domains_json, company_stage_preference_json,
                  must_haves_json, avoid_json
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (bundle_id) DO UPDATE SET
                  role_family_json = EXCLUDED.role_family_json,
                  domains_json = EXCLUDED.domains_json,
                  company_stage_preference_json = EXCLUDED.company_stage_preference_json,
                  must_haves_json = EXCLUDED.must_haves_json,
                  avoid_json = EXCLUDED.avoid_json
                """,
                (
                    bundle_id,
                    _dump_json(role_family),
                    _dump_json(domains),
                    _dump_json(company_stage_preference),
                    _dump_json(must_haves),
                    _dump_json(avoid),
                ),
            )
    return {"ok": True, "bundle_id": bundle_id}


@traced_postgres_function
def company_upsert(
    company_id: str,
    legal_name_best_effort: str,
    dba_names: Optional[list[str]] = None,
    website_url: Optional[str] = None,
    hq_region: Optional[str] = None,
    employee_count_band: Optional[str] = None,
    sector_labels: Optional[list[str]] = None,
    business_model: Optional[str] = None,
    one_liner: Optional[str] = None,
) -> dict[str, Any]:
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO intel_company (
                  company_id, legal_name_best_effort, dba_names_json, website_url, hq_region,
                  employee_count_band, sector_labels_json, business_model, one_liner
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (company_id) DO UPDATE SET
                  legal_name_best_effort = EXCLUDED.legal_name_best_effort,
                  dba_names_json = EXCLUDED.dba_names_json,
                  website_url = EXCLUDED.website_url,
                  hq_region = EXCLUDED.hq_region,
                  employee_count_band = EXCLUDED.employee_count_band,
                  sector_labels_json = EXCLUDED.sector_labels_json,
                  business_model = EXCLUDED.business_model,
                  one_liner = EXCLUDED.one_liner
                RETURNING id
                """,
                (
                    company_id,
                    legal_name_best_effort,
                    _dump_json(dba_names),
                    website_url,
                    hq_region,
                    employee_count_band,
                    _dump_json(sector_labels),
                    business_model,
                    one_liner,
                ),
            )
            row = cursor.fetchone()
            internal_id = int(row["id"])
    return {"company_internal_id": internal_id, "company_id": company_id}


@traced_postgres_function
def funding_round_upsert(
    company_id: str,
    round_id: str,
    round_label: Optional[str] = None,
    amount_currency: Optional[str] = None,
    amount_value: Optional[float] = None,
    amount_is_approximate: bool = False,
    announced_on: Optional[str] = None,
    lead_investors: Optional[list[str]] = None,
    participating_investors: Optional[list[str]] = None,
    stated_use_of_proceeds_keywords: Optional[list[str]] = None,
) -> dict[str, Any]:
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT id FROM intel_company WHERE company_id = %s",
                (company_id,),
            )
            company_row = cursor.fetchone()
            if company_row is None:
                return {"error": "unknown_company_id", "company_id": company_id}
            company_internal_id = int(company_row["id"])
            cursor.execute(
                """
                INSERT INTO intel_funding_round (
                  company_id, round_id, round_label, amount_currency, amount_value,
                  amount_is_approximate, announced_on, lead_investors_json,
                  participating_investors_json, stated_use_of_proceeds_keywords_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (company_id, round_id) DO UPDATE SET
                  round_label = EXCLUDED.round_label,
                  amount_currency = EXCLUDED.amount_currency,
                  amount_value = EXCLUDED.amount_value,
                  amount_is_approximate = EXCLUDED.amount_is_approximate,
                  announced_on = EXCLUDED.announced_on,
                  lead_investors_json = EXCLUDED.lead_investors_json,
                  participating_investors_json = EXCLUDED.participating_investors_json,
                  stated_use_of_proceeds_keywords_json = EXCLUDED.stated_use_of_proceeds_keywords_json
                RETURNING id
                """,
                (
                    company_internal_id,
                    round_id,
                    round_label,
                    amount_currency,
                    amount_value,
                    amount_is_approximate,
                    announced_on,
                    _dump_json(lead_investors),
                    _dump_json(participating_investors),
                    _dump_json(stated_use_of_proceeds_keywords),
                ),
            )
            row = cursor.fetchone()
            funding_internal_id = int(row["id"])
    return {
        "funding_round_internal_id": funding_internal_id,
        "company_id": company_id,
        "round_id": round_id,
    }


@traced_postgres_function
def lead_create(
    bundle_id: int,
    lead_id: str,
    hypothesis_statement: str,
    primary_company_id: str,
    duplicate_of_lead_id: Optional[str] = None,
    months_since_last_major_round: Optional[float] = None,
    funding_stage_inference: Optional[str] = None,
    overall_priority: Optional[float] = None,
    hypothesis_confidence: Optional[float] = None,
    fit_to_target_profile: Optional[float] = None,
    timing_urgency: Optional[float] = None,
    weights_used: Optional[dict[str, Any]] = None,
    next_actions: Optional[dict[str, Any]] = None,
    open_questions: Optional[list[str]] = None,
    posting_links_if_any: Optional[list[str]] = None,
) -> dict[str, Any]:
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT id FROM intel_company WHERE company_id = %s",
                (primary_company_id,),
            )
            company_row = cursor.fetchone()
            if company_row is None:
                return {"error": "unknown_primary_company_id", "primary_company_id": primary_company_id}
            primary_internal = int(company_row["id"])
            duplicate_internal: Optional[int] = None
            if duplicate_of_lead_id:
                cursor.execute(
                    "SELECT id FROM intel_lead WHERE lead_id = %s",
                    (duplicate_of_lead_id,),
                )
                dup_row = cursor.fetchone()
                if dup_row is None:
                    return {
                        "error": "unknown_duplicate_of_lead_id",
                        "duplicate_of_lead_id": duplicate_of_lead_id,
                    }
                duplicate_internal = int(dup_row["id"])
            cursor.execute(
                """
                INSERT INTO intel_lead (
                  bundle_id, lead_id, hypothesis_statement, primary_company_id,
                  duplicate_of_lead_internal_id, months_since_last_major_round,
                  funding_stage_inference, overall_priority, hypothesis_confidence,
                  fit_to_target_profile, timing_urgency, weights_used_json, next_actions_json,
                  open_questions_json, posting_links_if_any_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (lead_id) DO UPDATE SET
                  hypothesis_statement = EXCLUDED.hypothesis_statement,
                  primary_company_id = EXCLUDED.primary_company_id,
                  duplicate_of_lead_internal_id = EXCLUDED.duplicate_of_lead_internal_id,
                  months_since_last_major_round = EXCLUDED.months_since_last_major_round,
                  funding_stage_inference = EXCLUDED.funding_stage_inference,
                  overall_priority = EXCLUDED.overall_priority,
                  hypothesis_confidence = EXCLUDED.hypothesis_confidence,
                  fit_to_target_profile = EXCLUDED.fit_to_target_profile,
                  timing_urgency = EXCLUDED.timing_urgency,
                  weights_used_json = EXCLUDED.weights_used_json,
                  next_actions_json = EXCLUDED.next_actions_json,
                  open_questions_json = EXCLUDED.open_questions_json,
                  posting_links_if_any_json = EXCLUDED.posting_links_if_any_json
                RETURNING id
                """,
                (
                    bundle_id,
                    lead_id,
                    hypothesis_statement,
                    primary_internal,
                    duplicate_internal,
                    months_since_last_major_round,
                    funding_stage_inference,
                    overall_priority,
                    hypothesis_confidence,
                    fit_to_target_profile,
                    timing_urgency,
                    _dump_json(weights_used),
                    _dump_json(next_actions),
                    _dump_json(open_questions),
                    _dump_json(posting_links_if_any),
                ),
            )
            row = cursor.fetchone()
            lead_internal_id = int(row["id"])
    return {"lead_internal_id": lead_internal_id, "lead_id": lead_id}


@traced_postgres_function
def lead_update_scores(
    lead_id: str,
    overall_priority: Optional[float] = None,
    hypothesis_confidence: Optional[float] = None,
    fit_to_target_profile: Optional[float] = None,
    timing_urgency: Optional[float] = None,
    weights_used: Optional[dict[str, Any]] = None,
    next_actions: Optional[dict[str, Any]] = None,
    open_questions: Optional[list[str]] = None,
    posting_links_if_any: Optional[list[str]] = None,
) -> dict[str, Any]:
    assignments = []
    values: list[Any] = []
    if overall_priority is not None:
        assignments.append("overall_priority = %s")
        values.append(overall_priority)
    if hypothesis_confidence is not None:
        assignments.append("hypothesis_confidence = %s")
        values.append(hypothesis_confidence)
    if fit_to_target_profile is not None:
        assignments.append("fit_to_target_profile = %s")
        values.append(fit_to_target_profile)
    if timing_urgency is not None:
        assignments.append("timing_urgency = %s")
        values.append(timing_urgency)
    if weights_used is not None:
        assignments.append("weights_used_json = %s")
        values.append(_dump_json(weights_used))
    if next_actions is not None:
        assignments.append("next_actions_json = %s")
        values.append(_dump_json(next_actions))
    if open_questions is not None:
        assignments.append("open_questions_json = %s")
        values.append(_dump_json(open_questions))
    if posting_links_if_any is not None:
        assignments.append("posting_links_if_any_json = %s")
        values.append(_dump_json(posting_links_if_any))
    if not assignments:
        return {"error": "no_fields_to_update", "lead_id": lead_id}
    values.append(lead_id)
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"UPDATE intel_lead SET {', '.join(assignments)} WHERE lead_id = %s",
                values,
            )
            if cursor.rowcount == 0:
                return {"error": "unknown_lead_id", "lead_id": lead_id}
    return {"ok": True, "lead_id": lead_id, "updated_fields": len(assignments)}


@traced_postgres_function
def lead_link_funding_context(
    lead_id: str,
    primary_company_id: str,
    round_id: str,
) -> dict[str, Any]:
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT id FROM intel_lead WHERE lead_id = %s",
                (lead_id,),
            )
            lead_row = cursor.fetchone()
            if lead_row is None:
                return {"error": "unknown_lead_id", "lead_id": lead_id}
            cursor.execute(
                "SELECT id FROM intel_company WHERE company_id = %s",
                (primary_company_id,),
            )
            company_row = cursor.fetchone()
            if company_row is None:
                return {"error": "unknown_company_id", "primary_company_id": primary_company_id}
            cursor.execute(
                """
                SELECT id FROM intel_funding_round
                WHERE company_id = %s AND round_id = %s
                """,
                (int(company_row["id"]), round_id),
            )
            funding_row = cursor.fetchone()
            if funding_row is None:
                return {
                    "error": "unknown_funding_round",
                    "primary_company_id": primary_company_id,
                    "round_id": round_id,
                }
            funding_internal_id = int(funding_row["id"])
            cursor.execute(
                """
                UPDATE intel_lead SET most_recent_funding_round_id = %s
                WHERE lead_id = %s
                """,
                (funding_internal_id, lead_id),
            )
    return {
        "ok": True,
        "lead_id": lead_id,
        "most_recent_funding_round_id": funding_internal_id,
    }


@traced_postgres_function
def lead_add_related_company(lead_id: str, related_company_id: str) -> dict[str, Any]:
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT id FROM intel_lead WHERE lead_id = %s",
                (lead_id,),
            )
            lead_row = cursor.fetchone()
            if lead_row is None:
                return {"error": "unknown_lead_id", "lead_id": lead_id}
            cursor.execute(
                "SELECT id FROM intel_company WHERE company_id = %s",
                (related_company_id,),
            )
            company_row = cursor.fetchone()
            if company_row is None:
                return {"error": "unknown_related_company_id", "related_company_id": related_company_id}
            cursor.execute(
                """
                INSERT INTO intel_lead_related_company (lead_internal_id, company_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                """,
                (int(lead_row["id"]), int(company_row["id"])),
            )
    return {"ok": True, "lead_id": lead_id, "related_company_id": related_company_id}


@traced_postgres_function
def signal_add(
    lead_id: str,
    signal_type: str,
    strength: str,
    rationale: str,
) -> dict[str, Any]:
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT id FROM intel_lead WHERE lead_id = %s",
                (lead_id,),
            )
            lead_row = cursor.fetchone()
            if lead_row is None:
                return {"error": "unknown_lead_id", "lead_id": lead_id}
            cursor.execute(
                """
                INSERT INTO intel_signal (lead_internal_id, signal_type, strength, rationale)
                VALUES (%s, %s, %s, %s)
                RETURNING id
                """,
                (int(lead_row["id"]), signal_type, strength, rationale),
            )
            row = cursor.fetchone()
            signal_internal_id = int(row["id"])
    return {"signal_internal_id": signal_internal_id, "lead_id": lead_id}


@traced_postgres_function
def claim_add(
    claim_uuid: str,
    statement: str,
    confidence: str,
    claim_type: str,
    citations: list[dict[str, Any]],
    company_id: Optional[str] = None,
    lead_id: Optional[str] = None,
    funding_company_id: Optional[str] = None,
    funding_round_id: Optional[str] = None,
    signal_internal_id: Optional[int] = None,
) -> dict[str, Any]:
    if not citations:
        return {"error": "citations_required", "claim_uuid": claim_uuid}
    with _connect() as connection:
        with connection.cursor() as cursor:
            company_internal: Optional[int] = None
            if company_id:
                cursor.execute(
                    "SELECT id FROM intel_company WHERE company_id = %s",
                    (company_id,),
                )
                row = cursor.fetchone()
                if row is None:
                    return {"error": "unknown_company_id", "company_id": company_id}
                company_internal = int(row["id"])
            lead_internal: Optional[int] = None
            if lead_id:
                cursor.execute(
                    "SELECT id FROM intel_lead WHERE lead_id = %s",
                    (lead_id,),
                )
                row = cursor.fetchone()
                if row is None:
                    return {"error": "unknown_lead_id", "lead_id": lead_id}
                lead_internal = int(row["id"])
            funding_internal: Optional[int] = None
            if funding_company_id and funding_round_id:
                cursor.execute(
                    "SELECT id FROM intel_company WHERE company_id = %s",
                    (funding_company_id,),
                )
                crow = cursor.fetchone()
                if crow is None:
                    return {
                        "error": "unknown_funding_company_id",
                        "funding_company_id": funding_company_id,
                    }
                cursor.execute(
                    """
                    SELECT id FROM intel_funding_round
                    WHERE company_id = %s AND round_id = %s
                    """,
                    (int(crow["id"]), funding_round_id),
                )
                frow = cursor.fetchone()
                if frow is None:
                    return {
                        "error": "unknown_funding_round",
                        "funding_company_id": funding_company_id,
                        "funding_round_id": funding_round_id,
                    }
                funding_internal = int(frow["id"])
            if signal_internal_id is not None:
                cursor.execute(
                    "SELECT id FROM intel_signal WHERE id = %s",
                    (signal_internal_id,),
                )
                srow = cursor.fetchone()
                if srow is None:
                    return {
                        "error": "unknown_signal_internal_id",
                        "signal_internal_id": signal_internal_id,
                    }
            cursor.execute(
                """
                INSERT INTO intel_claim (
                  claim_uuid, statement, confidence, claim_type,
                  company_id, lead_internal_id, funding_round_id, signal_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (claim_uuid) DO UPDATE SET
                  statement = EXCLUDED.statement,
                  confidence = EXCLUDED.confidence,
                  claim_type = EXCLUDED.claim_type,
                  company_id = EXCLUDED.company_id,
                  lead_internal_id = EXCLUDED.lead_internal_id,
                  funding_round_id = EXCLUDED.funding_round_id,
                  signal_id = EXCLUDED.signal_id
                RETURNING id
                """,
                (
                    claim_uuid,
                    statement,
                    confidence,
                    claim_type,
                    company_internal,
                    lead_internal,
                    funding_internal,
                    signal_internal_id,
                ),
            )
            row = cursor.fetchone()
            claim_pk = int(row["id"])
            for citation in citations:
                cursor.execute(
                    """
                    INSERT INTO intel_citation (
                      source_url, retrieved_at_utc, title, publisher, quote, archived_url
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        citation["source_url"],
                        citation["retrieved_at_utc"],
                        citation.get("title"),
                        citation.get("publisher"),
                        citation.get("quote"),
                        citation.get("archived_url"),
                    ),
                )
                cite_row = cursor.fetchone()
                citation_pk = int(cite_row["id"])
                cursor.execute(
                    """
                    INSERT INTO intel_claim_citation (claim_id, citation_id)
                    VALUES (%s, %s)
                    """,
                    (claim_pk, citation_pk),
                )
    return {"ok": True, "claim_internal_id": claim_pk, "claim_uuid": claim_uuid}


@traced_postgres_function
def lead_add_primary_sources(lead_id: str, citations: list[dict[str, Any]]) -> dict[str, Any]:
    if not citations:
        return {"error": "citations_required", "lead_id": lead_id}
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT id FROM intel_lead WHERE lead_id = %s",
                (lead_id,),
            )
            lead_row = cursor.fetchone()
            if lead_row is None:
                return {"error": "unknown_lead_id", "lead_id": lead_id}
            lead_internal = int(lead_row["id"])
            inserted = 0
            for citation in citations:
                cursor.execute(
                    """
                    INSERT INTO intel_citation (
                      source_url, retrieved_at_utc, title, publisher, quote, archived_url
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        citation["source_url"],
                        citation["retrieved_at_utc"],
                        citation.get("title"),
                        citation.get("publisher"),
                        citation.get("quote"),
                        citation.get("archived_url"),
                    ),
                )
                cite_row = cursor.fetchone()
                citation_pk = int(cite_row["id"])
                cursor.execute(
                    """
                    INSERT INTO intel_lead_primary_source (lead_internal_id, citation_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (lead_internal, citation_pk),
                )
                inserted += 1
    return {"ok": True, "lead_id": lead_id, "primary_sources_linked": inserted}


@traced_postgres_function
def executive_motion_add(
    lead_id: str,
    motion: str,
    person_name: Optional[str] = None,
    title: Optional[str] = None,
    effective_date_best_effort: Optional[str] = None,
    stakeholder_category: Optional[str] = None,
) -> dict[str, Any]:
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT id FROM intel_lead WHERE lead_id = %s",
                (lead_id,),
            )
            lead_row = cursor.fetchone()
            if lead_row is None:
                return {"error": "unknown_lead_id", "lead_id": lead_id}
            cursor.execute(
                """
                INSERT INTO intel_executive_motion (
                  lead_internal_id, person_name, title, motion, effective_date_best_effort,
                  stakeholder_category
                ) VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    int(lead_row["id"]),
                    person_name,
                    title,
                    motion,
                    effective_date_best_effort,
                    stakeholder_category,
                ),
            )
            row = cursor.fetchone()
            motion_internal_id = int(row["id"])
    return {"executive_motion_internal_id": motion_internal_id, "lead_id": lead_id}


@traced_postgres_function
def interview_prep_put(
    lead_id: str,
    company_narrative: Optional[str] = None,
    market_context: Optional[str] = None,
    board_priorities: Optional[list[str]] = None,
    ninety_day_expectations: Optional[list[str]] = None,
    sharp_questions: Optional[list[str]] = None,
    risks: Optional[list[str]] = None,
    competitive_set: Optional[list[str]] = None,
    positioning_angles: Optional[list[str]] = None,
) -> dict[str, Any]:
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT id FROM intel_lead WHERE lead_id = %s",
                (lead_id,),
            )
            lead_row = cursor.fetchone()
            if lead_row is None:
                return {"error": "unknown_lead_id", "lead_id": lead_id}
            lead_internal = int(lead_row["id"])
            cursor.execute(
                """
                INSERT INTO intel_interview_prep (
                  lead_internal_id, company_narrative, market_context, board_priorities_json,
                  ninety_day_expectations_json, sharp_questions_json, risks_json,
                  competitive_set_json, positioning_angles_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (lead_internal_id) DO UPDATE SET
                  company_narrative = EXCLUDED.company_narrative,
                  market_context = EXCLUDED.market_context,
                  board_priorities_json = EXCLUDED.board_priorities_json,
                  ninety_day_expectations_json = EXCLUDED.ninety_day_expectations_json,
                  sharp_questions_json = EXCLUDED.sharp_questions_json,
                  risks_json = EXCLUDED.risks_json,
                  competitive_set_json = EXCLUDED.competitive_set_json,
                  positioning_angles_json = EXCLUDED.positioning_angles_json
                """,
                (
                    lead_internal,
                    company_narrative,
                    market_context,
                    _dump_json(board_priorities),
                    _dump_json(ninety_day_expectations),
                    _dump_json(sharp_questions),
                    _dump_json(risks),
                    _dump_json(competitive_set),
                    _dump_json(positioning_angles),
                ),
            )
    return {"ok": True, "lead_id": lead_id}


def bundle_get_detail(bundle_id: int) -> dict[str, Any]:
    """Return bundle row plus optional target executive profile for analyst workflows."""
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM intel_bundle WHERE id = %s", (bundle_id,))
            bundle_row = cursor.fetchone()
            if bundle_row is None:
                return {"error": "unknown_bundle_id", "bundle_id": bundle_id}
            cursor.execute(
                "SELECT * FROM intel_target_profile WHERE bundle_id = %s",
                (bundle_id,),
            )
            profile_row = cursor.fetchone()
    bundle_dict = dict(bundle_row)
    for key in list(bundle_dict.keys()):
        if key.endswith("_json") and isinstance(bundle_dict[key], str):
            bundle_dict[key] = _load_json(bundle_dict[key])
    profile_dict: Optional[dict[str, Any]] = None
    if profile_row:
        profile_dict = dict(profile_row)
        for key in list(profile_dict.keys()):
            if key.endswith("_json") and isinstance(profile_dict[key], str):
                profile_dict[key] = _load_json(profile_dict[key])
    return {
        "bundle_id": bundle_id,
        "bundle": bundle_dict,
        "target_profile": profile_dict,
    }


def bundles_list(limit: int = 20) -> dict[str, Any]:
    """List recent bundles with their search focus, lead count, and company count."""
    limit = max(1, min(int(limit), 100))
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT b.id AS bundle_id, b.generated_at_utc, b.agent_name, b.search_focus,
                       COUNT(DISTINCT l.id) AS lead_count,
                       COUNT(DISTINCT c.id) AS company_count
                FROM intel_bundle b
                LEFT JOIN intel_lead l ON l.bundle_id = b.id
                LEFT JOIN intel_company c ON c.id = l.primary_company_id
                GROUP BY b.id
                ORDER BY b.id DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cursor.fetchall()
    return {"bundles": [dict(row) for row in rows]}


def companies_for_bundle_news(bundle_id: int) -> list[dict[str, Any]]:
    """
    Distinct companies tied to a bundle via leads (primary + related companies) for downstream news ingest.
    """
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT company_id, legal_name_best_effort FROM (
                  SELECT c.company_id, c.legal_name_best_effort
                  FROM intel_lead l
                  JOIN intel_company c ON c.id = l.primary_company_id
                  WHERE l.bundle_id = %s
                  UNION
                  SELECT c2.company_id, c2.legal_name_best_effort
                  FROM intel_lead l
                  JOIN intel_lead_related_company rel ON rel.lead_internal_id = l.id
                  JOIN intel_company c2 ON c2.id = rel.company_id
                  WHERE l.bundle_id = %s
                ) AS combined
                ORDER BY company_id
                """,
                (bundle_id, bundle_id),
            )
            rows = cursor.fetchall()
    return [dict(row) for row in rows]


def leads_list(
    bundle_id: int,
    minimum_overall_priority: Optional[float] = None,
    limit: int = 50,
) -> dict[str, Any]:
    limit = max(1, min(int(limit), 200))
    query = """
        SELECT l.lead_id, l.hypothesis_statement, l.overall_priority, l.hypothesis_confidence,
               c.company_id, c.legal_name_best_effort
        FROM intel_lead l
        JOIN intel_company c ON c.id = l.primary_company_id
        WHERE l.bundle_id = %s
    """
    params: list[Any] = [bundle_id]
    if minimum_overall_priority is not None:
        query += " AND l.overall_priority >= %s"
        params.append(minimum_overall_priority)
    query += (
        " ORDER BY CASE WHEN l.overall_priority IS NULL THEN 1 ELSE 0 END, "
        "l.overall_priority DESC NULLS LAST, l.id DESC LIMIT %s"
    )
    params.append(limit)
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
    return {
        "bundle_id": bundle_id,
        "leads": [dict(row) for row in rows],
    }


def lead_get_full(lead_id: str) -> dict[str, Any]:
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT l.*, c.company_id AS primary_company_slug, c.legal_name_best_effort,
                       c.website_url, c.hq_region, c.one_liner
                FROM intel_lead l
                JOIN intel_company c ON c.id = l.primary_company_id
                WHERE l.lead_id = %s
                """,
                (lead_id,),
            )
            lead_row = cursor.fetchone()
            if lead_row is None:
                return {"error": "unknown_lead_id", "lead_id": lead_id}
            lead_internal = int(lead_row["id"])
            payload = dict(lead_row)
            for key in list(payload.keys()):
                if key.endswith("_json") and isinstance(payload[key], str):
                    payload[key] = _load_json(payload[key])

            cursor.execute(
                """
                SELECT co.company_id, co.legal_name_best_effort
                FROM intel_lead_related_company rel
                JOIN intel_company co ON co.id = rel.company_id
                WHERE rel.lead_internal_id = %s
                """,
                (lead_internal,),
            )
            related = cursor.fetchall()
            payload["related_companies"] = [dict(row) for row in related]

            cursor.execute(
                """
                SELECT f.* FROM intel_funding_round f
                WHERE f.company_id = %s
                ORDER BY CASE WHEN f.announced_on IS NULL THEN 1 ELSE 0 END, f.announced_on DESC NULLS LAST, f.id DESC
                """,
                (int(lead_row["primary_company_id"]),),
            )
            funding_rows = cursor.fetchall()
            funding_list = []
            for row in funding_rows:
                item = dict(row)
                for json_key in (
                    "lead_investors_json",
                    "participating_investors_json",
                    "stated_use_of_proceeds_keywords_json",
                ):
                    if json_key in item and isinstance(item[json_key], str):
                        item[json_key] = _load_json(item[json_key])
                funding_list.append(item)
            payload["funding_rounds_for_primary_company"] = funding_list

            if lead_row["most_recent_funding_round_id"]:
                cursor.execute(
                    "SELECT * FROM intel_funding_round WHERE id = %s",
                    (int(lead_row["most_recent_funding_round_id"]),),
                )
                fr = cursor.fetchone()
                payload["most_recent_funding_round"] = dict(fr) if fr else None
            else:
                payload["most_recent_funding_round"] = None

            cursor.execute(
                "SELECT * FROM intel_signal WHERE lead_internal_id = %s ORDER BY id",
                (lead_internal,),
            )
            signals = cursor.fetchall()
            payload["signals"] = [dict(row) for row in signals]

            cursor.execute(
                """
                SELECT cl.* FROM intel_claim cl
                WHERE cl.lead_internal_id = %s
                   OR cl.signal_id IN (SELECT id FROM intel_signal WHERE lead_internal_id = %s)
                ORDER BY cl.id
                """,
                (lead_internal, lead_internal),
            )
            claims = cursor.fetchall()
            claim_payload = []
            for claim_row in claims:
                claim_dict = dict(claim_row)
                cursor.execute(
                    """
                    SELECT ci.* FROM intel_citation ci
                    JOIN intel_claim_citation j ON j.citation_id = ci.id
                    WHERE j.claim_id = %s
                    """,
                    (int(claim_row["id"]),),
                )
                cite_rows = cursor.fetchall()
                claim_dict["citations"] = [dict(r) for r in cite_rows]
                claim_payload.append(claim_dict)
            payload["claims"] = claim_payload

            cursor.execute(
                """
                SELECT ci.* FROM intel_citation ci
                JOIN intel_lead_primary_source j ON j.citation_id = ci.id
                WHERE j.lead_internal_id = %s
                """,
                (lead_internal,),
            )
            primary_sources = cursor.fetchall()
            payload["primary_sources"] = [dict(row) for row in primary_sources]

            cursor.execute(
                """
                SELECT * FROM intel_executive_motion
                WHERE lead_internal_id = %s
                ORDER BY id
                """,
                (lead_internal,),
            )
            motions = cursor.fetchall()
            payload["executive_motions"] = [dict(row) for row in motions]

            cursor.execute(
                "SELECT * FROM intel_interview_prep WHERE lead_internal_id = %s",
                (lead_internal,),
            )
            prep = cursor.fetchone()
            if prep:
                prep_dict = dict(prep)
                for json_key in (
                    "board_priorities_json",
                    "ninety_day_expectations_json",
                    "sharp_questions_json",
                    "risks_json",
                    "competitive_set_json",
                    "positioning_angles_json",
                ):
                    if json_key in prep_dict and isinstance(prep_dict[json_key], str):
                        prep_dict[json_key] = _load_json(prep_dict[json_key])
                payload["interview_prep"] = prep_dict
            else:
                payload["interview_prep"] = None
    return payload
