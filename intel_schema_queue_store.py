"""
Queue for schema change requests from any agent (tool calls or direct Python).

Pending rows are appended to plans/schema_change_queue.md by export_pending_schema_requests(),
typically at the end of run_intel_pipeline.py. This does not execute DDL automatically.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from db_connection import connect as _connect
from langfuse_tracing import trace_postgres_write

logger = logging.getLogger("intro_agents.intel_schema_queue_store")


def _default_markdown_path() -> Path:
    raw = os.environ.get("INTEL_SCHEMA_QUEUE_MD", "plans/schema_change_queue.md")
    return Path(raw)


def schema_queue_submit(
    source_agent: str,
    request_title: str,
    request_description: str,
    request_kind: Optional[str] = None,
    related_table: Optional[str] = None,
    related_column: Optional[str] = None,
    bundle_id: Optional[int] = None,
    proposed_ddl: Optional[str] = None,
) -> dict[str, Any]:
    """
    Enqueue a schema change idea. Safe for all agents; does not modify DDL.
    """
    with trace_postgres_write(
        "schema_queue_submit",
        source_agent=source_agent,
        request_title=request_title,
        bundle_id=bundle_id,
    ):
        with _connect() as connection:
            if bundle_id is not None:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1 FROM intel_bundle WHERE id = %s", (bundle_id,))
                    if cursor.fetchone() is None:
                        return {"error": "unknown_bundle_id", "bundle_id": bundle_id}
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO intel_schema_change_queue (
                      source_agent, request_kind, request_title, request_description,
                      related_table, related_column, bundle_id, proposed_ddl, status
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'pending')
                    RETURNING id
                    """,
                    (
                        source_agent,
                        request_kind,
                        request_title,
                        request_description,
                        related_table,
                        related_column,
                        bundle_id,
                        proposed_ddl,
                    ),
                )
                row = cursor.fetchone()
                request_id = int(row["id"])
    logger.info("Schema queue request id=%s title=%r", request_id, request_title)
    return {"request_id": request_id, "status": "pending"}


def schema_queue_list(
    status: str = "pending",
    limit: int = 25,
) -> dict[str, Any]:
    limit = max(1, min(int(limit), 100))
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, created_at_utc, source_agent, request_kind, request_title,
                       related_table, related_column, bundle_id, status, proposed_ddl
                FROM intel_schema_change_queue
                WHERE status = %s
                ORDER BY id DESC
                LIMIT %s
                """,
                (status, limit),
            )
            rows = cursor.fetchall()
    return {"status_filter": status, "requests": [dict(row) for row in rows]}


def export_pending_schema_requests(markdown_path: Optional[Path] = None) -> dict[str, Any]:
    """
    Append all pending requests to markdown, then mark them exported.
    Idempotent per row: only rows still pending are exported.
    """
    path = markdown_path or _default_markdown_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with trace_postgres_write("export_pending_schema_requests", markdown_path=str(path)):
        with _connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT * FROM intel_schema_change_queue
                    WHERE status = 'pending'
                    ORDER BY id
                    FOR UPDATE
                    """
                )
                rows = cursor.fetchall()
                if not rows:
                    return {"exported": 0, "markdown_path": str(path)}
                lines = [
                    "",
                    f"## Schema change export — {stamp}",
                    "",
                ]
                exported_ids: list[int] = []
                for row in rows:
                    row_dict = dict(row)
                    request_id = int(row_dict["id"])
                    lines.append(f"### REQ-{request_id} — {row_dict.get('request_title')}")
                    lines.append("")
                    lines.append(f"- **Source agent:** {row_dict.get('source_agent')}")
                    lines.append(f"- **Kind:** {row_dict.get('request_kind') or 'unspecified'}")
                    lines.append(
                        f"- **Related:** `{row_dict.get('related_table') or ''}`.`{row_dict.get('related_column') or ''}`"
                    )
                    lines.append(f"- **Bundle id:** {row_dict.get('bundle_id')}")
                    lines.append(f"- **Description:** {row_dict.get('request_description')}")
                    lines.append("")
                    ddl = row_dict.get("proposed_ddl")
                    if ddl:
                        lines.append("```sql")
                        lines.append(ddl.strip())
                        lines.append("```")
                        lines.append("")
                    lines.append("---")
                    lines.append("")
                    exported_ids.append(request_id)
                block = "\n".join(lines)
                note = f"appended to {path}"
                if path.exists():
                    existing = path.read_text(encoding="utf-8")
                    path.write_text(existing + block, encoding="utf-8")
                else:
                    path.write_text(
                        "# Schema change queue\n\n"
                        "Pending items land here after `export_pending_schema_requests()` "
                        "(e.g. end of `run_intel_pipeline.py`). Review and merge into `sql/hiring_intel_schema.sql`.\n"
                        + block,
                        encoding="utf-8",
                    )
                for request_id in exported_ids:
                    cursor.execute(
                        """
                        UPDATE intel_schema_change_queue
                        SET status = 'exported', processing_notes = %s, processed_at_utc = timezone('utc', now())
                        WHERE id = %s
                        """,
                        (note, request_id),
                    )
    logger.info("Exported %d schema queue row(s) to %s", len(exported_ids), path)
    return {"exported": len(exported_ids), "markdown_path": str(path), "request_ids": exported_ids}
