#!/usr/bin/env python3
"""Export pending rows from intel_schema_change_queue to plans/schema_change_queue.md."""

from __future__ import annotations

import json
import logging
import sys

from dotenv import load_dotenv

from intel_schema_queue_store import export_pending_schema_requests

logger = logging.getLogger("intro_agents.process_intel_schema_queue")


def main() -> None:
    load_dotenv()
    from logging_config import configure_logging
    configure_logging()
    result = export_pending_schema_requests()
    print(json.dumps(result, indent=2))
    if result.get("exported", 0) == 0:
        sys.exit(0)


if __name__ == "__main__":
    main()
