"""
Centralized logging configuration for the intro_agents project.

Call configure_logging() once from any entry point (CLI main, pipeline, etc.)
before importing other project modules. All project loggers use the
"intro_agents.*" namespace so this configuration controls them uniformly.
"""

from __future__ import annotations

import logging
import os
import sys

_configured = False

LOG_FORMAT = "%(levelname)-8s %(name)s  %(message)s"
LOG_FORMAT_WITH_TIME = "%(asctime)s %(levelname)-8s %(name)s  %(message)s"

PROJECT_LOGGER_NAME = "intro_agents"


def configure_logging(
    level: int | str | None = None,
    include_timestamps: bool = False,
) -> None:
    """
    Set up the root logger and the project-wide 'intro_agents' logger.

    Calling this multiple times is safe; subsequent calls are no-ops unless
    the level changes.

    level: logging level (default from AGENT_LOG_LEVEL env, falling back to INFO).
    include_timestamps: use the timestamped format (useful for pipeline runs).
    """
    global _configured

    if level is None:
        raw = os.environ.get("AGENT_LOG_LEVEL", "INFO").strip().upper()
        level = getattr(logging, raw, logging.INFO)
    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)

    fmt = LOG_FORMAT_WITH_TIME if include_timestamps else LOG_FORMAT

    if not _configured:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter(fmt))

        root = logging.getLogger()
        root.setLevel(level)
        if not root.handlers:
            root.addHandler(handler)
        else:
            root.handlers[0].setFormatter(logging.Formatter(fmt))
            root.handlers[0].setLevel(level)

        project_logger = logging.getLogger(PROJECT_LOGGER_NAME)
        project_logger.setLevel(level)

        _configured = True
    else:
        logging.getLogger().setLevel(level)
        logging.getLogger(PROJECT_LOGGER_NAME).setLevel(level)
