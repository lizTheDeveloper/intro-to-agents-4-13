#!/usr/bin/env python3
"""
Interactive hiring research agent — single-shot CLI entry point.

Runs one prompt through the Groq-backed research agent with all available tools
(Tavily web search + hiring intel PostgreSQL ops).
"""

from dotenv import load_dotenv

load_dotenv()

from logging_config import configure_logging

configure_logging()

from prompting import prompt
from tools import tool_definitions

print(
    prompt(
        "Find me interesting CTO or Director of Engineering roles, "
        "preferably with Python and JavaScript, preferably in EdTech",
        tools=tool_definitions,
        model="qwen/qwen3-32b",
    )
)
