"""Shared helper for building OpenAI-compatible function tool definitions."""

from __future__ import annotations

from typing import Any


def function_tool(name: str, description: str, parameters: dict[str, Any]) -> dict[str, Any]:
    """Chat Completions shape (Groq/OpenAI-compatible): name lives under 'function'."""
    return {
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": parameters,
        },
    }
