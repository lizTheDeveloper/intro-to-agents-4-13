"""
Central Langfuse integration for this project.

Tracing is active when LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY are set (and
LANGFUSE_TRACING_DISABLED is not truthy). Optional: LANGFUSE_HOST (same as LANGFUSE_BASE_URL).

See https://langfuse.com/docs/integrations/openai for OpenAI-compatible clients.
"""

from __future__ import annotations

import atexit
import logging
import os
from contextlib import contextmanager
from functools import wraps
from typing import Any, Callable, Iterator, Optional

logger = logging.getLogger("intro_agents.langfuse_tracing")

_atexit_registered = False


def langfuse_credentials_configured() -> bool:
    if os.environ.get("LANGFUSE_TRACING_DISABLED", "").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
        return False
    public_key = os.environ.get("LANGFUSE_PUBLIC_KEY", "").strip()
    secret_key = os.environ.get("LANGFUSE_SECRET_KEY", "").strip()
    return bool(public_key and secret_key)


def _ensure_atexit_flush_registered() -> None:
    global _atexit_registered
    if _atexit_registered or not langfuse_credentials_configured():
        return
    _atexit_registered = True

    def flush_langfuse_on_exit() -> None:
        try:
            from langfuse import get_client

            get_client().flush()
        except Exception as flush_error:
            logger.warning("Langfuse flush on exit failed: %s", flush_error)

    atexit.register(flush_langfuse_on_exit)


def _langfuse_client() -> Any:
    from langfuse import get_client

    _ensure_atexit_flush_registered()
    return get_client()


@contextmanager
def trace_agent_session(agent_name: str, **metadata: Any) -> Iterator[None]:
    """Root or nested agent span (e.g. interactive research loop)."""
    if not langfuse_credentials_configured():
        yield
        return
    try:
        client = _langfuse_client()
        with client.start_as_current_observation(
            name=agent_name,
            as_type="agent",
            metadata=metadata or None,
        ):
            yield
    except Exception as tracing_error:
        logger.warning("Langfuse agent session skipped: %s", tracing_error)
        yield


@contextmanager
def pipeline_bundle_context(bundle_id: int, **extra_metadata: Any) -> Iterator[None]:
    """Attach session_id and metadata to all observations in a pipeline run."""
    if not langfuse_credentials_configured():
        yield
        return
    try:
        from langfuse import propagate_attributes

        _ensure_atexit_flush_registered()
        session_id = f"hiring_intel_bundle_{bundle_id}"
        merged_metadata: dict[str, Any] = {"bundle_id": bundle_id}
        merged_metadata.update(extra_metadata)
        with propagate_attributes(session_id=session_id, metadata=merged_metadata):
            yield
    except Exception as tracing_error:
        logger.warning("Langfuse pipeline context skipped: %s", tracing_error)
        yield


@contextmanager
def trace_retriever_step(name: str, **metadata: Any) -> Iterator[Any]:
    """Retriever-style span for non-tool external fetch (e.g. Tavily search in news ingest)."""
    if not langfuse_credentials_configured():
        yield None
        return
    try:
        with _langfuse_client().start_as_current_observation(
            name=name,
            as_type="retriever",
            metadata=metadata or None,
        ) as span:
            try:
                yield span
            except Exception as exc:
                span.update(level="ERROR", status_message=str(exc)[:2000])
                raise
    except Exception as tracing_error:
        logger.warning("Langfuse retriever trace skipped: %s", tracing_error)
        yield None


@contextmanager
def trace_postgres_write(operation_name: str, **metadata: Any) -> Iterator[None]:
    """Span for a PostgreSQL write path (nests under an active tool/agent span when present)."""
    if not langfuse_credentials_configured():
        yield
        return
    try:
        client = _langfuse_client()
        merged: dict[str, Any] = {"operation": operation_name}
        merged.update(metadata)
        observation = client.start_as_current_observation(
            name=f"postgres.{operation_name}",
            as_type="span",
            metadata=merged,
        )
    except Exception as tracing_error:
        logger.warning("Langfuse postgres trace setup failed: %s", tracing_error)
        yield
        return
    with observation as span:
        try:
            yield
            span.update(output={"status": "ok"})
        except Exception as exc:
            span.update(level="ERROR", status_message=str(exc)[:4000])
            raise


def traced_postgres_function(func: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator for store functions that perform inserts/updates."""

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        with trace_postgres_write(func.__name__):
            return func(*args, **kwargs)

    return wrapper


@contextmanager
def trace_tool_execution(tool_name: str, arguments: dict[str, Any]) -> Iterator[Any]:
    """Langfuse tool observation for each executed tool call."""
    if not langfuse_credentials_configured():
        yield None
        return
    try:
        client = _langfuse_client()
        with client.start_as_current_observation(
            name=f"tool.{tool_name}",
            as_type="tool",
            input={"name": tool_name, "arguments": arguments},
        ) as tool_span:
            yield tool_span
    except Exception as tracing_error:
        logger.warning("Langfuse tool trace skipped: %s", tracing_error)
        yield None


def _usage_details_from_completion(usage: Any) -> Optional[dict[str, int]]:
    if usage is None:
        return None
    details: dict[str, int] = {}
    for key in ("prompt_tokens", "completion_tokens", "total_tokens"):
        value = getattr(usage, key, None)
        if isinstance(value, int):
            details[key] = value
    return details or None


def _chat_completion_output_summary(completion: Any) -> dict[str, Any]:
    choices = getattr(completion, "choices", None)
    if not choices:
        return {"raw": "no_choices"}
    first = choices[0]
    message = getattr(first, "message", None)
    if message is None:
        return {"finish_reason": getattr(first, "finish_reason", None)}
    payload: dict[str, Any] = {
        "role": getattr(message, "role", None),
        "content": getattr(message, "content", None),
    }
    tool_calls = getattr(message, "tool_calls", None)
    if tool_calls:
        serialized = []
        for call in tool_calls:
            function = getattr(call, "function", None)
            serialized.append(
                {
                    "id": getattr(call, "id", None),
                    "name": getattr(function, "name", None) if function else None,
                    "arguments": getattr(function, "arguments", None) if function else None,
                }
            )
        payload["tool_calls"] = serialized
    return payload


def observe_groq_chat_completion_with_raw_response(
    client: Any,
    *,
    note_headers: Optional[Callable[[Any], None]] = None,
    observation_name: str = "groq.chat.completion",
    **create_kwargs: Any,
) -> Any:
    """
    Run chat.completions.with_raw_response.create and return the parsed completion.

    The stock Langfuse OpenAI wrapper instruments ``.create``, not ``with_raw_response``, so we trace here.
    ``note_headers`` is invoked with response headers when present (e.g. Groq rate-limit accounting).
    """
    from openai import OpenAIError

    if not langfuse_credentials_configured():
        raw = client.chat.completions.with_raw_response.create(**create_kwargs)
        headers = getattr(raw, "headers", None)
        if headers is not None and note_headers is not None:
            note_headers(headers)
        return raw.parse()

    client_ref = _langfuse_client()
    generation_cm = client_ref.start_as_current_observation(
        name=observation_name,
        as_type="generation",
        model=create_kwargs.get("model"),
        input={
            "messages": create_kwargs.get("messages"),
            "tools": create_kwargs.get("tools"),
        },
        metadata={"provider": "groq", "api_path": "chat.completions.with_raw_response"},
        model_parameters={
            key: create_kwargs[key]
            for key in ("max_tokens", "temperature", "top_p")
            if key in create_kwargs and create_kwargs[key] is not None
        },
    )
    with generation_cm as generation_span:
        try:
            raw = client.chat.completions.with_raw_response.create(**create_kwargs)
            headers = getattr(raw, "headers", None)
            if headers is not None and note_headers is not None:
                note_headers(headers)
            completion = raw.parse()
            generation_span.update(
                output=_chat_completion_output_summary(completion),
                usage_details=_usage_details_from_completion(getattr(completion, "usage", None)),
                model=create_kwargs.get("model"),
            )
            return completion
        except OpenAIError as exc:
            generation_span.update(level="ERROR", status_message=str(exc)[:4000])
            raise
