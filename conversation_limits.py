"""Size limits, message shrinking, and retry/backoff for Groq chat completions."""

from __future__ import annotations

import json
import logging
import os
import random
import time
from typing import Any, Mapping, Optional

from openai import APIConnectionError, APIStatusError, APITimeoutError, OpenAI, OpenAIError, RateLimitError

from langfuse_tracing import observe_groq_chat_completion_with_raw_response

logger = logging.getLogger("intro_agents.conversation_limits")


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid int for %s=%r; using default %s", name, raw, default)
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        logger.warning("Invalid float for %s=%r; using default %s", name, raw, default)
        return default


MAX_RETRIES = _env_int("AGENT_CHAT_MAX_RETRIES", 8)
BASE_DELAY_S = _env_float("AGENT_CHAT_RETRY_BASE_DELAY_S", 2.0)
MAX_DELAY_S = _env_float("AGENT_CHAT_RETRY_MAX_DELAY_S", 90.0)
MAX_REQUEST_JSON_CHARS = _env_int("AGENT_MAX_REQUEST_JSON_CHARS", 120_000)
MIN_DELAY_BETWEEN_ROUNDS_S = _env_float("AGENT_MIN_DELAY_BETWEEN_ROUNDS_S", 0.35)
MAX_COMPLETION_TOKENS = _env_int("AGENT_MAX_COMPLETION_TOKENS", 2048)
TOOL_RESULT_MAX_CHARS = _env_int("AGENT_TOOL_RESULT_MAX_CHARS", 10_000)
TPM_CHAR_BUCKET = _env_float("AGENT_TPM_CHAR_BUCKET", 1.8)
TPM_HEADROOM_RATIO = _env_float("AGENT_TPM_HEADROOM_RATIO", 0.14)
RPD_HEADROOM_RATIO = _env_float("AGENT_RPD_HEADROOM_RATIO", 0.02)


def _parse_groq_table_int(cell: str) -> Optional[int]:
    cell = cell.strip()
    if cell in ("-", "", "—"):
        return None
    normalized = cell.upper().replace(",", "")
    multiplier = 1
    if normalized.endswith("K"):
        multiplier = 1000
        normalized = normalized[:-1]
    try:
        return int(float(normalized) * multiplier)
    except ValueError:
        return None


# Groq chat model limits: two tiers selected by GROQ_PLAN (or GROQ_RATE_LIMIT_TIER).
# Defaults are conservative (free). Set GROQ_PLAN=dev for Developer / paid console limits.
# Values are RPM, RPD, TPM, TPD (None = not published / not used for chat sizing). Org limits may differ.

GROQ_CHAT_MODEL_LIMITS_FREE: dict[str, dict[str, Optional[int]]] = {
    "allam-2-7b": {"rpm": 30, "rpd": _parse_groq_table_int("7K"), "tpm": 6000, "tpd": 500_000},
    "canopylabs/orpheus-arabic-saudi": {"rpm": 10, "rpd": 100, "tpm": 1200, "tpd": 3600},
    "canopylabs/orpheus-v1-english": {"rpm": 10, "rpd": 100, "tpm": 1200, "tpd": 3600},
    "groq/compound": {"rpm": 30, "rpd": 250, "tpm": 70_000, "tpd": None},
    "groq/compound-mini": {"rpm": 30, "rpd": 250, "tpm": 70_000, "tpd": None},
    "llama-3.1-8b-instant": {"rpm": 30, "rpd": 14_400, "tpm": 6000, "tpd": 500_000},
    "llama-3.3-70b-versatile": {"rpm": 30, "rpd": 1000, "tpm": 12_000, "tpd": 100_000},
    "meta-llama/llama-4-scout-17b-16e-instruct": {"rpm": 30, "rpd": 1000, "tpm": 30_000, "tpd": 500_000},
    "meta-llama/llama-prompt-guard-2-22m": {"rpm": 30, "rpd": 14_400, "tpm": 15_000, "tpd": 500_000},
    "meta-llama/llama-prompt-guard-2-86m": {"rpm": 30, "rpd": 14_400, "tpm": 15_000, "tpd": 500_000},
    "moonshotai/kimi-k2-instruct": {"rpm": 60, "rpd": 1000, "tpm": 10_000, "tpd": 300_000},
    "moonshotai/kimi-k2-instruct-0905": {"rpm": 60, "rpd": 1000, "tpm": 10_000, "tpd": 300_000},
    "openai/gpt-oss-120b": {"rpm": 30, "rpd": 1000, "tpm": 8000, "tpd": 200_000},
    "openai/gpt-oss-20b": {"rpm": 30, "rpd": 1000, "tpm": 8000, "tpd": 200_000},
    "openai/gpt-oss-safeguard-20b": {"rpm": 30, "rpd": 1000, "tpm": 8000, "tpd": 200_000},
    "qwen/qwen3-32b": {"rpm": 60, "rpd": 1000, "tpm": 6000, "tpd": 500_000},
}

# Developer plan (Groq console published caps; TPM/RPD as in product table; TPD often unset for chat).
GROQ_CHAT_MODEL_LIMITS_DEV: dict[str, dict[str, Optional[int]]] = {
    "allam-2-7b": {"rpm": 300, "rpd": 60_000, "tpm": 60_000, "tpd": None},
    "canopylabs/orpheus-arabic-saudi": {"rpm": 250, "rpd": 100_000, "tpm": 50_000, "tpd": None},
    "canopylabs/orpheus-v1-english": {"rpm": 250, "rpd": 100_000, "tpm": 50_000, "tpd": None},
    "groq/compound": {"rpm": 200, "rpd": 20_000, "tpm": 200_000, "tpd": None},
    "groq/compound-mini": {"rpm": 200, "rpd": 20_000, "tpm": 200_000, "tpd": None},
    "llama-3.1-8b-instant": {"rpm": 1000, "rpd": 500_000, "tpm": 250_000, "tpd": None},
    "llama-3.3-70b-versatile": {"rpm": 1000, "rpd": 500_000, "tpm": 300_000, "tpd": None},
    "meta-llama/llama-4-scout-17b-16e-instruct": {"rpm": 1000, "rpd": 500_000, "tpm": 300_000, "tpd": None},
    "meta-llama/llama-prompt-guard-2-22m": {"rpm": 100, "rpd": 50_000, "tpm": 30_000, "tpd": None},
    "meta-llama/llama-prompt-guard-2-86m": {"rpm": 100, "rpd": 50_000, "tpm": 30_000, "tpd": None},
    "moonshotai/kimi-k2-instruct": {"rpm": 1000, "rpd": 500_000, "tpm": 250_000, "tpd": None},
    "moonshotai/kimi-k2-instruct-0905": {"rpm": 1000, "rpd": 500_000, "tpm": 250_000, "tpd": None},
    "openai/gpt-oss-120b": {"rpm": 1000, "rpd": 500_000, "tpm": 250_000, "tpd": None},
    "openai/gpt-oss-20b": {"rpm": 1000, "rpd": 500_000, "tpm": 250_000, "tpd": None},
    "openai/gpt-oss-safeguard-20b": {"rpm": 1000, "rpd": 500_000, "tpm": 150_000, "tpd": None},
    "qwen/qwen3-32b": {"rpm": 1000, "rpd": 500_000, "tpm": 300_000, "tpd": None},
    # Whisper: RPM/RPD from table; TPM column “-”; use ASH-style ceiling for request-size heuristics only.
    "whisper-large-v3": {"rpm": 300, "rpd": 200_000, "tpm": 200_000, "tpd": None},
    "whisper-large-v3-turbo": {"rpm": 400, "rpd": 200_000, "tpm": 400_000, "tpd": None},
}

_GROQ_PLAN_LOGGED: Optional[str] = None


def groq_plan_tier() -> str:
    """
    Rate-limit tier for built-in Groq model tables.

    ``GROQ_PLAN`` or ``GROQ_RATE_LIMIT_TIER``: ``free`` (default), ``dev`` / ``developer`` / ``paid``.
    """
    raw = (os.environ.get("GROQ_PLAN") or os.environ.get("GROQ_RATE_LIMIT_TIER") or "free").strip().lower()
    if raw in ("dev", "developer", "paid", "pro", "business"):
        return "dev"
    return "free"


def active_groq_chat_model_limits() -> dict[str, dict[str, Optional[int]]]:
    return GROQ_CHAT_MODEL_LIMITS_DEV if groq_plan_tier() == "dev" else GROQ_CHAT_MODEL_LIMITS_FREE


def get_chat_model_limits(model_id: str) -> Optional[dict[str, Optional[int]]]:
    global _GROQ_PLAN_LOGGED
    mid = model_id.strip()
    tier = groq_plan_tier()
    primary = GROQ_CHAT_MODEL_LIMITS_DEV if tier == "dev" else GROQ_CHAT_MODEL_LIMITS_FREE
    fallback = GROQ_CHAT_MODEL_LIMITS_FREE if tier == "dev" else GROQ_CHAT_MODEL_LIMITS_DEV
    if _GROQ_PLAN_LOGGED != tier:
        _GROQ_PLAN_LOGGED = tier
        logger.info("Groq rate-limit tier=%s (set GROQ_PLAN=dev for Developer plan caps)", tier)
    if mid in primary:
        return primary[mid]
    return fallback.get(mid)


def effective_max_request_json_chars(model_id: Optional[str]) -> int:
    """Upper bound for serialized `messages` size; tightens for low-TPM Groq chat models."""
    cap = MAX_REQUEST_JSON_CHARS
    if os.environ.get("AGENT_IGNORE_MODEL_TPM_CHAR_CAP", "").lower() in ("1", "true", "yes"):
        return cap
    if not model_id:
        return cap
    limits = get_chat_model_limits(model_id.strip())
    if not limits or limits.get("tpm") is None:
        return cap
    tpm = int(limits["tpm"])
    tpm_cap = max(3500, int(tpm * TPM_CHAR_BUCKET))
    return min(cap, tpm_cap)


_LAST_RATE_HEADERS: Optional[Mapping[str, str]] = None


def parse_groq_reset_interval(value: Optional[str]) -> Optional[float]:
    """Parse Groq reset headers like ``7.66s`` or ``2m59.56s`` into seconds."""
    if not value or not isinstance(value, str):
        return None
    text = value.strip().lower()
    if not text:
        return None
    total_seconds = 0.0
    if "h" in text:
        head, _, text = text.partition("h")
        try:
            total_seconds += float(head) * 3600.0
        except ValueError:
            return None
    if "m" in text:
        head, _, text = text.partition("m")
        try:
            total_seconds += float(head) * 60.0
        except ValueError:
            return None
    text = text.rstrip().removesuffix("s").strip()
    if text:
        try:
            total_seconds += float(text)
        except ValueError:
            return None
    return total_seconds if total_seconds > 0 else None


def _header_int(headers: Optional[Mapping[str, str]], name: str) -> Optional[int]:
    if headers is None:
        return None
    raw = headers.get(name)
    if raw is None:
        return None
    try:
        return int(str(raw).strip())
    except ValueError:
        return None


def _groq_rate_limit_sleep_hint(headers: Optional[Mapping[str, str]]) -> float:
    """Seconds to wait from Groq rate-limit headers (429/413 responses or Retry-After)."""
    if headers is None:
        return 0.0
    retry_after = headers.get("retry-after") or headers.get("Retry-After")
    if retry_after is not None:
        try:
            return max(0.0, float(str(retry_after).strip()))
        except ValueError:
            pass
    reset_tokens = parse_groq_reset_interval(
        headers.get("x-ratelimit-reset-tokens") or headers.get("X-Ratelimit-Reset-Tokens")
    )
    reset_requests = parse_groq_reset_interval(
        headers.get("x-ratelimit-reset-requests") or headers.get("X-Ratelimit-Reset-Requests")
    )
    candidates = [value for value in (reset_tokens, reset_requests) if value is not None]
    if not candidates:
        return 0.0
    return max(candidates)


def note_chat_completion_headers(headers: Optional[Mapping[str, str]]) -> None:
    """Store headers from the last successful chat completion (Groq rate-limit telemetry)."""
    global _LAST_RATE_HEADERS
    _LAST_RATE_HEADERS = headers


def proactive_throttle_delay_s() -> float:
    """Extra delay before the next call when remaining TPM/RPD headroom is low."""
    headers = _LAST_RATE_HEADERS
    if headers is None:
        return 0.0
    rem_tokens = _header_int(headers, "x-ratelimit-remaining-tokens")
    lim_tokens = _header_int(headers, "x-ratelimit-limit-tokens")
    rem_req = _header_int(headers, "x-ratelimit-remaining-requests")
    lim_req = _header_int(headers, "x-ratelimit-limit-requests")
    delay = 0.0
    if rem_tokens is not None and lim_tokens is not None and lim_tokens > 0:
        ratio = rem_tokens / lim_tokens
        if ratio < TPM_HEADROOM_RATIO:
            parsed = parse_groq_reset_interval(
                headers.get("x-ratelimit-reset-tokens") or headers.get("X-Ratelimit-Reset-Tokens")
            )
            if parsed is not None:
                delay = max(delay, min(MAX_DELAY_S, parsed + 0.25))
                logger.info(
                    "Low TPM headroom (remaining=%s limit=%s); backing off ~%.2fs",
                    rem_tokens,
                    lim_tokens,
                    delay,
                )
    if rem_req is not None and lim_req is not None and lim_req > 0:
        ratio = rem_req / lim_req
        if ratio < RPD_HEADROOM_RATIO:
            parsed = parse_groq_reset_interval(
                headers.get("x-ratelimit-reset-requests") or headers.get("X-Ratelimit-Reset-Requests")
            )
            if parsed is not None:
                delay = max(delay, min(MAX_DELAY_S, parsed + 0.25))
                logger.info(
                    "Low RPD headroom (remaining=%s limit=%s); backing off ~%.2fs",
                    rem_req,
                    lim_req,
                    delay,
                )
    return delay


def serialized_messages_size(messages: list[dict[str, Any]]) -> int:
    return len(json.dumps(messages, ensure_ascii=False))


def truncate_with_notice(text: str, max_chars: int) -> str:
    if max_chars <= 0:
        return ""
    if len(text) <= max_chars:
        return text
    reserve = min(200, max_chars // 4)
    head = max_chars - reserve
    omitted = len(text) - head
    notice = f"\n\n[... truncated: omitted approximately {omitted} characters ...]\n"
    return text[:head] + notice


def shrink_messages_for_request(
    messages: list[dict[str, Any]],
    max_json_chars: int = MAX_REQUEST_JSON_CHARS,
) -> None:
    """Shrink `messages` in place until JSON size is under the cap."""
    guard = 0
    while serialized_messages_size(messages) > max_json_chars:
        guard += 1
        if guard > 500:
            logger.error("shrink_messages_for_request: guard tripped; aborting further shrink")
            break
        trimmed = False
        for index, message in enumerate(messages):
            if message.get("role") != "tool":
                continue
            content = message.get("content")
            if not isinstance(content, str) or len(content) < 800:
                continue
            new_budget = max(600, len(content) // 2)
            message["content"] = truncate_with_notice(content, new_budget)
            trimmed = True
            logger.warning(
                "Shrunk tool message at index %d from %d to ~%d chars for request size cap",
                index,
                len(content),
                new_budget,
            )
            break
        if trimmed:
            continue
        if len(messages) > 3:
            dropped = messages.pop(1)
            logger.warning(
                "Dropped oldest post-bootstrap message (role=%s) to satisfy size cap",
                dropped.get("role"),
            )
            continue
        logger.error("Could not shrink messages further; breaking with oversize payload")
        break


def _is_retryable_error(exc: BaseException) -> bool:
    if isinstance(exc, (RateLimitError, APIConnectionError, APITimeoutError)):
        return True
    if isinstance(exc, APIStatusError):
        if exc.status_code in (413, 429) or exc.status_code >= 500:
            return True
        if exc.status_code == 400 and isinstance(exc.body, dict):
            message = str(exc.body.get("message", "")).lower()
            if "rate" in message or "tpm" in message or "token" in message:
                return True
    return False


def chat_completion_create_with_retry(
    client: OpenAI,
    *,
    max_retries: int = MAX_RETRIES,
    base_delay_s: float = BASE_DELAY_S,
    max_delay_s: float = MAX_DELAY_S,
    **kwargs: Any,
) -> Any:
    """Call chat completions with exponential backoff; records Groq rate-limit headers on success."""
    attempt = 0
    while True:
        try:
            completion = observe_groq_chat_completion_with_raw_response(
                client,
                note_headers=note_chat_completion_headers,
                **kwargs,
            )
            return completion
        except OpenAIError as exc:
            if attempt >= max_retries or not _is_retryable_error(exc):
                raise
            response = getattr(exc, "response", None)
            header_map = response.headers if response is not None else None
            groq_hint = _groq_rate_limit_sleep_hint(header_map)
            exp = min(max_delay_s, base_delay_s * (2**attempt))
            jitter = random.uniform(0.0, 0.35 * exp)
            sleep_s = max(groq_hint, exp + jitter)
            model_id = kwargs.get("model")
            limits = get_chat_model_limits(str(model_id)) if model_id else None
            logger.warning(
                "Chat completion failed (retryable): %s; sleeping %.2fs (attempt %d/%d); model_limits=%s",
                exc,
                sleep_s,
                attempt + 1,
                max_retries,
                limits,
            )
            time.sleep(sleep_s)
            attempt += 1


def maybe_throttle_between_rounds() -> None:
    proactive = proactive_throttle_delay_s()
    delay = max(MIN_DELAY_BETWEEN_ROUNDS_S, proactive)
    if delay > 0:
        time.sleep(delay)
