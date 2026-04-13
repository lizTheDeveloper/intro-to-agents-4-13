from openai import OpenAI
import logging
import os

from conversation_limits import (
    MAX_COMPLETION_TOKENS,
    chat_completion_create_with_retry,
    effective_max_request_json_chars,
    get_chat_model_limits,
    maybe_throttle_between_rounds,
    serialized_messages_size,
    shrink_messages_for_request,
)
from tools import handle_tool_calls, tool_definitions

logger = logging.getLogger("intro_agents.prompting")


def _assistant_message_for_api(message):
    payload = message.model_dump()
    for unsupported_key in ("annotations", "audio", "refusal"):
        payload.pop(unsupported_key, None)
    if payload.get("function_call") is None:
        payload.pop("function_call", None)
    return {key: value for key, value in payload.items() if value is not None}

client = OpenAI(
    api_key=os.environ.get("GROQ_API_KEY"),
    base_url="https://api.groq.com/openai/v1",
)

messages = []

def prompt(input_prompt,model="openai/gpt-oss-20b",tools=[]):
    limits = get_chat_model_limits(model)
    if limits:
        logger.info("Using Groq published limits for %s: %s", model, limits)
    messages.append({
        "role": "user",
        "content": input_prompt
    })
    while True:
        maybe_throttle_between_rounds()
        shrink_messages_for_request(
            messages,
            max_json_chars=effective_max_request_json_chars(model),
        )
        completion_kwargs = {
            "model": model,
            "messages": messages,
            "tools": tool_definitions,
        }
        if MAX_COMPLETION_TOKENS > 0:
            completion_kwargs["max_tokens"] = MAX_COMPLETION_TOKENS
        response = chat_completion_create_with_retry(
            client,
            **completion_kwargs,
        )
        assistant_message = response.choices[0].message
        messages.append(_assistant_message_for_api(assistant_message))

        tool_results = handle_tool_calls(response)
        if not tool_results:
            return assistant_message.content or ""

        messages.extend(tool_results)
        logger.debug(
            "Extended conversation with %d tool result(s); serialized messages ~%d chars",
            len(tool_results),
            serialized_messages_size(messages),
        )