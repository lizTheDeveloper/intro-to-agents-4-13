# To install: pip install tavily-python
import inspect
import json
import logging
import os

from tavily import TavilyClient

from conversation_limits import TOOL_RESULT_MAX_CHARS, truncate_with_notice
from hiring_intel_tools import HIRING_INTEL_HANDLERS, HIRING_INTEL_TOOL_DEFINITIONS
from langfuse_tracing import trace_tool_execution
from tool_spec import function_tool as _function_tool

logger = logging.getLogger("intro_agents.tools")

_tavily_client = None


def _get_tavily_client() -> TavilyClient:
    global _tavily_client
    if _tavily_client is None:
        _tavily_client = TavilyClient(os.environ.get("TAVILY_API_KEY"))
    return _tavily_client

_TAVILY_MAX_RESULTS = max(1, int(os.environ.get("AGENT_TAVILY_MAX_RESULTS", "6")))
_TAVILY_SEARCH_DEPTH = os.environ.get("AGENT_TAVILY_SEARCH_DEPTH", "basic")
_TAVILY_SNIPPET_CHARS = max(200, int(os.environ.get("AGENT_TAVILY_CONTENT_PER_RESULT_CHARS", "900")))
_TAVILY_TOP_LEVEL_CHARS = max(500, int(os.environ.get("AGENT_TAVILY_TOP_LEVEL_TEXT_CHARS", "4000")))


tool_definitions = [
    _function_tool(
        "web_search",
        "Search the web using Tavily",
        {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The web search query to perform",
                }
            },
            "required": ["query"],
        },
    ),
    _function_tool(
        "web_extract",
        "Extract clean, structured text content from one or more web page URLs using Tavily",
        {
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "description": "The URL of the page to extract content from",
                }
            },
            "required": ["url"],
        },
    ),
    _function_tool(
        "web_crawl",
        "Crawl a website starting from a URL, following links according to natural-language instructions, using Tavily",
        {
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "description": "The starting URL for the crawl",
                },
                "instructions": {
                    "type": "string",
                    "description": "Natural-language instructions describing what to crawl for or which pages to prioritize",
                },
            },
            "required": ["url", "instructions"],
        },
    ),
    _function_tool(
        "web_map",
        "Discover and list URLs reachable from a site (site map / link discovery) using Tavily",
        {
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "description": "The root or entry URL to map from",
                }
            },
            "required": ["url"],
        },
    ),
    _function_tool(
        "web_research",
        "Run a multi-step Tavily research job for a complex question or task and return synthesized findings",
        {
            "type": "object",
            "properties": {
                "task": {
                    "type": "string",
                    "description": "The research question, topic, or task to investigate in depth",
                }
            },
            "required": ["task"],
        },
    ),
] + HIRING_INTEL_TOOL_DEFINITIONS


def _compact_tavily_payload(payload):
    if not isinstance(payload, dict):
        return payload
    compact = dict(payload)
    for heavy_key in ("raw_content", "images", "image_urls", "favicon"):
        compact.pop(heavy_key, None)
    for text_key in ("content", "answer"):
        blob = compact.get(text_key)
        if isinstance(blob, str) and len(blob) > _TAVILY_TOP_LEVEL_CHARS:
            compact[text_key] = truncate_with_notice(blob, _TAVILY_TOP_LEVEL_CHARS)
    results = compact.get("results")
    if isinstance(results, list):
        slim = []
        for item in results[:_TAVILY_MAX_RESULTS]:
            if not isinstance(item, dict):
                slim.append(item)
                continue
            node = {
                key: item.get(key)
                for key in ("title", "url", "content", "snippet", "score")
                if key in item
            }
            for text_key in ("content", "snippet"):
                blob = node.get(text_key)
                if isinstance(blob, str) and len(blob) > _TAVILY_SNIPPET_CHARS:
                    node[text_key] = truncate_with_notice(blob, _TAVILY_SNIPPET_CHARS)
            slim.append(node)
        compact["results"] = slim
    return compact


def tavily_search(query):
    response = _get_tavily_client().search(
        query=query,
        search_depth=_TAVILY_SEARCH_DEPTH,
        max_results=_TAVILY_MAX_RESULTS,
        include_raw_content=False,
    )
    return _compact_tavily_payload(response)

def tavily_extract(url):
    response = _get_tavily_client().extract(url)
    return _compact_tavily_payload(response)

def tavily_crawl(url,instructions):
    response = _get_tavily_client().crawl(url, instructions=instructions)
    return _compact_tavily_payload(response)

def tavily_map(url):
    response = _get_tavily_client().map(url)
    return _compact_tavily_payload(response)

def tavily_research(task):
    response = _get_tavily_client().research(task)
    return _compact_tavily_payload(response)


available_functions = {
    "web_search": tavily_search,
    "web_extract": tavily_extract,
    "web_crawl": tavily_crawl,
    "web_map": tavily_map,
    "web_research": tavily_research,
    **HIRING_INTEL_HANDLERS,
}

def _filter_call_args(callable_obj, raw_args: dict) -> dict:
    """Drop kwargs the LLM invented so Python handlers do not raise TypeError."""
    try:
        signature = inspect.signature(callable_obj)
    except (TypeError, ValueError):
        return raw_args
    parameters = signature.parameters
    if any(param.kind == inspect.Parameter.VAR_KEYWORD for param in parameters.values()):
        return raw_args
    filtered = {name: raw_args[name] for name in parameters if name in raw_args}
    dropped = set(raw_args) - set(filtered)
    if dropped:
        logger.debug(
            "Ignoring unknown parameters for %s: %s",
            getattr(callable_obj, "__name__", repr(callable_obj)),
            sorted(dropped),
        )
    return filtered


def _resolve_function_name(requested_name: str) -> str:
    if requested_name in available_functions:
        return requested_name
    stem = requested_name.split("<", 1)[0].strip()
    if stem in available_functions:
        logger.warning("Normalized tool name %r -> %r", requested_name, stem)
        return stem
    raise LookupError(requested_name)


def execute_tool_call(tool_call):
    """Parse and execute a single tool call, returning JSON string result."""
    try:
        function_name = _resolve_function_name(tool_call.function.name)
    except LookupError:
        logger.warning("Unknown tool requested: %s", tool_call.function.name)
        return json.dumps(
            {
                "error": "unknown_tool",
                "requested": tool_call.function.name,
                "allowed": sorted(available_functions.keys()),
            }
        )
    function_to_call = available_functions[function_name]
    try:
        function_args = json.loads(tool_call.function.arguments)
    except (json.JSONDecodeError, TypeError) as parse_err:
        logger.error("Failed to parse arguments for %s: %s", function_name, parse_err)
        return json.dumps({"error": "argument_parse_failure", "tool": function_name, "detail": str(parse_err)})
    if not isinstance(function_args, dict):
        function_args = {}
    with trace_tool_execution(function_name, function_args) as tool_span:
        try:
            results = function_to_call(**_filter_call_args(function_to_call, function_args))
        except Exception as tool_err:
            logger.error("Tool %s raised exception: %s", function_name, tool_err, exc_info=True)
            if tool_span is not None:
                tool_span.update(level="ERROR", status_message=str(tool_err)[:4000])
            return json.dumps({"error": "tool_execution_failed", "tool": function_name, "detail": str(tool_err)[:2000]})
        if isinstance(results, dict):
            results = _compact_tavily_payload(results)
        if isinstance(results, (dict, list)):
            text = json.dumps(results, ensure_ascii=False, default=str)
        else:
            text = str(results)
        truncated = truncate_with_notice(text, TOOL_RESULT_MAX_CHARS)
        if tool_span is not None:
            preview_limit = 8000
            tool_span.update(
                output={
                    "truncated_to_chars": len(truncated),
                    "result": truncated if len(truncated) <= preview_limit else truncated[:preview_limit],
                }
            )
        return truncated


def handle_tool_calls(response):
    message = response.choices[0].message
    tool_calls = message.tool_calls
    if not tool_calls:
        return []
    tool_responses = []
    for tool_call in tool_calls:
        results = execute_tool_call(tool_call)
        tool_message = {
            "tool_call_id": tool_call.id,
            "role": "tool",
            "content": results,
        }
        tool_responses.append(tool_message)
    return tool_responses
            