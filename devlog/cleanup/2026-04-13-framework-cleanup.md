# Framework Cleanup — 2026-04-13

## Summary
Removed cruft, fixed bugs, consolidated duplicated code, and made the agent
framework more robust and observable. All 27 tests (6 existing + 21 new) pass.

## Bugs Fixed

### Double `@contextmanager` on `trace_retriever_step`
`langfuse_tracing.py` had `@contextmanager` applied twice (lines 99-100).
This broke the generator protocol when the inner body raised — contextlib
couldn't properly stop the generator after `throw()`. Removed the duplicate.

### Mutable default argument `tools=[]` in `prompting.prompt()`
Classic Python gotcha — the same list object was shared across all calls.
Changed to `tools=None` with an `if tools is not None` guard. Also fixed the
fact that the `tools` parameter was entirely ignored (the function always used
the module-level `tool_definitions`). Now the parameter actually overrides.

## Logging Improvements

### Centralized logging (`logging_config.py`)
Previously every entry point had its own `logging.basicConfig(...)` call with
slightly different formats. Created `logging_config.configure_logging()` as the
single configuration point. All 6 CLI entry points now use it.

### Langfuse tracing failures elevated from DEBUG to WARNING
All `except Exception` handlers in `langfuse_tracing.py` were logging at
`logger.debug(...)`, which meant tracing misconfiguration was completely invisible
at default log levels. Elevated to `logger.warning(...)`.

### `news_vector_store.py` and `news_text_chunking.py` now log
Both files defined `logger` but never used it. Added operational logging at
key points: article upserts, chunk inserts, semantic search results, and
chunk count mismatches.

## Deduplication

### Shared DB connection (`db_connection.py`)
Four modules (`hiring_intel_store`, `news_vector_store`, `intel_schema_queue_store`,
`intel_db_reset`) each had their own `_conninfo()` / `_connect()` with identical
logic. Extracted into `db_connection.py` with `conninfo()`, `redacted_conninfo()`,
and `connect(register_vector_ext=...)`.

### Shared tool spec (`tool_spec.py`)
`_function_tool()` was identically defined in both `tools.py` and
`hiring_intel_tools.py`. Extracted to `tool_spec.function_tool()` and imported
in both places.

## Exception Handling

### `execute_tool_call` now catches handler exceptions
Previously, if a tool handler (Tavily, DB store function, etc.) raised an
exception, it propagated up and crashed the agent chat loop. Now:
- JSON parse failures on tool arguments → structured error JSON
- Handler exceptions → logged at ERROR with traceback, Langfuse span marked ERROR,
  structured error JSON returned to the LLM so it can retry/adjust

## Cruft Removal

- `main_agent.py`: removed OODA comment block and stale notes
- `.gitignore`: removed stale `hiring_intel.db` / `jobs.db` SQLite references
  (project is PostgreSQL-only), added `reports/`

## New Files
- `logging_config.py` — centralized logging setup
- `db_connection.py` — shared PostgreSQL connection
- `tool_spec.py` — shared tool definition builder
- `tests/test_integration_smoke.py` — 21 integration tests
- `plans/cleanup-plan.md` — plan document for this work

## Test Results
```
27 passed in 48.71s
```
