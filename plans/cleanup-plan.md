# Agent Framework Cleanup Plan

## Problem Statement
The agent framework has accumulated cruft, swallowed exceptions, inconsistent logging,
duplicated helpers, and Langfuse tracing gaps that make debugging and reliability worse.

## Changes

### Bugs
1. **Double `@contextmanager`** on `trace_retriever_step` (langfuse_tracing.py:99-100)
2. **Mutable default arg** `tools=[]` in `prompting.py:prompt()`

### Logging
3. Create `logging_config.py` — single place for format, level, handler config
4. Wire all entry points to call `configure_logging()` instead of per-file `basicConfig`
5. Add operational logging to `news_vector_store.py` and `news_text_chunking.py`
6. Elevate Langfuse misconfiguration from DEBUG → WARNING (silent failures are the worst kind)

### Deduplication
7. Extract `db_connection.py` — shared `_conninfo()` / `_connect()` used by 4 modules
8. Remove duplicate `_function_tool()` from `hiring_intel_tools.py` (already in `tools.py`)

### Exception Handling
9. `tools.py execute_tool_call` — catch handler exceptions, log at ERROR, return structured error JSON
10. Langfuse tracing context managers — log at WARNING not DEBUG when tracing setup fails

### Cruft Removal
11. Clean `main_agent.py` — remove OODA comment block, stale notes
12. Remove stale SQLite references from `.gitignore`

### Testing
13. Write integration smoke test that exercises the real module wiring (not mocked)

### Documentation
14. Devlog entry summarizing all changes
