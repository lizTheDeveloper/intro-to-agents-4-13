# Langfuse tracing (2026-04-13)

## What shipped

- `langfuse_tracing.py`: credential gate, `trace_agent_session`, `pipeline_bundle_context` (Langfuse `propagate_attributes` + session id), `trace_postgres_write`, `traced_postgres_function`, `trace_tool_execution`, `trace_retriever_step`, `observe_groq_chat_completion_with_raw_response`, at-exit flush.
- **LLM:** `conversation_limits.chat_completion_create_with_retry` records each Groq attempt as a Langfuse generation (OpenAI SDK `with_raw_response` is not auto-instrumented). `intel_analyst_agent` uses `langfuse.openai.OpenAI` so `chat.completions.create` is auto-traced with `name` / `metadata`.
- **Tools:** `tools.execute_tool_call` wraps each dispatch in `as_type="tool"` with argument capture and truncated result output.
- **Postgres:** `@traced_postgres_function` on mutating APIs in `hiring_intel_store.py`; explicit `trace_postgres_write` in `news_vector_store`, `intel_schema_queue_store`, `intel_db_reset`; pipeline uses `pipeline_bundle_context` after `bundle_id` exists so research/news/analyst subprocess work shares session metadata where context propagates (subprocess analyst starts a separate trace).
- **News ingest:** `trace_agent_session` + `trace_retriever_step` for Tavily search/extract (not LLM, but external retrieval).

## Configuration

- Required when tracing: `LANGFUSE_PUBLIC_KEY`, `LANGFUSE_SECRET_KEY`.
- Optional: `LANGFUSE_HOST` / `LANGFUSE_BASE_URL`, `LANGFUSE_TRACING_DISABLED=1` to disable.
