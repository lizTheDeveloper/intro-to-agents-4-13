# Plan: hiring intel SQLite + tools

1. Maintain canonical DDL in `sql/hiring_intel_schema.sql`.
2. Implement CRUD-style operations in `hiring_intel_store.py` with explicit logging.
3. Register OpenAI-compatible tool definitions in `hiring_intel_tools.py` and merge into `tools.py`.
4. Smoke-test initialization and a minimal lead write path against PostgreSQL (`DATABASE_URL` in `.env`).
5. News CLI: Tavily → chunk → MLX Qwen3 0.6B-class embeddings → `intel_company_news_*` + pgvector; tests without MLX for DB path.
6. Intel analyst CLI: gather bundle + leads + news → compact JSON → Groq markdown brief (or `--no-llm` JSON export).
7. Schema queue: `intel_schema_change_queue` + tools + post-run export to `plans/schema_change_queue.md`.
8. Langfuse: `langfuse_tracing.py` centralizes opt-in tracing; Groq research chat uses manual generation spans (`with_raw_response` path); `intel_analyst_agent` uses `langfuse.openai.OpenAI`; `run_intel_pipeline` uses `propagate_attributes(session_id=…)` for a shared bundle session; hiring intel / news / schema-queue / reset DB writes emit `postgres.*` spans; `tools.execute_tool_call` emits `tool.*` observations.
