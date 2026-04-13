# Hiring intel persistence

- PostgreSQL schema must mirror the executive hiring hypothesis model (bundles, companies, funding, leads, signals, claims with citations, interview prep).
- Agents must not rely on ad-hoc SQL; persistence is exposed only through named tools with validated parameters.
- Schema application must be idempotent (`CREATE TABLE IF NOT EXISTS`, etc.).
- Connection string is read from `DATABASE_URL` or `HIRING_INTEL_DATABASE_URL` (never committed; use `.env`).
- Groq chat throttling uses `GROQ_PLAN` / `GROQ_RATE_LIMIT_TIER`: `free` (default) vs `dev` (Developer plan RPM/RPD/TPM table in `conversation_limits.py`).
- Company news is chunked and stored with pgvector (`vector(1024)`); embeddings use MLX + `mlx-community/Qwen3-Embedding-0.6B-4bit-DWQ` by default (`MLX_EMBEDDING_MODEL` override).
- News ingestion runs as a separate CLI (`news_ingestion_agent.py`) so MLX load time and Tavily quotas stay out of the main chat loop.
- Intel analyst (`intel_analyst_agent.py`) loads one bundle’s leads + news, optionally MLX-ranks news, then calls Groq to produce a markdown brief (`--no-llm` dumps JSON only).
- `intel_db_reset.py` / `truncate_all_intel_tables()` wipes all `intel_*` rows; `run_intel_pipeline.py --reset` chains reset → research → news → analyst. News ingest uses `companies_for_bundle_news(bundle_id)` (primary + related companies on leads) so discovery-first runs need no `--company-slug`; optional `--max-news-companies` / `PIPELINE_MAX_NEWS_COMPANIES` caps Tavily cost.
- Schema drift: agents enqueue rows in `intel_schema_change_queue` via tools `intel_schema_queue_submit` / `intel_schema_queue_list` / `intel_schema_queue_export_pending` (or `intel_schema_queue_store.schema_queue_submit` from Python). `run_intel_pipeline.py` and `process_intel_schema_queue.py` append pending items to `plans/schema_change_queue.md` and mark them `exported` (no auto DDL).
- **Langfuse (optional):** set `LANGFUSE_PUBLIC_KEY` and `LANGFUSE_SECRET_KEY` to record traces (Groq chat completions, tool executions, PostgreSQL writes, Tavily retriever steps in news ingest, pipeline session id per `bundle_id`). Use `LANGFUSE_HOST` / `LANGFUSE_BASE_URL` for self-hosted. Set `LANGFUSE_TRACING_DISABLED=1` to force tracing off even when keys exist.
