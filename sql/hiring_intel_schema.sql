-- Executive hiring intelligence (PostgreSQL / Neon)
-- Apply with intel_initialize_database (idempotent).

CREATE TABLE IF NOT EXISTS intel_bundle (
  id BIGSERIAL PRIMARY KEY,
  generated_at_utc TEXT NOT NULL,
  agent_name TEXT NOT NULL,
  search_focus TEXT,
  research_window_start TEXT,
  research_window_end TEXT,
  geo_focus_json TEXT,
  sector_focus_json TEXT,
  data_sources_used_json TEXT,
  limitations_json TEXT,
  open_questions_global_json TEXT
);

ALTER TABLE intel_bundle ADD COLUMN IF NOT EXISTS search_focus TEXT;

CREATE TABLE IF NOT EXISTS intel_target_profile (
  bundle_id BIGINT PRIMARY KEY REFERENCES intel_bundle (id) ON DELETE CASCADE,
  role_family_json TEXT,
  domains_json TEXT,
  company_stage_preference_json TEXT,
  must_haves_json TEXT,
  avoid_json TEXT
);

CREATE TABLE IF NOT EXISTS intel_company (
  id BIGSERIAL PRIMARY KEY,
  company_id TEXT NOT NULL UNIQUE,
  legal_name_best_effort TEXT NOT NULL,
  dba_names_json TEXT,
  website_url TEXT,
  hq_region TEXT,
  employee_count_band TEXT,
  sector_labels_json TEXT,
  business_model TEXT,
  one_liner TEXT
);

CREATE TABLE IF NOT EXISTS intel_funding_round (
  id BIGSERIAL PRIMARY KEY,
  company_id BIGINT NOT NULL REFERENCES intel_company (id) ON DELETE CASCADE,
  round_id TEXT NOT NULL,
  round_label TEXT,
  amount_currency TEXT,
  amount_value DOUBLE PRECISION,
  amount_is_approximate BOOLEAN NOT NULL DEFAULT FALSE,
  announced_on TEXT,
  lead_investors_json TEXT,
  participating_investors_json TEXT,
  stated_use_of_proceeds_keywords_json TEXT,
  UNIQUE (company_id, round_id)
);

CREATE TABLE IF NOT EXISTS intel_lead (
  id BIGSERIAL PRIMARY KEY,
  bundle_id BIGINT NOT NULL REFERENCES intel_bundle (id) ON DELETE CASCADE,
  lead_id TEXT NOT NULL UNIQUE,
  hypothesis_statement TEXT NOT NULL,
  primary_company_id BIGINT NOT NULL REFERENCES intel_company (id),
  duplicate_of_lead_internal_id BIGINT REFERENCES intel_lead (id),
  most_recent_funding_round_id BIGINT REFERENCES intel_funding_round (id),
  months_since_last_major_round DOUBLE PRECISION,
  funding_stage_inference TEXT,
  overall_priority DOUBLE PRECISION,
  hypothesis_confidence DOUBLE PRECISION,
  fit_to_target_profile DOUBLE PRECISION,
  timing_urgency DOUBLE PRECISION,
  weights_used_json TEXT,
  next_actions_json TEXT,
  open_questions_json TEXT,
  posting_links_if_any_json TEXT,
  created_at_utc TIMESTAMPTZ NOT NULL DEFAULT (timezone('utc', now()))
);

CREATE TABLE IF NOT EXISTS intel_lead_related_company (
  lead_internal_id BIGINT NOT NULL REFERENCES intel_lead (id) ON DELETE CASCADE,
  company_id BIGINT NOT NULL REFERENCES intel_company (id) ON DELETE CASCADE,
  PRIMARY KEY (lead_internal_id, company_id)
);

CREATE TABLE IF NOT EXISTS intel_signal (
  id BIGSERIAL PRIMARY KEY,
  lead_internal_id BIGINT NOT NULL REFERENCES intel_lead (id) ON DELETE CASCADE,
  signal_type TEXT NOT NULL,
  strength TEXT NOT NULL,
  rationale TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS intel_citation (
  id BIGSERIAL PRIMARY KEY,
  source_url TEXT NOT NULL,
  retrieved_at_utc TEXT NOT NULL,
  title TEXT,
  publisher TEXT,
  quote TEXT,
  archived_url TEXT
);

CREATE TABLE IF NOT EXISTS intel_claim (
  id BIGSERIAL PRIMARY KEY,
  claim_uuid TEXT NOT NULL UNIQUE,
  statement TEXT NOT NULL,
  confidence TEXT NOT NULL,
  claim_type TEXT NOT NULL,
  company_id BIGINT REFERENCES intel_company (id) ON DELETE SET NULL,
  lead_internal_id BIGINT REFERENCES intel_lead (id) ON DELETE CASCADE,
  funding_round_id BIGINT REFERENCES intel_funding_round (id) ON DELETE SET NULL,
  signal_id BIGINT REFERENCES intel_signal (id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS intel_claim_citation (
  claim_id BIGINT NOT NULL REFERENCES intel_claim (id) ON DELETE CASCADE,
  citation_id BIGINT NOT NULL REFERENCES intel_citation (id) ON DELETE CASCADE,
  PRIMARY KEY (claim_id, citation_id)
);

CREATE TABLE IF NOT EXISTS intel_lead_primary_source (
  lead_internal_id BIGINT NOT NULL REFERENCES intel_lead (id) ON DELETE CASCADE,
  citation_id BIGINT NOT NULL REFERENCES intel_citation (id) ON DELETE CASCADE,
  PRIMARY KEY (lead_internal_id, citation_id)
);

CREATE TABLE IF NOT EXISTS intel_executive_motion (
  id BIGSERIAL PRIMARY KEY,
  lead_internal_id BIGINT NOT NULL REFERENCES intel_lead (id) ON DELETE CASCADE,
  person_name TEXT,
  title TEXT,
  motion TEXT NOT NULL,
  effective_date_best_effort TEXT,
  stakeholder_category TEXT
);

CREATE TABLE IF NOT EXISTS intel_interview_prep (
  lead_internal_id BIGINT PRIMARY KEY REFERENCES intel_lead (id) ON DELETE CASCADE,
  company_narrative TEXT,
  market_context TEXT,
  board_priorities_json TEXT,
  ninety_day_expectations_json TEXT,
  sharp_questions_json TEXT,
  risks_json TEXT,
  competitive_set_json TEXT,
  positioning_angles_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_intel_lead_bundle ON intel_lead (bundle_id);

CREATE INDEX IF NOT EXISTS idx_intel_claim_lead ON intel_claim (lead_internal_id);

CREATE INDEX IF NOT EXISTS idx_intel_signal_lead ON intel_signal (lead_internal_id);

CREATE INDEX IF NOT EXISTS idx_intel_funding_company ON intel_funding_round (company_id);

-- pgvector: company news for semantic recall (see news_ingestion_agent.py)
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS intel_company_news_article (
  id BIGSERIAL PRIMARY KEY,
  company_id BIGINT NOT NULL REFERENCES intel_company (id) ON DELETE CASCADE,
  canonical_url TEXT NOT NULL,
  title TEXT,
  published_at TEXT,
  snippet TEXT,
  body_text TEXT,
  ingest_source TEXT NOT NULL,
  search_query TEXT,
  raw_metadata_json TEXT,
  fetched_at_utc TIMESTAMPTZ NOT NULL DEFAULT (timezone('utc', now())),
  UNIQUE (company_id, canonical_url)
);

CREATE TABLE IF NOT EXISTS intel_company_news_chunk (
  id BIGSERIAL PRIMARY KEY,
  article_id BIGINT NOT NULL REFERENCES intel_company_news_article (id) ON DELETE CASCADE,
  chunk_index INTEGER NOT NULL,
  content TEXT NOT NULL,
  embedding vector(1024) NOT NULL,
  UNIQUE (article_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_intel_news_article_company ON intel_company_news_article (company_id);

CREATE INDEX IF NOT EXISTS idx_intel_news_chunk_hnsw
  ON intel_company_news_chunk USING hnsw (embedding vector_cosine_ops);

-- Cross-agent queue: proposed relational / vector schema changes (human or CI applies DDL)
CREATE TABLE IF NOT EXISTS intel_schema_change_queue (
  id BIGSERIAL PRIMARY KEY,
  created_at_utc TIMESTAMPTZ NOT NULL DEFAULT (timezone('utc', now())),
  source_agent TEXT NOT NULL,
  request_kind TEXT,
  request_title TEXT NOT NULL,
  request_description TEXT NOT NULL,
  related_table TEXT,
  related_column TEXT,
  bundle_id BIGINT REFERENCES intel_bundle (id) ON DELETE SET NULL,
  proposed_ddl TEXT,
  status TEXT NOT NULL DEFAULT 'pending',
  processing_notes TEXT,
  processed_at_utc TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_intel_schema_queue_pending ON intel_schema_change_queue (status, created_at_utc DESC);
