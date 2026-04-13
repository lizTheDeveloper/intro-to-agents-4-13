# Data Accrual Mode — 2026-04-13

## Summary
Changed the pipeline from truncate-and-rebuild to accumulate-across-runs.
Each pipeline run creates a new bundle. Companies, funding rounds, and news
articles are shared/updated. Leads and claims are scoped to their bundle but
handle collisions gracefully via upsert.

## Schema Changes

### `search_focus` column on `intel_bundle`
Added `search_focus TEXT` so you can tell bundles apart (what the user searched for).
The ALTER TABLE is idempotent (`ADD COLUMN IF NOT EXISTS`).

## Store Changes

### `bundles_list()` — new function
Lists recent bundles with lead count, company count, and search_focus.
Exposed as `intel_bundles_list` tool (20 tools total now, up from 19).

### `lead_create()` — now upserts on duplicate `lead_id`
Previously, a duplicate `lead_id` across runs would crash with a unique violation.
Now uses ON CONFLICT DO UPDATE so the same lead_id gets refreshed with new data.

### `claim_add()` — now upserts on duplicate `claim_uuid`
Same pattern. Duplicate claim UUIDs update the existing row instead of crashing.

### `bundle_create()` — accepts `search_focus`
Stored alongside the bundle metadata.

## Pipeline Changes

### Default is accrual (no more `--reset` in examples)
- Docstring updated to show accrual as the normal mode
- `--reset` still exists as an explicit escape hatch
- `search_focus` is passed from `--search` into the bundle

### Research prompt namespaces IDs with bundle_id
Lead IDs are now `b{bundle_id}-{slug}` and claim UUIDs `b{bundle_id}-{slug}`.
This prevents collisions when the LLM picks similar names across runs.

## Analyst Changes

### Multi-bundle support
`intel_analyst_agent.py` now supports:
- `--bundle-id N` (single bundle, as before)
- `--bundle-ids 1,2,3` (comma-separated list)
- `--latest-bundles N` (most recent N bundles from `bundles_list`)

`gather_bundle_intel()` accepts `int | list[int]` and deduplicates leads
across bundles.

## Test Results
```
31 passed in 74.22s
```
4 new accrual-specific tests: `bundles_list`, lead upsert, claim upsert, multi-bundle gather.
