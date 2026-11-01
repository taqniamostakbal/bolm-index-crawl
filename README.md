# bolm-index-crawl — the Bolm own-index project's crawl & build repo

**Repository:** [taqniamostakbal/bolm-index-crawl](https://github.com/taqniamostakbal/bolm-index-crawl) (public)

This repository **is** the Bolm own-index project: a search index of the
Moroccan web (plus an Arab-world tier) built from
[Common Crawl](https://commoncrawl.org) data, distilled for serving by the
Bolm Search worker. The GitHub Actions workflows in this repo are not
borrowed compute — they **are this project's index build and publication
pipeline**, the same way other open projects build and publish their
dataset releases from Actions. Every run produces this project's own
artifacts: crawl chunks, extracted documents, and the distilled index
exports published to a Hugging Face Dataset and mirrored as Releases of
this repo.

Design reference: the Phase-3 host-routes study (GitHub Actions public
repo as crawl host, HF Datasets as landing zone, tier-sharded distilled
exports for D1 serving). Standard GitHub-hosted runners only; matrix
concurrency ≤ 20; runs are spread over days, not bursted.

## Pipeline

```
Discovery ──► chunks (5,000 captures each) ──► Extract (matrix, ≤20 parallel)
    │                                               │
    │ DuckDB over the CC URL-Index                  │ WARC range GETs →
    │ parquet over HTTPS (no AWS/Athena)            │ trafilatura → canon
    │                                               │ dedupe + spam filters
    ▼                                               ▼
 HF Dataset: chunks/<collection>/          HF Dataset: docs/<collection>/
                                                        │
                                                        ▼
                              Distill ──► distilled JSONL + per-shard exports
                                            ├─► HF Dataset: distilled/<collection>/
                                            └─► GitHub Release mirror (this repo)
```

## The distilled record (shared contract)

One JSONL line, exactly these fields:

```json
{"url","title","host","tier","shard","cat","lang","captured","text","snippet"}
```

| Field | Meaning |
|---|---|
| `tier` | 0 = official · 1 = established (press / banks / universities / key services) · 2 = general Moroccan · 3 = Arab-world |
| `shard` | Routing bucket: 0 = officials-admin · 1 = education-research (.ac.ma / universities) · 2 = press-news · 3 = commerce-services (banks / telecom / commerce / jobs portals) · 4 = morocco-general (other .ma) · 5 = arab-world (non-.ma) |
| `text` | Extracted body text, capped at 6,000 chars |
| `snippet` | Meta description, or the first ~240 chars of `text` |
| `captured` | Capture/fetch timestamp when known, else `""` |

Tier/shard assignment is by host rules (`pipeline/common.py`): registry
domains (`seeds/official-registry.json`, 201 officials) and `*.gov.ma` →
tier 0; known press/banks/universities/key services → tier 1; other `.ma`
→ tier 2; non-`.ma` → tier 3. Per-host caps in distill default to
400 (officials) / 300 (press) / 60 (general), CLI-tunable. Output is
sorted Moroccan-first (tier, then shard, then host).

## Layout

```
.github/workflows/
  discovery.yml   DuckDB discovery → 5,000-capture chunks
  extract.yml     Matrix over chunk ids (max-parallel 20) → docs JSONL
  distill.yml     Distilled JSONL + per-shard exports → HF + Release mirror
pipeline/
  discover.py     URL-Index parquet query → chunk files + manifest.json
  extract.py      Chunk → WARC range GETs → extraction → docs JSONL
  distill.py      Docs JSONL → distilled JSONL (contract above)
  common.py       Tier/shard host classification + guarded HF upload
  normalize.py    ┐
  canon.py        │ Stage-2 pipeline modules, copied verbatim (logic
  spam.py         │ unchanged, origin header added) — the proven
  translit.py     ┘ normalisation / dedupe / spam / transliteration code
seeds/
  official-registry.json   The 201-domain official registry {d, c, k}
requirements.txt           requests, trafilatura, duckdb, huggingface_hub
```

## Start procedure — push + secrets → it runs

1. **Push this tree** to the (already created, public) repo
   `taqniamostakbal/bolm-index-crawl` — e.g. clone it, copy this tree in,
   commit, push to `main`.
2. **Set the repository secret** (Settings → Secrets and variables → Actions):
   - `HF_TOKEN` — a Hugging Face token with write access to the dataset repo.
3. **Set the repository variables** (same screen, *Variables* tab):
   - `HF_DATASET_REPO` — e.g. `anasbolm/bolm-index-data` (the landing zone
     for chunks, docs and distilled exports).
   - `CC_COLLECTION` — *(optional)* default Common Crawl collection,
     e.g. `CC-MAIN-2026-43`. If unset, Discovery resolves `latest`.
4. **Run the workflows** (Actions tab → *Run workflow*), in order:
   1. **Discovery** — start with `limit = 5000` (and/or `distilled_only = true`)
      for a smoke pass; check `chunks/manifest.json` in the run summary and
      `chunks/<collection>/` on the HF Dataset.
   2. **Extract** — inputs: the `collection` and a comma-separated list of
      chunk ids from the manifest (e.g. `00000`). Each job uploads its
      `docs/<collection>/chunk-<id>.jsonl` to the HF Dataset in the same job.
   3. **Distill** — inputs: the `collection` and a `release_tag`
      (e.g. `distilled-2026-11`). Publishes `distilled/<collection>/` to the
      HF Dataset and mirrors the gzipped exports as a Release of
      `taqniamostakbal/bolm-index-crawl`.

Everything is parameterized by workflow inputs and the vars above — no
hostnames, tokens or collection ids are hardcoded in the pipeline.

## Local development

```bash
pip install -r requirements.txt   # or reuse the project venv
# Distill smoke test (stdlib-only path; no network, no duckdb needed):
python3 pipeline/distill.py <stage-2 docs.jsonl> \
    --out out/distilled-smoke.jsonl --per-shard
# Prints kept/dropped stats + per-tier and per-shard counts.
```

Notes:

- `discover.py` requires `duckdb` and network access to
  `data.commoncrawl.org`; it is designed to run in the Discovery workflow,
  not on small dev machines.
- `extract.py` uploads to HF only when `HF_TOKEN` is set; without it the
  upload is skipped cleanly and the docs JSONL stays on disk.
- GitHub artifacts are used only as an in-run hand-off. The durable
  archive is the HF Dataset (chunks + docs + distilled) with GitHub
  Releases of this repo as the public mirror for distilled exports.
- Free-only by design: no AWS/Athena, no paid runners, no card anywhere.
