# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

> **Universal baseline:** Follow `~/dev/.standards/AGENT-STANDARDS.md` for session logging and protocols.

Per-project context for Claude Code working in `epstein-forensic-finance`.

## What This Is

Solo forensic-financial reconstruction of the DOJ Epstein Files Transparency Act (EFTA) corpus — 1.48M documents + 503K media items modeled into a 36-table relational database with directional fund-flow analysis. The **published** artifact is methodology + findings + summary data; the raw 8.03 GB SQLite database and full extraction pipeline are deliberately NOT in the repo (see "Findings-only design" below and README § "Why Findings Only").

Repo: `plac9/epstein-forensic-finance` (GitHub remote points at the legacy `randallscott25-star/` namespace — keep it) · License: CC BY 4.0 · Status: Active research

## Layout

| Path | Purpose |
|---|---|
| `data/` | Published summary datasets — `master_wire_ledger_phase5i.json`, `..._phase5j.json` (+ flat), `entity_classification.json`. Raw corpus / DB are gitignored and absent. |
| `payment_extraction/` | Extraction pipelines for wires, CHIPS, SWIFT/MT103, checks, book transfers, bank statements (`*_extractor.py`, `full_sweep_pipeline.py`, `extraction_framework.py`). |
| `tools/` | One-off Python analysis scripts (balance walks, classifiers, Benford/round-number checks, dedup audits, link injection). |
| `narratives/` | 19 published narratives — `NN_slug.md` (markdown + source appendices). |
| `visualizations/` | Interactive D3/network HTML (shell network, N17 one-way-money, N19 blueprint). One full narrative is rendered as HTML at `narratives/19_grand_opus_narrative.html`. |
| `docs/` | METHODOLOGY, FINDINGS, NETWORK, SCHEMA, COMPLIANCE, `SOURCE_APPENDIX_TEMPLATE.md`. |

## Build / Run / Test

There is **no build system, package manifest, lint config, or test suite** — no `requirements.txt`, `pyproject.toml`, `Makefile`, or `.github/` workflows. Tools are standalone Python 3 scripts run directly:

```bash
python3 tools/final_audit.py          # run a single analysis script
python3 payment_extraction/chips_extractor.py
```

- **Scripts expect the source SQLite DB at `~/Desktop/epstein_files.db`** (hardcoded `DB_PATH = os.path.expanduser("~/Desktop/epstein_files.db")`). That database is gitignored and not distributed, so most `tools/` and `payment_extraction/` scripts will fail to run from a clean clone — they document the methodology rather than being a runnable-from-checkout pipeline. There is no "single test" to run because there are no tests; the closest reproducibility unit is re-running one script against the DB and diffing its `*_results.txt` / `*_results.json` sidecar.
- Common deps used by the scripts: `sqlite3` (stdlib), `re`, `json`, `collections` (stdlib). A few may use `spacy` for NLP; install ad hoc only if a script imports it.

## Conventions

- **Source anchoring is mandatory.** Every numeric claim links to a Bates stamp, court exhibit, or DS dataset reference. New narratives use `docs/SOURCE_APPENDIX_TEMPLATE.md`.
- **Tools are one-off scripts, not a framework.** Each script writes a `*_results.txt` / `*_results.json` next to itself for reproducibility (committed alongside the code).
- **Narratives are versioned** by number (`NN_slug.md`); never renumber published narratives.
- **GitHub Pages publishes from `main`** at `randallscott25-star.github.io/epstein-forensic-finance/` (legacy username — keep links). Narrative/visualization HTML is served from there; cross-links between narratives and visualizations use the Pages URL, not relative paths, so HTML renders instead of downloading.
- **`.gitignore` quirk:** a later block ignores `*.py`, `*.sh`, `*.json`, `*.txt`, yet 37 `.py` scripts, the `data/*.json` ledgers, and `*_results.txt` sidecars ARE tracked (added before/around those rules). Don't assume a Python file is ignored — check `git ls-files` before "re-adding" anything.

## Findings-only design (do not "fix" by publishing the pipeline)

Per README § "Why Findings Only — No Source Code or Database" and § "Ethical Standards", omission of the raw DB and full pipeline is intentional (AICPA SSFS No. 1 / AU-C §230 working-paper control, victim protection). Treat the absent database and the `~/Desktop/...` hardcoded path as by-design, not bugs to refactor.

## CI / Runners

<<<<<<< HEAD
No GitHub Actions workflows exist in this repo today (no `.github/workflows/`). This is a `plac9/*` personal repo — if CI is ever added, workflows MUST use `runs-on: ubuntu-latest`, never `[self-hosted, homelab]` (homelab runners are LaClair-Technologies-org-scoped). GitHub Pages publishes directly from `main` (no Actions deploy step).
=======
This is a `plac9/*` personal repo — any future workflow MUST use `runs-on: ubuntu-latest`, never `[self-hosted, homelab]` (homelab runners are LaClair-Technologies-org-scoped). There are currently no workflows.
>>>>>>> f916a16 (docs(claude): standardize CLAUDE.md header + accuracy refresh)

## Don't

- Don't commit raw DOJ corpus files (PDFs, media) or the SQLite DB — they live outside the repo (`~/Desktop/epstein_files.db`, gitignored).
- Don't surface victim names, identifying details, or testimony content — victim-adjacent redactions are noted by proximity only.
- Don't paraphrase narrative findings without re-checking the underlying SQL/CSV; one rounded number breaks Benford analysis downstream. All amounts are "(Unverified)" automated extractions — preserve that tag.
- Don't change shell-network counts (123 nodes / 313 edges / 14 shells / 8 banks) without re-running `tools/final_audit.py`.
