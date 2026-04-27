# CLAUDE.md

Per-project context for Claude Code working in `epstein-forensic-finance`.

## What This Is

Solo forensic-financial reconstruction of the DOJ Epstein Files Transparency Act (EFTA) corpus — 1.48M documents + 503K media items modeled into a 36-table relational database with directional fund-flow analysis.

Repo: `plac9/epstein-forensic-finance` · License: CC BY 4.0 · Status: Active research

## Layout

| Path | Purpose |
|---|---|
| `data/` | Source datasets, extracted text, financial ledgers (large; `.gitignore` excludes raw corpus) |
| `payment_extraction/` | Extraction pipelines for wires, transactions, ledger entries |
| `tools/` | Python analysis scripts (balance walks, classifiers, Benford checks, audits) |
| `narratives/` | 19 published narratives — markdown + per-narrative HTML visualizations |
| `visualizations/` | D3/network HTML for narratives 1–19 |
| `docs/` | METHODOLOGY, FINDINGS, NETWORK, SCHEMA, COMPLIANCE, source appendix template |

## Conventions

- **Source anchoring is mandatory.** Every numeric claim links to a Bates stamp, court exhibit, or DS dataset reference. New narratives use `docs/SOURCE_APPENDIX_TEMPLATE.md`.
- **Tools are one-off scripts**, not a framework. Each `tools/*.py` writes a `*_results.txt` next to itself for reproducibility.
- **Narratives are versioned** by number (`NN_slug.md` + `NN_slug.html`); never renumber published narratives.
- **GitHub Pages publishes from `main`** at `randallscott25-star.github.io/epstein-forensic-finance/` (legacy username — keep links).

## CI / Runners

This is a `plac9/*` personal repo — workflows MUST use `runs-on: ubuntu-latest`, never `[self-hosted, homelab]` (homelab runners are LaClair-Technologies-org-scoped).

## Don't

- Don't commit raw DOJ corpus files (PDFs, media) — they live in `data/` but are gitignored.
- Don't paraphrase narrative findings without re-checking the underlying SQL/CSV; one rounded number breaks Benford analysis downstream.
- Don't change shell-network counts (123 nodes / 313 edges / 14 shells / 8 banks) without re-running `tools/final_audit.py`.
