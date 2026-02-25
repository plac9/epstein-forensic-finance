#!/bin/bash
# Phase 5J Git Push — JSON v26 + session save (NO master_notebook)
set -e
PROJECT="$HOME/Desktop/epstein-forensic-finance"
cd "$PROJECT"

echo "=========================================="
echo "  PHASE 5J GIT PUSH"
echo "=========================================="

# Stage git-tracked files only (NOT master_notebook/)
echo "Staging..."

# JSON for git (lives in workbook/)
git add -v workbook/master_wire_ledger_phase5j.json 2>/dev/null || echo "  phase5j JSON not found"
git add -v workbook/master_wire_ledger_phase5j_flat.json 2>/dev/null || true

# Parser and cleanup scripts that ARE public
git add -v workbook/multi_bank_parser.py 2>/dev/null || true
git add -v workbook/classify_bank_records.py 2>/dev/null || true
git add -v workbook/source_doc_classifier.py 2>/dev/null || true
git add -v workbook/inflation_fix_layer*.py 2>/dev/null || true
git add -v workbook/date_recovery_scan.py 2>/dev/null || true
git add -v workbook/final_audit.py 2>/dev/null || true
git add -v workbook/final_cleanup.py 2>/dev/null || true

# .gitignore update
git add -v .gitignore 2>/dev/null || true

# Session save + any root MDs
git add -v SESSION_SAVE_Phase5J_COMPLETE.md 2>/dev/null || true
git add -v *.md 2>/dev/null || true

echo ""
echo "Staged:"
git diff --cached --name-only
echo ""

read -p "Commit and push? (y/n): " confirm
if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
    git commit -m "Phase 5J: 13-bank parser + 11-layer inflation fix

Multi-bank: 24,563 raw → 1,202 verified (\$430K) across 8 banks
Inflation removed: \$68.7B → \$430K (99.999374%)
Key: Visoski \$241K (Citi), South Street \$12K (UBS), Maxwell (UBS)
JSON v26: master_wire_ledger_phase5j.json
ALL money matters. For the girls."

    git push origin main
    echo "✅ PUSHED"
else
    echo "Aborted."
fi
