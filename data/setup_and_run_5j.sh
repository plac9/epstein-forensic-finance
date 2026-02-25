#!/bin/bash
# Phase 5J Setup + Run
# Creates master_notebook/, gitignores it, runs workbook v8 + JSON v26
set -e

PROJECT="$HOME/Desktop/epstein-forensic-finance"
NOTEBOOK="$PROJECT/master_notebook"
WORKBOOK="$PROJECT/workbook"

echo "=========================================="
echo "  PHASE 5J SETUP"
echo "=========================================="

# ── Step 1: Create master_notebook folder ──
echo ""
echo "1. Creating master_notebook/..."
mkdir -p "$NOTEBOOK"

# ── Step 2: Add to .gitignore ──
echo "2. Updating .gitignore..."
if ! grep -q "master_notebook/" "$PROJECT/.gitignore" 2>/dev/null; then
    echo "" >> "$PROJECT/.gitignore"
    echo "# Private workbook scripts and outputs — not for repo" >> "$PROJECT/.gitignore"
    echo "master_notebook/" >> "$PROJECT/.gitignore"
    echo "   Added master_notebook/ to .gitignore"
else
    echo "   Already in .gitignore"
fi

# ── Step 3: Move workbook .py and .xlsx into master_notebook ──
echo "3. Moving workbook files to master_notebook/..."

# Move .xlsx files
for f in "$WORKBOOK"/*.xlsx; do
    [ -f "$f" ] && mv -v "$f" "$NOTEBOOK/" || true
done

# Move workbook generation scripts (keep parser/cleanup scripts in workbook/ for git)
for f in forensic_workbook_v7.py forensic_workbook_v8.py workbook_5j_refresh.py verify_5j_db.py generate_json_v26.py; do
    [ -f "$WORKBOOK/$f" ] && mv -v "$WORKBOOK/$f" "$NOTEBOOK/" || true
done

# Move any old workbook versions
for f in "$WORKBOOK"/forensic_workbook_v*.py; do
    [ -f "$f" ] && mv -v "$f" "$NOTEBOOK/" || true
done

echo ""
echo "   master_notebook/ contents:"
ls -la "$NOTEBOOK/"

# ── Step 4: Run JSON v26 ──
echo ""
echo "=========================================="
echo "4. Generating JSON v26..."
echo "=========================================="
cd "$NOTEBOOK"
python generate_json_v26.py

# ── Step 5: Run Workbook v8 ──
echo ""
echo "=========================================="
echo "5. Building Workbook v8..."
echo "=========================================="
python forensic_workbook_v8.py 2>&1 | tee forensic_workbook_v8_output.txt

echo ""
echo "=========================================="
echo "  ✅ SETUP COMPLETE"
echo ""
echo "  master_notebook/ (PRIVATE - gitignored):"
ls "$NOTEBOOK"/*.xlsx "$NOTEBOOK"/*.py 2>/dev/null | while read f; do echo "    $(basename $f)"; done
echo ""
echo "  workbook/ (PUBLIC - git tracked):"
ls "$WORKBOOK"/*.json 2>/dev/null | while read f; do echo "    $(basename $f)"; done
echo ""
echo "  Next: bash git_push_5j.sh"
echo "=========================================="
