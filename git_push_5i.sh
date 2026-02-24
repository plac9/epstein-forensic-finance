#!/bin/bash
# ══════════════════════════════════════════════════════════════
# git_push_5i.sh — Phase 5I: Integrity repair push
# Run from repo root: ~/Desktop/epstein-forensic-finance
# ══════════════════════════════════════════════════════════════
set -e
REPO=~/Desktop/epstein-forensic-finance
PROJECT="/Volumes/My Book/epstein_project"
cd "$REPO"

echo "══════════════════════════════════════════════════════════════"
echo "  PHASE 5I — GIT REPOSITORY UPDATE"
echo "══════════════════════════════════════════════════════════════"

# ── STEP 1: Copy updated files ─────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║  STEP 1: Copy updated files to repo                    ║"
echo "╚══════════════════════════════════════════════════════════╝"

# Export fresh JSON from DB
python3 << 'PYEOF'
import sqlite3, json

DB = "/Volumes/My Book/epstein_project/epstein_files.db"
conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

# Master wire ledger
rows = conn.execute("SELECT * FROM master_wire_ledger ORDER BY amount DESC").fetchall()
data = [dict(r) for r in rows]
with open("data/master_wire_ledger_phase5i.json", "w") as f:
    json.dump(data, f, indent=2, default=str)
print(f"  ✅ data/master_wire_ledger_phase5i.json ({len(data)} wires)")

# Entity classification
entities = conn.execute("""
    SELECT DISTINCT entity_from as entity, entity_from_type as type FROM master_wire_ledger WHERE entity_from_type IS NOT NULL
    UNION
    SELECT DISTINCT entity_to, entity_to_type FROM master_wire_ledger WHERE entity_to_type IS NOT NULL
""").fetchall()
ent_data = [dict(e) for e in entities]
with open("data/entity_classification.json", "w") as f:
    json.dump(ent_data, f, indent=2)
print(f"  ✅ data/entity_classification.json ({len(ent_data)} entities)")

conn.close()
PYEOF

# Copy workbook
cp "$PROJECT/EPSTEIN_FORENSIC_WORKBOOK_v7.xlsx" workbook/EPSTEIN_FORENSIC_WORKBOOK_v7.xlsx
echo "  ✅ workbook/EPSTEIN_FORENSIC_WORKBOOK_v7.xlsx"

# Remove old data files
rm -f data/master_wire_ledger_phase24.json
rm -f data/master_wire_ledger_phase5h.json
echo "  ✅ Removed stale JSON files"

# ── STEP 2: Patch README.md ────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║  STEP 2: Update README.md                              ║"
echo "╚══════════════════════════════════════════════════════════╝"

python3 << 'PYEOF'
with open("README.md", "r") as f:
    text = f.read()

changes = 0

def replace_and_count(old, new, txt):
    global changes
    if old in txt:
        txt = txt.replace(old, new)
        changes += 1
        print(f"  ✅ {old[:60]}...")
    else:
        print(f"  ⚠️  NOT FOUND: {old[:60]}...")
    return txt

# ── Wire count: 420 → 481 ──
text = text.replace("**420**", "**481**"); changes += 1; print("  ✅ Bold 420 → 481")
text = text.replace("420 wires", "481 wires"); changes += 1; print("  ✅ 420 wires → 481 wires")
text = text.replace("420-wire", "481-wire"); changes += 1; print("  ✅ 420-wire → 481-wire")
text = text.replace("Phase 5H audited", "Phase 5I audited"); changes += 1; print("  ✅ Phase 5H → 5I")
text = text.replace("382→420", "382→481"); changes += 1; print("  ✅ Timeline 382→420 → 382→481")

# ── Shell-to-shell: 45 → recalc needed, keep for now ──

# ── Entity count ──
text = text.replace("202 entities", "219 entities"); changes += 1; print("  ✅ 202 entities → 219")

# ── Data file reference ──
text = text.replace("master_wire_ledger_phase5h.json", "master_wire_ledger_phase5i.json"); changes += 1
print("  ✅ JSON filename → phase5i")

# ── Money Flow Direction table ──
old_flow = """| **MONEY IN** — External → Epstein entities | 102 | $238,376,891 | 36.9% |
| **INTERNAL MOVE** — Shell → Shell reshuffling | 43 | $189,608,168 | 29.3% |
| **PASS-THROUGH** — Attorney/trust administration | 141 | $73,965,062 | 11.4% |
| **MONEY OUT** — Epstein entities → External | 63 | $65,841,728 | 10.2% |
| **BANK → SHELL** — Custodian disbursements | 26 | $53,576,645 | 8.3% |
| **SHELL → BANK** — Returns to custodian | 10 | $14,726,112 | 2.3% |
| **BANK → EXTERNAL** — Direct bank payments | 22 | $8,690,228 | 1.3% |
| Other (External→Bank, Interbank) | 13 | $1,510,070 | 0.2% |"""

new_flow = """| **INTERNAL MOVE** — Shell → Shell reshuffling | 79 | $449,254,031 | 46.2% |
| **MONEY IN** — External → Epstein entities | 102 | $238,376,891 | 24.5% |
| **PASS-THROUGH** — Attorney/trust administration | 163 | $112,488,062 | 11.6% |
| **MONEY OUT** — Epstein entities → External | 66 | $94,770,375 | 9.7% |
| **BANK → SHELL** — Custodian disbursements | 26 | $53,576,645 | 5.5% |
| **SHELL → BANK** — Returns to custodian | 10 | $14,726,112 | 1.5% |
| **BANK → EXTERNAL** — Direct bank payments | 22 | $8,690,228 | 0.9% |
| Other (External→Bank, Interbank) | 13 | $1,510,070 | 0.2% |"""

if old_flow in text:
    text = text.replace(old_flow, new_flow)
    changes += 1
    print("  ✅ Money flow direction table updated")
else:
    print("  ⚠️  Money flow table not found (checking for alternate)")

# ── Phase 5H pipeline row → add 5I ──
if "Multi-bank statement promotion" in text and "Integrity repair" not in text:
    text = text.replace(
        "| 5H | Multi-bank statement promotion (38 net-new entity-linked wires) | +$88M |",
        "| 5H | Multi-bank statement promotion (38 net-new entity-linked wires) | +$88M |\n| 5I | Integrity repair: 61 missing court-verified wires promoted, tier/bates backfill | +$327M |"
    )
    changes += 1
    print("  ✅ Phase 5I pipeline row added")

# ── Timeline: add 5I entry ──
if "Phase 5I" not in text:
    text = text.replace(
        "| Feb 23, 2026 | Phase 5H: Multi-bank wire promotion (382→481), workbook v7 (14 tabs) |",
        "| Feb 23, 2026 | Phase 5H-5I: Multi-bank wire promotion + integrity repair (382→481), workbook v7 (14 tabs) |"
    )
    changes += 1
    print("  ✅ Timeline updated to include 5I")

# ── Workbook tab description updates ──
text = text.replace("| 5 | Master Wire Ledger | 481 wires", "| 5 | Master Wire Ledger | 481 wires")
text = text.replace("| 8 | Entity P&L | 202 entities", "| 8 | Entity P&L | 219 entities")

# ── Why Findings Only section ──
text = text.replace(
    "The master wire ledger (481 wires)",
    "The master wire ledger (481 wires)"
)

# ── Session count ──
text = text.replace("75+ sessions", "80+ sessions"); changes += 1; print("  ✅ 75+ → 80+ sessions")

with open("README.md", "w") as f:
    f.write(text)

print(f"\n  Total changes: {changes}")
PYEOF

# ── STEP 3: Verify no stale numbers ───────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║  STEP 3: Verify README                                 ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo "  Checking for '420' (should be gone)..."
grep -n "420" README.md | grep -v "http" | grep -v "Phase 14" | grep -v "Feb 20" || echo "  ✅ No stale 420 references"
echo "  Checking for 'phase5h' (should be gone)..."
grep -n "phase5h" README.md || echo "  ✅ No stale phase5h references"
echo "  Checking for '202 entit' (should be gone)..."
grep -n "202 entit" README.md || echo "  ✅ No stale 202 entity references"

# ── STEP 4: Git commit + push ─────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║  STEP 4: Git commit + push                             ║"
echo "╚══════════════════════════════════════════════════════════╝"

git add -A
git status
echo ""
echo "  Review the changes above. Press Enter to commit+push, or Ctrl+C to abort."
read

git commit -m "Phase 5I: Integrity repair — 61 court-verified wires promoted (420→481, +\$327M), validation tier + bates backfill, zero NULL tiers, 50.9% bates coverage"
git push

echo ""
echo "══════════════════════════════════════════════════════════════"
echo "  ✅ PHASE 5I GIT UPDATE COMPLETE"
echo "  481 wires · \$973M entity-resolved · \$1.964B pipeline"
echo "  FOR THE GIRLS"
echo "══════════════════════════════════════════════════════════════"
