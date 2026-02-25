"""
narrative_sql_tools.py
Epstein Forensic Finance — SQL queries backing all 18 data narratives.

Each function hits the local SQLite database and returns the dataset
that feeds one published narrative. Run any function standalone for
a quick console dump, or import into a notebook / pipeline.

Database: ~/Desktop/epstein_files.db (~8 GB)
Phase: 5L (publication_ledger live, four-tier GAGAS framework)

Author: Randall Scott Taylor
"""

import sqlite3
import os
from collections import defaultdict
from contextlib import contextmanager
from math import log10, floor

# ── connection ──────────────────────────────────────────────

DB_PATH = os.path.expanduser("~/Desktop/epstein_files.db")

@contextmanager
def db():
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA cache_size=-64000")
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def run(sql, params=None):
    """Quick one-shot query. Returns list of dicts."""
    with db() as conn:
        cur = conn.execute(sql, params or [])
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


# ════════════════════════════════════════════════════════════
# NARRATIVE 01 — The Jeepers Pipeline
# 24 wires, $57.9M, Exhibit C capitalization of NOW/SuperNow
# ════════════════════════════════════════════════════════════

def n01_jeepers_pipeline():
    """Exhibit C wires: Jeepers Inc → personal checking accounts."""
    sql = """
        SELECT date, entity_from, entity_to, amount, bates, exhibit
        FROM verified_wires
        WHERE exhibit = 'C'
          AND (entity_from LIKE '%Jeepers%' OR entity_to LIKE '%Jeepers%')
        ORDER BY date
    """
    rows = run(sql)
    total = sum(r['amount'] for r in rows)
    print(f"N01 — Jeepers Pipeline: {len(rows)} wires, ${total:,.2f}")
    for r in rows:
        print(f"  {r['date']}  ${r['amount']:>14,.2f}  {r['bates']}  {r['entity_from']} → {r['entity_to']}")
    return rows


# ════════════════════════════════════════════════════════════
# NARRATIVE 02 — Art Market as Liquidity Channel
# Sotheby's + Christie's into Haze Trust → downstream shells
# ════════════════════════════════════════════════════════════

def n02_art_market():
    """Exhibit D: auction house proceeds into The Haze Trust + outflows."""

    # inflows from auction houses
    inflows_sql = """
        SELECT date, entity_from, entity_to, amount, bates
        FROM verified_wires
        WHERE exhibit = 'D'
          AND (entity_from LIKE '%Christie%' OR entity_from LIKE '%Sotheby%')
        ORDER BY date
    """
    inflows = run(inflows_sql)

    # all Haze Trust outflows
    outflows_sql = """
        SELECT date, entity_from, entity_to, amount, bates
        FROM verified_wires
        WHERE entity_from LIKE '%Haze%Trust%'
          AND entity_to NOT LIKE '%Haze%'
        ORDER BY date
    """
    outflows = run(outflows_sql)

    in_total = sum(r['amount'] for r in inflows)
    out_total = sum(r['amount'] for r in outflows)
    print(f"N02 — Art Market: {len(inflows)} inflows ${in_total:,.2f}, {len(outflows)} outflows ${out_total:,.2f}")
    return {"inflows": inflows, "outflows": outflows}


# ════════════════════════════════════════════════════════════
# NARRATIVE 03 — The Plan D Question
# $18M out to Leon Black, near-zero in. Where'd Plan D get funded?
# ════════════════════════════════════════════════════════════

def n03_plan_d():
    """Plan D LLC flows + Black family → Southern Trust inflows."""

    plan_d_sql = """
        SELECT date, entity_from, entity_to, amount, bates, exhibit
        FROM verified_wires
        WHERE entity_from LIKE '%Plan D%' OR entity_to LIKE '%Plan D%'
        ORDER BY date
    """
    plan_d = run(plan_d_sql)

    black_sql = """
        SELECT date, entity_from, entity_to, amount, bates, exhibit
        FROM verified_wires
        WHERE exhibit = 'A'
          AND (entity_from LIKE '%Black%' OR entity_from LIKE '%Narrow%'
               OR entity_from LIKE '%Elysium%')
        ORDER BY date
    """
    black_inflows = run(black_sql)

    pd_out = sum(r['amount'] for r in plan_d if 'Black' in (r['entity_to'] or ''))
    pd_in = sum(r['amount'] for r in plan_d if 'Plan D' in (r['entity_to'] or ''))
    blk = sum(r['amount'] for r in black_inflows)
    print(f"N03 — Plan D: outflows to Black ${pd_out:,.2f}, inflows ${pd_in:,.2f}")
    print(f"       Black family → Southern Trust: {len(black_inflows)} wires, ${blk:,.2f}")
    return {"plan_d_flows": plan_d, "black_inflows": black_inflows}


# ════════════════════════════════════════════════════════════
# NARRATIVE 04 — Chain-Hop Anatomy
# 4-tier shell mapping + double-count removal ($311M inflation)
# ════════════════════════════════════════════════════════════

EPSTEIN_SHELLS = [
    'Southern Trust', 'Southern Financial', 'Haze Trust', 'Gratitude America',
    'Jeepers', 'Plan D', 'BV70', 'Financial Trust', 'Butterfly Trust',
    'Outgoing Money Trust', 'NES LLC', "Villard Int'l",
    'PJLP Investments', 'Zorro Trust'
]

def _is_shell(name):
    if not name:
        return False
    low = name.lower()
    return any(s.lower() in low for s in EPSTEIN_SHELLS)

def n04_chain_hop():
    """All wires classified as shell-to-shell vs external-to-shell."""
    sql = """
        SELECT date, entity_from, entity_to, amount, bates, exhibit
        FROM verified_wires
        ORDER BY date
    """
    wires = run(sql)

    shell_to_shell = []
    external_in = []
    external_out = []

    for w in wires:
        fr_shell = _is_shell(w['entity_from'])
        to_shell = _is_shell(w['entity_to'])
        if fr_shell and to_shell:
            shell_to_shell.append(w)
        elif not fr_shell and to_shell:
            external_in.append(w)
        elif fr_shell and not to_shell:
            external_out.append(w)

    s2s_total = sum(r['amount'] for r in shell_to_shell)
    ext_in = sum(r['amount'] for r in external_in)
    ext_out = sum(r['amount'] for r in external_out)
    print(f"N04 — Chain-Hop: {len(shell_to_shell)} shell-to-shell wires ${s2s_total:,.2f} (double-count)")
    print(f"       External in: {len(external_in)} wires ${ext_in:,.2f}")
    print(f"       External out: {len(external_out)} wires ${ext_out:,.2f}")
    return {
        "shell_to_shell": shell_to_shell,
        "external_in": external_in,
        "external_out": external_out
    }


# ════════════════════════════════════════════════════════════
# NARRATIVE 05 — Deutsche Bank's Role
# 38 wires, $56.8M — every major entity touched DB
# ════════════════════════════════════════════════════════════

def n05_deutsche_bank():
    """Deutsche Bank wire activity from verified ledger + corpus mentions."""

    wire_sql = """
        SELECT entity_to, COUNT(*) as wires, SUM(amount) as total
        FROM verified_wires
        WHERE exhibit IN ('A','B','C','D','E')
        GROUP BY entity_to
        ORDER BY total DESC
    """
    by_recipient = run(wire_sql)

    # corpus presence
    corpus_sql = """
        SELECT COUNT(DISTINCT e.file_id) as file_count,
               COUNT(*) as mention_count
        FROM entities e
        JOIN files f ON e.file_id = f.id
        WHERE e.entity_text LIKE '%Deutsche Bank%'
          AND f.doc_type IN ('financial', 'bank_statement', 'spreadsheet')
    """
    corpus = run(corpus_sql)

    print(f"N05 — Deutsche Bank wires by recipient:")
    for r in by_recipient:
        print(f"  {r['entity_to']:40s}  {r['wires']:>3} wires  ${r['total']:>14,.2f}")
    if corpus:
        print(f"  Corpus: {corpus[0]['file_count']} financial files, {corpus[0]['mention_count']} mentions")
    return {"by_recipient": by_recipient, "corpus": corpus}


# ════════════════════════════════════════════════════════════
# NARRATIVE 06 — Gratitude America
# 88% to investments, 7% charitable
# ════════════════════════════════════════════════════════════

def n06_gratitude_america():
    """Gratitude America wire flows from exhibit E + Phase 25."""
    sql = """
        SELECT date, entity_from, entity_to, amount, bates, exhibit
        FROM verified_wires
        WHERE entity_from LIKE '%Gratitude%' OR entity_to LIKE '%Gratitude%'
        ORDER BY date
    """
    rows = run(sql)
    total = sum(r['amount'] for r in rows)
    print(f"N06 — Gratitude America: {len(rows)} wires, ${total:,.2f}")
    for r in rows:
        print(f"  {r['date']}  ${r['amount']:>12,.2f}  {r['entity_from']} → {r['entity_to']}")
    return rows


# ════════════════════════════════════════════════════════════
# NARRATIVE 07 — Follow the Money, Follow the Plane
# Wire-flight temporal correlation analysis
# ════════════════════════════════════════════════════════════

def n07_wire_flight_correlation():
    """Cross-reference dated wires against flight dates for temporal matches."""

    wire_sql = """
        SELECT date, entity_from, entity_to, amount, bates
        FROM verified_wires
        WHERE date IS NOT NULL AND date != ''
        ORDER BY date
    """
    wires = run(wire_sql)

    # flight dates from the entities/files corpus
    # APIS records live in extracted_text for datasets 8, 9, 11
    flight_sql = """
        SELECT DISTINCT d.date_value as flight_date
        FROM dates_found d
        JOIN files f ON d.file_id = f.id
        WHERE f.doc_type = 'flight_log'
          AND d.date_value IS NOT NULL
        ORDER BY d.date_value
    """
    flights = run(flight_sql)
    flight_dates = set(r['flight_date'] for r in flights)

    # match windows: same-day and ±3 days
    from datetime import datetime, timedelta
    same_day = []
    within_3 = []

    for w in wires:
        try:
            wd = datetime.strptime(w['date'], '%Y-%m-%d')
        except (ValueError, TypeError):
            continue

        w_str = w['date']
        if w_str in flight_dates:
            same_day.append(w)

        for delta in range(-3, 4):
            check = (wd + timedelta(days=delta)).strftime('%Y-%m-%d')
            if check in flight_dates:
                within_3.append(w)
                break

    sd_total = sum(r['amount'] for r in same_day)
    w3_total = sum(r['amount'] for r in within_3)
    print(f"N07 — Wire-Flight Correlation:")
    print(f"  Dated wires: {len(wires)}, Flight dates: {len(flight_dates)}")
    print(f"  Same-day matches: {len(same_day)}, ${sd_total:,.2f}")
    print(f"  ±3-day matches: {len(within_3)}, ${w3_total:,.2f}")
    return {"same_day": same_day, "within_3": within_3, "flight_dates": sorted(flight_dates)}


# ════════════════════════════════════════════════════════════
# NARRATIVE 08 — Infrastructure of Access
# Entity co-occurrence across financial / victim / flight docs
# ════════════════════════════════════════════════════════════

TARGET_NAMES_N08 = [
    'Ghislaine Maxwell', 'Darren Indyke', 'Lesley Groff',
    'Sarah Kellen', 'Nadia Marcinkova', 'Richard Kahn',
    'Adriana Ross', 'Jean-Luc Brunel', 'Haley Robson',
    'Igor Zinoviev'
]

def n08_infrastructure_of_access():
    """Per-person file counts across financial, victim, and flight docs."""
    results = []
    for name in TARGET_NAMES_N08:
        sql = """
            SELECT
                SUM(CASE WHEN f.doc_type IN ('financial','spreadsheet','bank_statement')
                         OR f.summary LIKE '%wire%' OR f.summary LIKE '%payment%'
                         OR f.summary LIKE '%account%'
                    THEN 1 ELSE 0 END) as financial_files,
                SUM(CASE WHEN f.doc_type IN ('police_report','court_filing')
                         OR f.summary LIKE '%victim%' OR f.summary LIKE '%abuse%'
                         OR f.summary LIKE '%minor%' OR f.summary LIKE '%trafficking%'
                         OR f.summary LIKE '%Jane Doe%'
                    THEN 1 ELSE 0 END) as victim_files,
                SUM(CASE WHEN f.doc_type = 'flight_log'
                    THEN 1 ELSE 0 END) as flight_files,
                COUNT(DISTINCT e.file_id) as total_files
            FROM entities e
            JOIN files f ON e.file_id = f.id
            WHERE e.entity_type = 'PERSON'
              AND e.entity_text LIKE ?
        """
        row = run(sql, [f"%{name}%"])
        if row:
            r = row[0]
            r['name'] = name
            results.append(r)
            print(f"  {name:25s}  fin={r['financial_files']}  vic={r['victim_files']}  flt={r['flight_files']}  total={r['total_files']}")

    return results


# ════════════════════════════════════════════════════════════
# NARRATIVE 09 — 734,122 Names
# Unsupervised bridging query: who crosses financial + victim
# ════════════════════════════════════════════════════════════

def n09_bridging_names(min_financial=3, min_victim=3):
    """Every PERSON entity bridging financial and victim document categories."""
    sql = """
        SELECT
            e.entity_text as name,
            SUM(CASE WHEN f.doc_type IN ('financial','spreadsheet','bank_statement')
                     OR f.summary LIKE '%wire%' OR f.summary LIKE '%payment%'
                     OR f.summary LIKE '%account%'
                THEN 1 ELSE 0 END) as financial_files,
            SUM(CASE WHEN f.doc_type IN ('police_report','court_filing')
                     OR f.summary LIKE '%victim%' OR f.summary LIKE '%abuse%'
                     OR f.summary LIKE '%minor%' OR f.summary LIKE '%trafficking%'
                     OR f.summary LIKE '%Jane Doe%'
                THEN 1 ELSE 0 END) as victim_files,
            SUM(CASE WHEN f.doc_type = 'flight_log'
                THEN 1 ELSE 0 END) as flight_files,
            COUNT(DISTINCT e.file_id) as total_files
        FROM entities e
        JOIN files f ON e.file_id = f.id
        WHERE e.entity_type = 'PERSON'
        GROUP BY e.entity_text
        HAVING financial_files >= ? AND victim_files >= ?
        ORDER BY financial_files + victim_files DESC
    """
    rows = run(sql, [min_financial, min_victim])
    print(f"N09 — Bridging Names: {len(rows)} persons with >= {min_financial} financial + >= {min_victim} victim files")
    for r in rows[:20]:
        print(f"  {r['name']:35s}  fin={r['financial_files']:>4}  vic={r['victim_files']:>4}  flt={r['flight_files']:>3}")
    return rows


# ════════════════════════════════════════════════════════════
# NARRATIVE 10 — The Round Number Problem
# Benford's Law on verified wire transfer amounts
# ════════════════════════════════════════════════════════════

def _leading_digit(amt):
    if amt <= 0:
        return None
    return int(str(int(amt))[0])

def _first_two(amt):
    if amt <= 0:
        return None
    s = str(int(amt))
    return int(s[:2]) if len(s) >= 2 else int(s[0])

def _is_round(amt):
    """True if amount is round to nearest $1000."""
    return amt % 1000 == 0

def n10_round_number_problem():
    """Benford analysis on dated verified wires (Exhibits A-E)."""
    sql = """
        SELECT amount FROM verified_wires
        WHERE date IS NOT NULL AND date != ''
          AND amount > 0
    """
    rows = run(sql)
    amounts = [r['amount'] for r in rows]

    # leading digit distribution
    digit_counts = defaultdict(int)
    for a in amounts:
        d = _leading_digit(a)
        if d:
            digit_counts[d] += 1

    benford_expected = {
        1: 30.1, 2: 17.6, 3: 12.5, 4: 9.7,
        5: 7.9, 6: 6.7, 7: 5.8, 8: 5.1, 9: 4.6
    }

    total_wires = len(amounts)
    round_count = sum(1 for a in amounts if _is_round(a))
    round_pct = round_count / total_wires * 100 if total_wires else 0
    round_volume = sum(a for a in amounts if _is_round(a))

    # first-two-digit concentration
    ft_counts = defaultdict(int)
    for a in amounts:
        ft = _first_two(a)
        if ft:
            ft_counts[ft] += 1

    top_4 = sorted(ft_counts.items(), key=lambda x: -x[1])[:4]

    print(f"N10 — Round Number Problem: {total_wires} wires")
    print(f"  Round amounts: {round_count} ({round_pct:.1f}%), ${round_volume:,.2f}")
    print(f"\n  Leading digit distribution vs Benford:")
    for d in range(1, 10):
        actual = digit_counts[d] / total_wires * 100 if total_wires else 0
        expected = benford_expected[d]
        print(f"    {d}: {actual:5.1f}% (expected {expected:.1f}%)")
    print(f"\n  Top first-two-digit prefixes:")
    for prefix, cnt in top_4:
        print(f"    {prefix}: {cnt} wires ({cnt/total_wires*100:.1f}%)")

    return {
        "digit_counts": dict(digit_counts),
        "round_pct": round_pct,
        "round_count": round_count,
        "total_wires": total_wires,
        "first_two": dict(ft_counts)
    }


# ════════════════════════════════════════════════════════════
# NARRATIVE 11 — The Shell Map
# 14 shells, 178K money references, 4 shells w/ zero wires
# ════════════════════════════════════════════════════════════

def n11_shell_map():
    """Shell entity corpus footprint — files + money mentions + wire presence."""
    shells_to_check = [
        ('Southern Trust Company', 'Southern Trust'),
        ('Southern Financial LLC', 'Southern Financial'),
        ('The Haze Trust', 'Haze Trust'),
        ('Gratitude America', 'Gratitude America'),
        ('Jeepers Inc', 'Jeepers'),
        ('Plan D LLC', 'Plan D'),
        ('BV70 LLC', 'BV70'),
        ('Financial Trust Company', 'Financial Trust'),
        ('Butterfly Trust', 'Butterfly Trust'),
        ('Outgoing Money Trust', 'Outgoing Money'),
        ('NES LLC', 'NES'),
        ("Villard Int'l Corp", 'Villard'),
        ('PJLP Investments', 'PJLP'),
        ('Zorro Trust', 'Zorro'),
    ]
    results = []
    for full_name, short in shells_to_check:
        # corpus presence
        corpus_sql = """
            SELECT COUNT(DISTINCT e.file_id) as total_files,
                   SUM(CASE WHEN f.doc_type IN ('financial','spreadsheet','bank_statement')
                       THEN 1 ELSE 0 END) as financial_files
            FROM entities e
            JOIN files f ON e.file_id = f.id
            WHERE e.entity_text LIKE ?
        """
        c = run(corpus_sql, [f"%{short}%"])

        # money mentions near this entity
        money_sql = """
            SELECT COUNT(*) as money_mentions
            FROM entities e1
            JOIN entities e2 ON e1.file_id = e2.file_id
            WHERE e1.entity_text LIKE ?
              AND e2.entity_type = 'MONEY'
        """
        m = run(money_sql, [f"%{short}%"])

        # wire count
        wire_sql = """
            SELECT COUNT(*) as wire_count
            FROM verified_wires
            WHERE entity_from LIKE ? OR entity_to LIKE ?
        """
        w = run(wire_sql, [f"%{short}%", f"%{short}%"])

        row = {
            'entity': full_name,
            'total_files': c[0]['total_files'] if c else 0,
            'financial_files': c[0]['financial_files'] if c else 0,
            'money_mentions': m[0]['money_mentions'] if m else 0,
            'wire_count': w[0]['wire_count'] if w else 0
        }
        results.append(row)
        print(f"  {full_name:30s}  files={row['total_files']:>5}  fin={row['financial_files']:>4}  money={row['money_mentions']:>7}  wires={row['wire_count']:>3}")

    return results


# ════════════════════════════════════════════════════════════
# NARRATIVE 12 — The Bank Nobody Prosecuted
# Bear Stearns money mentions vs Deutsche Bank vs JPMorgan
# ════════════════════════════════════════════════════════════

BANKS_N12 = [
    ('Bear Stearns', '%Bear Stearns%'),
    ('JPMorgan/Chase', '%JPMorgan%'),
    ('Deutsche Bank', '%Deutsche Bank%'),
    ('Citibank', '%Citibank%'),
    ('HSBC', '%HSBC%'),
    ('Bank of America', '%Bank of America%'),
    ('Barclays', '%Barclays%'),
]

def n12_bank_nobody_prosecuted():
    """Money-mention volumes by banking institution across the full corpus."""
    results = []
    for label, pattern in BANKS_N12:
        sql = """
            SELECT COUNT(DISTINCT e.file_id) as file_count
            FROM entities e
            JOIN files f ON e.file_id = f.id
            WHERE e.entity_text LIKE ?
              AND f.doc_type IN ('financial','spreadsheet','bank_statement')
        """
        files = run(sql, [pattern])

        money_sql = """
            SELECT COUNT(*) as money_mentions
            FROM entities e1
            JOIN entities e2 ON e1.file_id = e2.file_id
            WHERE e1.entity_text LIKE ?
              AND e2.entity_type = 'MONEY'
        """
        mentions = run(money_sql, [pattern])

        row = {
            'bank': label,
            'financial_files': files[0]['file_count'] if files else 0,
            'money_mentions': mentions[0]['money_mentions'] if mentions else 0
        }
        results.append(row)
        print(f"  {label:20s}  fin_files={row['financial_files']:>5}  money_mentions={row['money_mentions']:>9}")

    return results


# ════════════════════════════════════════════════════════════
# NARRATIVE 13 — Seven Banks, One Trust
# Outgoing Money Trust bank co-occurrence
# ════════════════════════════════════════════════════════════

BANKS_N13 = [
    'Deutsche Bank', 'Wells Fargo', 'Bank of America',
    'TD Bank', 'JPMorgan', 'PNC Bank', 'Citibank'
]

def n13_seven_banks():
    """Outgoing Money Trust: co-occurrence with each banking institution."""
    results = []
    for bank in BANKS_N13:
        sql = """
            SELECT COUNT(DISTINCT e1.file_id) as shared_files
            FROM entities e1
            JOIN entities e2 ON e1.file_id = e2.file_id
            JOIN files f ON e1.file_id = f.id
            WHERE e1.entity_text LIKE '%Outgoing Money Trust%'
              AND e2.entity_text LIKE ?
              AND f.doc_type IN ('financial','spreadsheet','bank_statement')
        """
        r = run(sql, [f"%{bank}%"])
        cnt = r[0]['shared_files'] if r else 0
        results.append({'bank': bank, 'shared_financial_files': cnt})
        print(f"  Outgoing Money Trust × {bank:25s}  shared files: {cnt}")

    return results


# ════════════════════════════════════════════════════════════
# NARRATIVE 14 — Where Leon Black's Money Went
# 1,600 files, $60.5M verified, downstream shell tracing
# ════════════════════════════════════════════════════════════

BLACK_VARIANTS = [
    'Leon Black', 'Leon & Debra Black', 'Black Family Partners',
    'Elysium Management', 'Narrow Holdings', 'BV70'
]

def n14_leon_black():
    """Leon Black file footprint + wire flows + shell co-occurrence."""

    # file counts per variant
    variant_counts = {}
    for v in BLACK_VARIANTS:
        sql = """
            SELECT COUNT(DISTINCT file_id) as files
            FROM entities
            WHERE entity_text LIKE ?
        """
        r = run(sql, [f"%{v}%"])
        cnt = r[0]['files'] if r else 0
        variant_counts[v] = cnt

    # verified wires involving Black entities
    wire_sql = """
        SELECT date, entity_from, entity_to, amount, bates, exhibit
        FROM verified_wires
        WHERE entity_from LIKE '%Black%'
           OR entity_from LIKE '%Narrow%'
           OR entity_from LIKE '%Elysium%'
           OR entity_from LIKE '%BV70%'
           OR entity_to LIKE '%Black%'
        ORDER BY date
    """
    wires = run(wire_sql)
    wire_total = sum(r['amount'] for r in wires)

    # shell co-occurrence
    shells = ['Southern Trust', 'Financial Trust', 'Haze Trust',
              'Southern Financial', 'Gratitude America', 'Butterfly Trust',
              'Outgoing Money Trust']
    shell_overlap = {}
    for s in shells:
        sql = """
            SELECT COUNT(DISTINCT e1.file_id) as shared
            FROM entities e1
            JOIN entities e2 ON e1.file_id = e2.file_id
            WHERE (e1.entity_text LIKE '%Leon Black%'
                   OR e1.entity_text LIKE '%Black Family%'
                   OR e1.entity_text LIKE '%Elysium%')
              AND e2.entity_text LIKE ?
        """
        r = run(sql, [f"%{s}%"])
        shell_overlap[s] = r[0]['shared'] if r else 0

    print(f"N14 — Leon Black:")
    for v, c in variant_counts.items():
        print(f"  {v:30s}  {c} files")
    print(f"  Verified wires: {len(wires)}, ${wire_total:,.2f}")
    for s, c in sorted(shell_overlap.items(), key=lambda x: -x[1]):
        print(f"  × {s:25s}  {c} shared files")

    return {"variant_counts": variant_counts, "wires": wires, "shell_overlap": shell_overlap}


# ════════════════════════════════════════════════════════════
# NARRATIVE 15 — Gratitude America (Expanded)
# Investment fund co-occurrence + Shuliak mentions
# ════════════════════════════════════════════════════════════

FUNDS_N15 = [
    'Apollo', 'Coatue', 'Honeycomb', 'Morgan Stanley',
    'Valar Ventures', 'Boothbay', 'Perry Capital'
]
CHARITABLE_N15 = ['Baleto Teatras', 'Har Zion', 'cancer research']

def n15_gratitude_expanded():
    """Gratitude America: fund co-occurrence, Shuliak mentions, charitable entities."""

    fund_results = []
    for fund in FUNDS_N15:
        sql = """
            SELECT COUNT(DISTINCT e1.file_id) as shared
            FROM entities e1
            JOIN entities e2 ON e1.file_id = e2.file_id
            JOIN files f ON e1.file_id = f.id
            WHERE e1.entity_text LIKE '%Gratitude America%'
              AND e2.entity_text LIKE ?
              AND f.doc_type IN ('financial','spreadsheet','bank_statement','email')
        """
        r = run(sql, [f"%{fund}%"])
        cnt = r[0]['shared'] if r else 0
        fund_results.append({'fund': fund, 'shared_files': cnt})

    # Shuliak
    shuliak_sql = """
        SELECT COUNT(DISTINCT e1.file_id) as shared
        FROM entities e1
        JOIN entities e2 ON e1.file_id = e2.file_id
        WHERE e1.entity_text LIKE '%Gratitude America%'
          AND e2.entity_text LIKE '%Shuliak%'
    """
    shuliak = run(shuliak_sql)
    shuliak_cnt = shuliak[0]['shared'] if shuliak else 0

    print(f"N15 — Gratitude America (Expanded):")
    for f in fund_results:
        print(f"  × {f['fund']:20s}  {f['shared_files']} shared files")
    print(f"  × Shuliak: {shuliak_cnt} shared files")

    return {"funds": fund_results, "shuliak_files": shuliak_cnt}


# ════════════════════════════════════════════════════════════
# NARRATIVE 16 — The Accountant
# HBRK / Richard Kahn doc footprint and email routing
# ════════════════════════════════════════════════════════════

def n16_the_accountant():
    """Richard Kahn / HBRK Associates document footprint and shell reach."""

    # total files
    kahn_sql = """
        SELECT COUNT(DISTINCT file_id) as files
        FROM entities
        WHERE entity_text LIKE '%Richard Kahn%' OR entity_text LIKE '%HBRK%'
    """
    kahn = run(kahn_sql)

    # by doc type
    doctype_sql = """
        SELECT f.doc_type, COUNT(DISTINCT e.file_id) as files
        FROM entities e
        JOIN files f ON e.file_id = f.id
        WHERE e.entity_text LIKE '%Richard Kahn%' OR e.entity_text LIKE '%HBRK%'
        GROUP BY f.doc_type
        ORDER BY files DESC
    """
    by_type = run(doctype_sql)

    # shell co-occurrence via email
    shells = ['Southern Trust', 'Southern Financial', 'Financial Trust',
              'Haze Trust', 'Gratitude America', 'Butterfly Trust',
              'Outgoing Money Trust', 'Jeepers']
    shell_emails = {}
    for s in shells:
        sql = """
            SELECT COUNT(DISTINCT e1.file_id) as shared
            FROM entities e1
            JOIN entities e2 ON e1.file_id = e2.file_id
            JOIN files f ON e1.file_id = f.id
            WHERE (e1.entity_text LIKE '%Kahn%' OR e1.entity_text LIKE '%HBRK%')
              AND e2.entity_text LIKE ?
              AND f.doc_type = 'email'
        """
        r = run(sql, [f"%{s}%"])
        shell_emails[s] = r[0]['shared'] if r else 0

    total_files = kahn[0]['files'] if kahn else 0
    print(f"N16 — The Accountant: {total_files} total files")
    for t in by_type:
        print(f"  {t['doc_type']:20s}  {t['files']} files")
    print(f"\n  Shell email co-occurrence:")
    for s, c in sorted(shell_emails.items(), key=lambda x: -x[1]):
        print(f"    × {s:25s}  {c} shared emails")

    return {"total_files": total_files, "by_type": by_type, "shell_emails": shell_emails}


# ════════════════════════════════════════════════════════════
# NARRATIVE 17 — One-Way Money (The Architecture)
# Balance sheet across 9 shell entities, 5 exhibits
# ════════════════════════════════════════════════════════════

def n17_one_way_money():
    """Full balance sheet: inflows vs outflows across all verified wires."""

    # per-exhibit breakdown
    exhibit_sql = """
        SELECT exhibit, COUNT(*) as wires, SUM(amount) as total
        FROM verified_wires
        GROUP BY exhibit
        ORDER BY exhibit
    """
    by_exhibit = run(exhibit_sql)

    # per-entity balance sheet
    entity_sql = """
        SELECT entity, SUM(inflow) as total_in, SUM(outflow) as total_out,
               SUM(inflow) - SUM(outflow) as net
        FROM (
            SELECT entity_to as entity, amount as inflow, 0 as outflow
            FROM verified_wires
            UNION ALL
            SELECT entity_from as entity, 0 as inflow, amount as outflow
            FROM verified_wires
        )
        GROUP BY entity
        HAVING total_in > 0 OR total_out > 0
        ORDER BY total_in + total_out DESC
        LIMIT 20
    """
    balance = run(entity_sql)

    # the big numbers
    all_sql = """
        SELECT COUNT(*) as total_wires,
               SUM(amount) as grand_total,
               COUNT(DISTINCT entity_from) + COUNT(DISTINCT entity_to) as entity_count
        FROM verified_wires
    """
    totals = run(all_sql)

    print(f"N17 — One-Way Money:")
    if totals:
        t = totals[0]
        print(f"  {t['total_wires']} wires, ${t['grand_total']:,.2f}, ~{t['entity_count']} entities")
    print(f"\n  By exhibit:")
    for e in by_exhibit:
        print(f"    {e['exhibit']:5s}  {e['wires']:>3} wires  ${e['total']:>14,.2f}")
    print(f"\n  Top entity balances:")
    for b in balance[:15]:
        print(f"    {b['entity']:40s}  in=${b['total_in']:>12,.0f}  out=${b['total_out']:>12,.0f}  net=${b['net']:>+12,.0f}")

    return {"by_exhibit": by_exhibit, "balance": balance, "totals": totals}


# ════════════════════════════════════════════════════════════
# NARRATIVE 18 — Offshore Architecture (Brunel–BVI–ICIJ)
# DOJ corpus × ICIJ Offshore Leaks cross-reference
# ════════════════════════════════════════════════════════════

def n18_offshore_architecture():
    """Cross-reference EFTA entities against ICIJ Offshore Leaks."""

    # Brunel presence in EFTA corpus
    brunel_sql = """
        SELECT COUNT(DISTINCT file_id) as files
        FROM entities
        WHERE entity_text LIKE '%Brunel%'
    """
    brunel = run(brunel_sql)

    # ICIJ entities matching known Epstein/Brunel names
    targets = ['Scouting International', 'Butterfly Trust', 'MC2',
               'Brunel', 'Southern Trust', 'Financial Trust']
    icij_hits = []
    for t in targets:
        sql = """
            SELECT name, jurisdiction, company_type
            FROM icij_entities
            WHERE name LIKE ?
            LIMIT 10
        """
        rows = run(sql, [f"%{t}%"])
        for r in rows:
            r['search_term'] = t
            icij_hits.append(r)

    # NetIncorp exhaustive check (689 entities)
    netincorp_sql = """
        SELECT COUNT(*) as total FROM icij_entities
        WHERE name LIKE '%NetIncorp%'
    """
    netincorp = run(netincorp_sql)

    # Brunel fund flows from publication ledger
    brunel_flows_sql = """
        SELECT entity_from, entity_to, amount, date_ref, confidence
        FROM fund_flows
        WHERE entity_from LIKE '%Brunel%' OR entity_to LIKE '%Brunel%'
           OR entity_from LIKE '%MC2%' OR entity_to LIKE '%MC2%'
        ORDER BY amount DESC
        LIMIT 25
    """
    flows = run(brunel_flows_sql)

    print(f"N18 — Offshore Architecture:")
    print(f"  Brunel in EFTA corpus: {brunel[0]['files'] if brunel else 0} files")
    print(f"  ICIJ matches: {len(icij_hits)}")
    for h in icij_hits:
        print(f"    [{h['search_term']}] → {h['name']}, {h.get('jurisdiction','?')}, {h.get('company_type','?')}")
    if flows:
        print(f"  Brunel/MC2 fund flows: {len(flows)}")
        for fl in flows[:10]:
            print(f"    {fl['entity_from']} → {fl['entity_to']}  ${fl['amount']:>12,.2f}  conf={fl['confidence']}")

    return {"brunel_files": brunel, "icij_hits": icij_hits, "flows": flows}


# ════════════════════════════════════════════════════════════
# PUBLICATION LEDGER — Phase 5L master totals
# ════════════════════════════════════════════════════════════

def pub_ledger_summary():
    """Phase 5L publication ledger: four-tier totals."""
    sql = """
        SELECT confidence_tier,
               COUNT(*) as txn_count,
               SUM(amount) as total
        FROM publication_ledger
        GROUP BY confidence_tier
        ORDER BY confidence_tier
    """
    rows = run(sql)
    grand = sum(r['total'] for r in rows)
    grand_ct = sum(r['txn_count'] for r in rows)
    print(f"Publication Ledger — Phase 5L: {grand_ct} unique txns, ${grand:,.2f}")
    for r in rows:
        print(f"  {r['confidence_tier']:30s}  {r['txn_count']:>5} txns  ${r['total']:>16,.2f}")
    return rows


def pub_ledger_by_payment_type():
    """Publication ledger broken down by payment type."""
    sql = """
        SELECT payment_type,
               COUNT(*) as txn_count,
               SUM(amount) as total
        FROM publication_ledger
        GROUP BY payment_type
        ORDER BY total DESC
    """
    rows = run(sql)
    print(f"Publication Ledger by Payment Type:")
    for r in rows:
        print(f"  {r['payment_type']:30s}  {r['txn_count']:>5} txns  ${r['total']:>16,.2f}")
    return rows


# ════════════════════════════════════════════════════════════
# QUICK RUNNER
# ════════════════════════════════════════════════════════════

ALL_NARRATIVES = {
    '01': ('The Jeepers Pipeline', n01_jeepers_pipeline),
    '02': ('Art Market as Liquidity Channel', n02_art_market),
    '03': ('The Plan D Question', n03_plan_d),
    '04': ('Chain-Hop Anatomy', n04_chain_hop),
    '05': ("Deutsche Bank's Role", n05_deutsche_bank),
    '06': ('Gratitude America', n06_gratitude_america),
    '07': ('Follow the Money, Follow the Plane', n07_wire_flight_correlation),
    '08': ('Infrastructure of Access', n08_infrastructure_of_access),
    '09': ('734,122 Names', n09_bridging_names),
    '10': ('The Round Number Problem', n10_round_number_problem),
    '11': ('The Shell Map', n11_shell_map),
    '12': ('The Bank Nobody Prosecuted', n12_bank_nobody_prosecuted),
    '13': ('Seven Banks, One Trust', n13_seven_banks),
    '14': ("Where Leon Black's Money Went", n14_leon_black),
    '15': ('Gratitude America (Expanded)', n15_gratitude_expanded),
    '16': ('The Accountant', n16_the_accountant),
    '17': ('One-Way Money', n17_one_way_money),
    '18': ('Offshore Architecture', n18_offshore_architecture),
    'PL': ('Publication Ledger Summary', pub_ledger_summary),
    'PT': ('Publication Ledger by Payment Type', pub_ledger_by_payment_type),
}


def run_all():
    """Run every narrative query and print results."""
    for key in sorted(ALL_NARRATIVES.keys()):
        title, func = ALL_NARRATIVES[key]
        print(f"\n{'='*70}")
        print(f"  {key} — {title}")
        print(f"{'='*70}")
        try:
            func()
        except Exception as ex:
            print(f"  ERROR: {ex}")


def run_one(key):
    """Run a single narrative query by key (e.g. '01', '17', 'PL')."""
    if key not in ALL_NARRATIVES:
        print(f"Unknown key '{key}'. Available: {', '.join(sorted(ALL_NARRATIVES.keys()))}")
        return
    title, func = ALL_NARRATIVES[key]
    print(f"\n{'='*70}")
    print(f"  {key} — {title}")
    print(f"{'='*70}")
    return func()


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            run_one(arg.zfill(2) if arg.isdigit() else arg)
    else:
        print("Usage: python narrative_sql_tools.py [01|02|...|18|PL|PT|all]")
        print(f"Available: {', '.join(sorted(ALL_NARRATIVES.keys()))}")
        print("  python narrative_sql_tools.py 07      # single narrative")
        print("  python narrative_sql_tools.py 01 05 17 # multiple")
        print("  python narrative_sql_tools.py all      # everything")
