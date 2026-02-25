"""
classify_bank_stmt_txns.py
Classifies the 1,093 'unknown' tx_type rows in bank_statement_transactions
where record_type = 'TRANSACTION', and flags garbage rows for demotion.

Run from: ~/Desktop/epstein-forensic-finance/
DB: ~/Desktop/epstein_files.db

Two passes:
  1. CLASSIFY — pattern-match descriptions to assign tx_type
  2. GARBAGE — flag rows that aren't real transactions

DRY RUN by default. Set COMMIT = True to write changes.
"""

import sqlite3
import re

DB = "/Users/randall/Desktop/epstein_files.db"
COMMIT = True  # flip to True after reviewing dry run output

conn = sqlite3.connect(DB)
cur = conn.cursor()

# ── Pull the unknown TRANSACTION rows ──
cur.execute("""
    SELECT id, bank, description, tx_amount, bates
    FROM bank_statement_transactions
    WHERE record_type = 'TRANSACTION' AND tx_type = 'unknown'
""")
rows = cur.fetchall()
print(f"Total unknown TRANSACTION rows: {len(rows)}\n")

# ── PASS 1: Classification rules ──
# Order matters — first match wins

classify_rules = [
    # ── Debits / recurring payments ──
    # Clean patterns
    (r'(?i)preauthori[zs]ed\s*debit', 'debit'),
    # OCR-mangled variants: Preauthoneed, Preatithoriied, Prcauthonzed,
    # Prea1101011/ed, Reauthorized, Pr.:authorized
    (r'(?i)pr[e.]?a[u1][t.]?h[o0][nr][ei1][zs]?e?d?\s*d[ei]b[il]t', 'debit'),
    (r'(?i)re?a[u.]?thori[zs]ed\s*d[ei]b[il]t', 'debit'),
    (r'(?i)pr[.:]+authorized\s*d[ei]b[il]t', 'debit'),
    # Broad catch: any line with date + "Debit" or "Dehit" or "lehit"
    (r'(?i)\d{2}[-.]?\d{2}\s+.*[dl][ei][bhk][il]t', 'debit'),
    # "Preauthorized" alone (without Debit, truncated OCR)
    (r'(?i)\d{2}[-.]?\d{2}\s+Preauthori', 'debit'),
    
    # ── Transfers ──
    (r'(?i)outgoing\s*(money|in|fa|mona)\s*(transfer|trnst|trost|tr[nmk]sf|int[.:s]|tramfer|treti)', 'transfer'),
    (r'(?i)outgoing\s*(f[as]|money|mona)', 'transfer'),
    # OCR: "<hawing Money Treti'" = Drawing Money Transfer
    (r'(?i)[<d]hawing\s*money', 'transfer'),
    # "Outgoing Fs Tramfer", "Outgoing Vonev TrMI"
    (r'(?i)outgoing\s+\w{1,5}\s+(tr[a-z]+f|tr[a-z]+i)', 'transfer'),
    (r'(?i)incoming\s*(money|wire|transfer|tmsf|tmsl)', 'deposit'),
    # "Incoming Money Tmsf" / "Incoming Money Tmsl"
    (r'(?i)\d{2}[-.]?\d{2}\s+incoming', 'deposit'),
    
    # ── Checks ──
    (r'(?i)che[ec]ks?\s*paid', 'check'),
    (r'(?i)check[s.]?\s*paid', 'check'),
    
    # ── Cash management transfers (Deutsche Bank) ──
    (r'(?i)cash\s*m[gni][gm]?[nit]*\s*(tr[sa]n?sf?r?|tar)\s*(cr|db)', 'transfer'),
    (r'(?i)cash\s*migni\s*tr', 'transfer'),  # "Cash Migni Trsfr"
    (r'(?i)trar+ifer\s*(of\s*)?funds', 'transfer'),  # "k Trarrifer Of Funds"
    
    # ── Checks — OCR variants ──
    (r'(?i)che[ecv][iok][eo.]?t?\s*paid', 'check'),  # Cheviot = Check
    
    # ── ATM / Debit card withdrawals ──
    # "VIM and Dshit Card WithtIrmals" = ATM and Debit Card Withdrawals
    (r'(?i)(atm|vim)\s*(and|&)\s*(debit|dshit|d[ei]b)\s*card\s*with', 'withdrawal'),
    (r'(?i)cash/atm\s*transaction', 'withdrawal'),
    (r'(?i)cash\s*withdr[wv]', 'withdrawal'),  # "Cash Withdrwals"
    (r'(?i)withdrawal', 'withdrawal'),
    
    # ── Fees ──
    (r'(?i)atm\s*fee\s*rebate', 'fee_rebate'),
    (r'(?i)service\s*charge', 'fee'),
    (r'(?i)monthly\s*service\s*fee', 'fee'),
    (r'(?i)transaction\s*fee', 'fee'),
    (r'(?i)\bfees?\b', 'fee'),
    
    # ── Interest ──
    (r'(?i)\binterest\b', 'interest'),
    (r'(?i)\binter[oe]i?t\b', 'interest'),  # OCR mangling
    
    # ── Deposits ──
    # "Mposits and Other Credit" / "INixIsits and (Mier Crod its"
    (r'(?i)(de?posit|mposit|[il1]n[ilx]+[ils]it)[s.]?\s*(and|&)\s*(other|mier)\s*(credit|crod)', 'deposit'),
    (r'(?i)deposit[s.]?\s*(and|&)\s*other\s*credit', 'deposit'),
    (r'(?i)\bdeposit[s]?\b', 'deposit'),
    # OCR: just "Deposits" mangled
    (r'(?i)mposit[s]?\s*(and|&)', 'deposit'),
    # Broader catch: anything ending with "Crod its" or "Credits" or "Cretins" or "Crixlit"
    (r'(?i)(and|&)\s*(other|mier|niter|alter)\s*(credit|crod|cretin|crixl)', 'deposit'),
    # Ultra-mangled: "l)esxlsits" "lkixIsits" "lkposits" "IN:posits" "INixIsits" = Deposits
    (r'(?i)[li1IN][)\]:k]?[eoxnN][sp][xioI][ils][sit]+[s]?\s*(and|&)', 'deposit'),
    (r'(?i)IN[.:x]?posit[s]?\s*(and|&)', 'deposit'),
    # Specific OCR mangle patterns seen in data
    (r'(?i)INix[Il]sit[s]?\s*(and|&)', 'deposit'),  # "INixIsits and"
    (r'(?i)lkix[Il]sit[s]?\s*(and|&)', 'deposit'),  # "lkixIsits and"
    
    # ── Named payees (Epstein-specific) ──
    (r'(?i)visoski', 'payment_to_person'),
    (r'(?i)designs?\s*llc', 'payment_to_entity'),
    (r'(?i)south\s*street\s*capital', 'payment_to_entity'),
    
    # ── Spending categories (UBS breakdowns) ──
    # "Restaisants" = Restaurants
    (r'(?i)rest(aurant|aisant|amant)[s]?', 'purchase'),
    (r'(?i)professional\s*service', 'purchase'),
    # "Miscellaneous" / "MiwelaneoustUrxlassitied" / "MiscelaneousrUnclassified"
    (r'(?i)mi[sw][ce][el]an[eo]', 'purchase'),
    (r'(?i)unclassified', 'purchase'),
    (r'(?i)purchase[s]?', 'purchase'),
    
    # ── Past due / loan payments ──
    (r'(?i)past\s*due', 'loan_payment'),
    (r'(?i)navy\s*federal\s*credit', 'loan_payment'),
    
    # ── Distributions ──
    (r'(?i)distribution', 'distribution'),
    
    # ── Savings transfers ──
    (r'(?i)savings', 'savings_transfer'),
    
    # ── UBS spending categories ──
    (r'(?i)transportation', 'purchase'),
    (r'(?i)travel\s*arrangement', 'purchase'),
    (r'(?i)card\s*purchase', 'purchase'),
    (r'(?i)merchandise', 'purchase'),
    (r'(?i)grocery|grocer', 'purchase'),
    (r'(?i)utility|utilities', 'purchase'),
    (r'(?i)insurance', 'purchase'),
    (r'(?i)medical|healthcare|pharmacy', 'purchase'),
    (r'(?i)education', 'purchase'),
    (r'(?i)entertainment', 'purchase'),
    (r'(?i)clothing|apparel', 'purchase'),
    (r'(?i)communication[s]?', 'purchase'),
    (r'(?i)home\s*(improvement|furnish)', 'purchase'),
    (r'(?i)personal\s*care', 'purchase'),
    (r'(?i)employment\s*related', 'purchase'),
    (r'(?i)government\s*service', 'purchase'),
    (r'(?i)membership', 'purchase'),
    
    # ── Tax ──
    (r'(?i)taxes?\s*withheld', 'tax'),
    (r'(?i)tax\s*payment', 'tax'),
    
    # ── Securities / investment ──
    (r'(?i)proceeds\s*from\s*investment', 'securities'),
    (r'(?i)funds?\s*debited', 'withdrawal'),
    
    # ── Deutsche Bank date-prefixed transactions with no other keyword ──
    # "( ) 01-17 l/cis6it" or similar truncated OCR with date
    # If it has a date pattern and made it to TRANSACTION, it's probably real
    (r'(?i)^\(?[\s.]*\)?\s*\d{2}[-.]?\d{2}\s+', 'debit'),
    # Also catch "(.00 ) MM-DD Outgoing" where backslash breaks earlier patterns
    (r'(?i)\d{2}[-.]?\d{2}\s+outgoing', 'transfer'),
]

# ── PASS 2: Garbage detection rules ──
# These are NOT transactions — demote to appropriate record_type

garbage_rules = [
    # Bank boilerplate
    (r'(?i)all\s*items\s*(are|am)\s*credited\s*subject\s*to', 'NOISE:LEGAL_BOILERPLATE'),
    (r'(?i)unconditional\s*(credit|stain)\s*to\s*and\s*accepted', 'NOISE:LEGAL_BOILERPLATE'),
    (r'(?i)final\s*collection\s*and\s*receipt', 'NOISE:LEGAL_BOILERPLATE'),
    (r'(?i)items\s*(are|am|an)\s*credited\s*(subject|lathier)', 'NOISE:LEGAL_BOILERPLATE'),
    (r'(?i)this\s*[mr][iu]lt\s*card\s*is\s*(termed|issued)', 'NOISE:LEGAL_BOILERPLATE'),
    
    # Page/statement headers
    (r'(?i)your\s*(financial|si\s*sandal)\s*advisor', 'NOISE:PAGE_HEADER'),
    (r'(?i)account\s*name:', 'NOISE:PAGE_HEADER'),
    (r'(?i)page\s*\d+\s*of', 'NOISE:PAGE_HEADER'),
    (r'(?i)CONFIDENTIAL', 'NOISE:PAGE_HEADER'),
    
    # Column headers / labels
    (r'(?i)^\.?00\s*date\s*description$', 'NOISE:COLUMN_HEADER'),
    (r'(?i)^date\s*(acquired|description)', 'NOISE:COLUMN_HEADER'),
    (r'(?i)^amount\s*(sent|service)', 'NOISE:COLUMN_HEADER'),
    (r'(?i)^\d[\d,.]+\s*date$', 'NOISE:COLUMN_HEADER'),  # "14,035,453.40 Date"
    
    # Tax/reporting boilerplate  
    (r'(?i)for\s*purposes\s*of\s*this\s*statement.*tax', 'NOISE:TAX_BOILERPLATE'),
    (r'(?i)taxab[il]e\s*interest', 'NOISE:TAX_BOILERPLATE'),
    (r'(?i)tax\s*reporting\s*persp', 'NOISE:TAX_BOILERPLATE'),
    
    # Summary/total lines that slipped through
    (r'(?i)year[\s-]*to[\s-]*date', 'WM_SUMMARY'),
    (r'(?i)yea\s*iodate', 'WM_SUMMARY'),  # OCR of "Year to date"
    (r'(?i)subtraction[s]?$', 'WM_SUMMARY'),
    (r'(?i)^[\d.,\s$]*addition[s]?$', 'WM_SUMMARY'),
    (r'(?i)accrued\s*interest\s*\$', 'BALANCE'),
    
    # SDNY reference lines
    (r'(?i)S[OD]NY\s*GM\s*\d+', 'NOISE:SDNY_REF'),
    
    # OCR garbage with no real description
    (r'^[\(\)\s\.\d,\-\$]+$', 'OCR_NOISE'),  # only numbers and punctuation
    # Lines that are just big numbers strung together
    (r'^\s*\.?\d{2}\s+[\d,.]+\s+[\d,.]+\s*\(?\s*[S\$]?[\d,.]+\s*\)?\s*$', 'OCR_NOISE'),
    
    # Bank marketing / product info
    (r'(?i)bank\s*loans?\s*utilized', 'NOISE:BANK_MARKETING'),
    (r'(?i)credit\s*card\s*(is\s*termed|feature|will\s*aut)', 'NOISE:BANK_MARKETING'),
    (r'(?i)selection\s*of\s*rewards', 'NOISE:BANK_MARKETING'),
    (r'(?i)cash\s*limit\s*\$?[\d,]+', 'NOISE:BANK_MARKETING'),
    
    # Column headers with "Date" / "Dale" (OCR)
    (r'(?i)^[S\$]?[\d,O.]+\s*[I1l]?\s*(date|dale)\s*$', 'NOISE:COLUMN_HEADER'),
    (r'(?i)^date\s*(a[ec]t[hi]vity|acquired|description)', 'NOISE:COLUMN_HEADER'),
    (r'(?i)^dale\s*act[io]', 'NOISE:COLUMN_HEADER'),  # "Dale Actitify"
    (r'(?i)date\s*ae[tc]h[io]', 'NOISE:COLUMN_HEADER'),  # "Date Aethity"
    (r'(?i)other\s*funds?\s*debited\s*date', 'NOISE:COLUMN_HEADER'),
    (r'(?i)\d[\d,.]+\s*amount\s*$', 'NOISE:COLUMN_HEADER'),  # "572.921.98 Amount"
    
    # Statement metadata
    (r'(?i)enclosure[s]?\s*trans[ai][cw]tion\s*detail', 'NOISE:STMT_METADATA'),
    (r'(?i)[ef][.,]?nclosure[s]?\s*trans', 'NOISE:STMT_METADATA'),  # "F.nclosures"
    (r'(?i)avail?a[bt][il]e?\s*credit\s*(line|tine)', 'NOISE:STMT_METADATA'),
    (r'(?i)avalat[il]e?\s*credit\s*tine', 'NOISE:STMT_METADATA'),  # "Avalatie Credit tine"
    (r'(?i)avai[at]te?\s*credit\s*line', 'NOISE:STMT_METADATA'),  # "Avaiatte Credit Line"
    (r'(?i)outstanding\s*b[ad]l?ance', 'NOISE:STMT_METADATA'),
    (r'(?i)transaction\s*detail', 'NOISE:STMT_METADATA'),  # "Transaction Detail"
    (r'(?i)account\s*instruction', 'NOISE:STMT_METADATA'),
    (r'(?i)redemption[s]?\s*(are|we)\s*not\s*permitted', 'NOISE:BANK_MARKETING'),
    (r'(?i)credit\s*c[ao]d\s*is\s*enrolled', 'NOISE:BANK_MARKETING'),
    (r'(?i)underlying\s*security\s*price', 'BALANCE'),
    (r'(?i)EPST[BE]I?N\s*VIRGIN\s*ISLANDS', 'NOISE:PAGE_HEADER'),
    
    # Name-only lines (account holder, not transactions)
    (r'(?i)^GHISLAINE\s*MAXWELL$', 'NOISE:PAGE_HEADER'),
    
    # Unrealized gains/losses
    (r'(?i)[il1]a[il]real[il]zed', 'BALANCE'),  # "lAirealized"
    (r'(?i)unreali[zs]ed', 'BALANCE'),
    (r'(?i)untested\s*w[ei]', 'BALANCE'),  # "Untested Wenn" = unrealized?
    
    # More column headers
    (r'(?i)date\s*de\s*c[rt]ip[lt]i[ao]n', 'NOISE:COLUMN_HEADER'),  # "Date De ctiplion"
    (r'(?i)^[\d.,\s$SO]+amount\s*[\d$SO]*$', 'NOISE:COLUMN_HEADER'),  # "Amount S36.63"
    
    # Date range headers (UBS period lines)
    (r'(?i)^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s*\d{1,2}\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|to)\s*\d{1,2}', 'PATTERN:STATEMENT_PERIOD'),
    (r'(?i)^\d{1,4}\.\d{2}\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s*\d{1,2}', 'PATTERN:STATEMENT_PERIOD'),
    
    # UBS member/reference strings
    (r'(?i)member\s*svc\s*cnp', 'NOISE:PAGE_HEADER'),
    (r'(?i)^CNP\d', 'NOISE:PAGE_HEADER'),
    
    # Lines that are just dollar amounts with no description
    (r'^[S\$\s.,\d\(\)\-]+$', 'OCR_NOISE'),
]

# Additional: flag $1.00 rows where description starts with ( and contains
# a larger number — these are parser artifacts
def is_parser_artifact(desc, amount):
    if abs(amount - 1.0) < 0.01 and desc and desc.startswith('('):
        # check if description contains what looks like a real dollar amount
        nums = re.findall(r'[\d,]+\.\d{2}', desc)
        if nums:
            return True
    return False


# ── Run both passes ──
classified = {}
garbage = {}
artifacts = []
untouched = []

for row_id, bank, desc, amount, bates in rows:
    if not desc:
        desc = ''
    
    # Check parser artifacts first
    if is_parser_artifact(desc, amount):
        artifacts.append((row_id, bank, desc, amount, bates))
        continue
    
    # Check garbage
    hit_garbage = False
    for pattern, new_record_type in garbage_rules:
        if re.search(pattern, desc):
            garbage.setdefault(new_record_type, []).append(
                (row_id, bank, desc, amount, bates)
            )
            hit_garbage = True
            break
    if hit_garbage:
        continue
    
    # Check classification
    hit_class = False
    for pattern, new_type in classify_rules:
        if re.search(pattern, desc):
            classified.setdefault(new_type, []).append(
                (row_id, bank, desc, amount, bates)
            )
            hit_class = True
            break
    
    if not hit_class:
        untouched.append((row_id, bank, desc, amount, bates))


# ── Report ──
print("=" * 70)
print("CLASSIFICATION RESULTS")
print("=" * 70)

total_classified = 0
for tx_type, items in sorted(classified.items(), key=lambda x: -len(x[1])):
    total_amt = sum(abs(r[3]) for r in items)
    print(f"\n  {tx_type}: {len(items)} rows, ${total_amt:,.2f}")
    for r in items[:3]:
        print(f"    [{r[1]}] {r[2][:70]}  ${r[3]:,.2f}")
    if len(items) > 3:
        print(f"    ... and {len(items)-3} more")
    total_classified += len(items)

print(f"\nTotal classifiable: {total_classified}")

print("\n" + "=" * 70)
print("GARBAGE / DEMOTIONS")
print("=" * 70)

total_garbage = 0
for rec_type, items in sorted(garbage.items(), key=lambda x: -len(x[1])):
    total_amt = sum(abs(r[3]) for r in items)
    print(f"\n  → {rec_type}: {len(items)} rows, ${total_amt:,.2f}")
    for r in items[:3]:
        print(f"    [{r[1]}] {r[2][:70]}  ${r[3]:,.2f}")
    total_garbage += len(items)

print(f"\nParser artifacts ($1.00 misreads): {len(artifacts)}")
for r in artifacts[:5]:
    print(f"  [{r[1]}] {r[2][:70]}  ${r[3]:,.2f}")
if len(artifacts) > 5:
    print(f"  ... and {len(artifacts)-5} more")

total_garbage += len(artifacts)
print(f"\nTotal garbage/artifacts: {total_garbage}")

print("\n" + "=" * 70)
print("STILL UNCLASSIFIED")
print("=" * 70)
print(f"Remaining: {len(untouched)} rows")
total_unt = sum(abs(r[3]) for r in untouched)
print(f"Amount: ${total_unt:,.2f}")
for r in sorted(untouched, key=lambda x: -abs(x[3]))[:20]:
    print(f"  [{r[1]}] {r[2][:70]}  ${r[3]:,.2f}")

print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)
print(f"  Classified:    {total_classified}")
print(f"  Garbage:       {total_garbage}")
print(f"  Untouched:     {len(untouched)}")
print(f"  Total:         {total_classified + total_garbage + len(untouched)} / {len(rows)}")

# ── Apply changes if COMMIT ──
if COMMIT:
    print("\n*** COMMITTING CHANGES ***")
    
    update_type = 0
    for tx_type, items in classified.items():
        for row_id, *_ in items:
            cur.execute(
                "UPDATE bank_statement_transactions SET tx_type = ? WHERE id = ?",
                (tx_type, row_id)
            )
            update_type += 1
    
    demote_count = 0
    for rec_type, items in garbage.items():
        for row_id, *_ in items:
            cur.execute(
                "UPDATE bank_statement_transactions SET record_type = ? WHERE id = ?",
                (rec_type, row_id)
            )
            demote_count += 1
    
    for row_id, *_ in artifacts:
        cur.execute(
            "UPDATE bank_statement_transactions SET record_type = 'PARSER_ARTIFACT' WHERE id = ?",
            (row_id,)
        )
        demote_count += 1
    
    conn.commit()
    print(f"  Updated tx_type: {update_type} rows")
    print(f"  Demoted record_type: {demote_count} rows")
else:
    print("\n*** DRY RUN — set COMMIT = True to apply ***")

conn.close()
