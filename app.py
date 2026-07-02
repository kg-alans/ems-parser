from flask import Flask, request, jsonify, send_from_directory
from datetime import datetime
import tempfile
import os
import base64
import json
import re
import xml.etree.ElementTree as ET
from dbfread import DBF

app = Flask(__name__)
# ─── Display Board data store ─────────────────────────────────────
_board_data = []
_estimator_data = []
_SYNC_FILE = os.path.join(os.path.dirname(__file__), 'last_sync.txt')

# ─── Phase 3 mapping tables ───────────────────────────────────────

# Body technician → DTBS Tech choice value.
#
# Architecture note (Batch 4, June 29 2026): this lookup, along with PAINTER_MAPPING,
# TECH_PLACEHOLDERS, and GLASS_TECHS, is hardcoded in app.py rather than living in a
# SharePoint list. Trade-off considered: an SP-list version would let HR/personnel
# changes ship without a git deploy. Costs: extra round-trip per sync, new failure
# mode (SP list unreachable → all techs fall through to blank), one more list to
# maintain. For a stable team that changes a couple times a year, the deploy cost
# is small and a hardcoded list is more reliable. Revisit if turnover frequency
# rises or if the SP-list pattern proves itself for similar lookups.
TECH_MAPPING = {
    'Dmitriy Runov':   'Dmitriy',
    'Ludek Srajer':    'Ludek',
    'Alex Demchenko':  'Demchenko',
    'Jason Moffitt':   'Jason',
    'Uriah Scalf':     'Uriah',
    'Kyle Parks':      'Kyle',
    'Carlos Orozco':   'Carlos',
    'Nic Moffitt':     'Nic',
    'Tyler Evans':     'Tyler E',
    # Jesus Zavala intentionally excluded — placeholder, never write
}

# Names CCC writes into tech fields that are NOT real technicians.
# Stripped at parse time so downstream selection logic and any future
# tech-related reporting never sees them. (Batch 4, June 29 2026.)
#  - Jesus Zavala: shop polisher; time for scans/polishing is flagged to
#    him on many files, so he appears constantly in mechanical (47x) and
#    occasionally in body (2x). Never a real tech.
#  - David / Mike Ford: paint preppers; usually under paint but can land
#    in body — always ignore as techs.
TECH_PLACEHOLDERS = {'Jesus Zavala', 'David', 'Mike Ford'}

# Glass technicians. CCC has no glass-tech field, so glass techs appear in
# body OR mechanical depending on the file. Used by the tech-selection
# priority logic to recognize them wherever they land. Members must also
# be present in TECH_MAPPING above for the name translation to succeed.
GLASS_TECHS = {'Tyler Evans', 'Nic Moffitt'}

# Paint technician → DTBS Painter choice value
PAINTER_MAPPING = {
    'Doug Curtis':     'Doug',
    'Wayne Decker':    'Wayne',
    'Rick Hopkins':    'Rick',
    'Admir Huskic':    'Admir',
}

# Phase names from CCC's `repair_phase_name` that mean "production is finished
# with this car" — used by Production Sync to drive Done=True (Batch 4,
# June 29 2026). Critical: a Done phase means production-complete, NOT
# delivered. The car can sit for weeks post-Done while the estimator awaits
# pickup or fights insurance. Closed (delivery / file closed in CCC) is a
# separate milestone owned by Cleanup Sync.
#
# Why each pair: the production team is migrating phase names. Old format
# uses a space after the colon (`6: Done, To Estimator`); new format omits
# it (`6:Done, To Estimator`). Per Alan's note: once a phase is on a file,
# updating the backend phase name does NOT retroactively change the file's
# stored phase, so live data permanently carries both formats during/after
# migration. Include BOTH spellings for every phase to survive the crossover.
#
# Phase semantics (verified June 29 2026 against the live Production Schedule
# XML + the team's current phases CSV):
#   - `6: Done, To Estimator` / `6:Done, To Estimator`: in-progress handoff
#   - `6: Waiting on Insurance for Delivery` / no-space variant: production
#     finished, waiting for insurance approval to release
#   - `6:Repairs Complete, Customer Notified` (new phase, no legacy form):
#     production-complete, customer notified — confirmed Done by Alan
#   - `[Completed]`: universal key-to-estimator milestone (47 such rows in
#     the June 19 reconciliation export carried "done key to Cord" comments)
#   - `9:TL Car Has Released` / space variant: total loss released — production
#     will never work it again. (NOT included: `9:TL Needs Release` — release
#     hasn't happened yet, decision still pending; `9:Possible Total Loss` —
#     might still be repaired.)
#
# A phase that was in an earlier draft of this set — `9: Confirmed Total Loss`
# — has been retired and does not exist in either the live data or the
# current phases CSV. Removed June 29 2026 before deploy.
PRODUCTION_DONE_PHASES = {
    '6: Done, To Estimator',
    '6:Done, To Estimator',
    '6: Waiting on Insurance for Delivery',
    '6:Waiting on Insurance for Delivery',
    '6:Repairs Complete, Customer Notified',
    '[Completed]',
    '9: TL Car Has Released',
    '9:TL Car Has Released',
}

# DTBS Repair Status rank — for phase progression logic.
# Higher = further along. Only overwrite if new rank > current rank.
# Sublet (13) and Total Loss (99) are exceptions handled in mapping logic.
DTBS_STATUS_RANK = {
    '':                       0,
    'Prelim':                 1,
    'Pre-Production':         2,
    'Dispatch':               3,
    'Teardown':               4,
    'Waiting on Insurance':   5,
    'Supp-Estimator':         6,
    'Supp-Insurance':         7,
    'Waiting on Parts':       8,
    'Need Parts Update':      9,
    'Frame':                  10,
    'Repair in Process':      11,
    'Paint':                  12,
    'Sublet':                 13,   # special — always overwrites
    'Reassembly':             14,
    'QC':                     15,
    'Wash':                   16,
    'Done':                   17,
    'Ready for Delivery':     18,
    'Delivered':              19,
    'Total Loss':             99,   # special — terminal, always overwrites
}

# CCC Repair Phase → DTBS Repair Status mapping.
# Returns None for "skip — manual stays". Whitespace in CCC values is normalized
# at lookup time (collapse multiple spaces, trim) so "2:X-Ray" and "2: X-Ray" both match.
PHASE_MAPPING = {
    # Bracketed (auto-assigned)
    '[Not Started]':                       None,
    '[Scheduled]':                         None,
    '[No Plan]':                           None,
    '[Completed]':                         'Ready for Delivery',

    # Phase 0: Intake placeholders
    '0:Check-In':                          None,
    '0:Towed-In (Needs Estimate)':         None,
    '0:Vehicle Arrived at Shop':           None,

    # Phase 1: Intake/Prep
    '1:Pre-Scan':                          'Teardown',
    '1:Pre-Wash':                          'Teardown',
    '1:Dispatch':                          'Dispatch',
    '1:PDR':                               'Teardown',
    '1:Disassembly':                       'Teardown',
    '1:Disassembly ANNEX':                 'Teardown',
    '1:Blueprinting':                      'Teardown',
    '1:Estimator file review':             'Teardown',
    '1:Glass Removal':                     'Teardown',
    '1:Scope for Hail Damage (not disass)': 'Teardown',
    '1:To PDR for Scope':                  'Teardown',
    '1:Done at PDR, ready to Dispatch':    'Dispatch',

    # Phase 2: Repair (Frame work has its own DTBS status)
    '2:X-Ray':                             'Teardown',
    '2:Body':                              'Repair in Process',
    '2:Body ANNEX':                        'Repair in Process',
    '2:Frame repair in process':           'Frame',
    '2:Frame/Unibody':                     'Frame',
    '2:Repair In Process':                 'Repair in Process',
    '2:Repair Ready to Start':             'Repair in Process',
    '2:Re-Work Reqd - Body':               'Repair in Process',
    '2:Re-Work Requd - Body':              'Repair in Process',  # CCC's actual phase name (added May 28)

    # Phase 3: Paint
    '3:Prep / Prime':                      'Paint',
    '3:Paint':                             'Paint',
    '3:Paint ANNEX':                       'Paint',
    '3:Paint In Process':                  'Paint',
    '3:Glass Install':                     'Paint',
    '3:Buff/Polish':                       'Paint',
    '3:Re-Work Reqd - Paint':              'Paint',
    '3.1:Paint':                           'Paint',
    '3.2:Paint ANNEX':                     'Paint',
    '3:Re-Work Requd - Paint':             'Paint',  # CCC's actual phase name (added May 28)

    # Phase 4: Reassembly
    '4:Reassembly':                        'Reassembly',
    '4:Reassembly ANNEX':                  'Reassembly',
    '4:Reassy':                            'Reassembly',  # Added May 28 — CCC shortened "Reassembly" to "Reassy" (sync output showed unmapped)
    '4:Glass Install':                     'Reassembly',  # Added May 28 — distinct from deprecated 3:Glass Install

    # Phase 5: QC / Detail
    '5:Detail':                            'QC',
    '5:QC':                                'QC',
    '5:QC FAIL':                           'QC',
    '5:Post-Scan':                         'QC',
    '5:Wash':                              'Wash',  # Added May 28 — CCC phase not in admin list but appearing in production (sync output showed unmapped)

    # Phase 6: Done / Customer
    '6:Done, To Estimator':                'Done',
    '6:Repairs Complete, Customer Notified': 'Ready for Delivery',
    '6:Waiting on Insurance for Delivery': 'Ready for Delivery',
    '6:CustomerRequestRecall/Oilchange ef': None,  # service work — skip
    '6:CustomerRequestRecall/Oilchange etc': None,  # CCC's actual phase name — service work, skip (added May 28)

    # Phase 7: Sublet (special — always overwrites)
    '7:Sublet Alignment':                  'Sublet',
    '7:Sublet Calibration':                'Sublet',
    '7:Sublet Clear Bra / Tint / Vinyl':   'Sublet',
    '7:Sublet Mechanical':                 'Sublet',
    '7:Sublet Other (see notes)':          'Sublet',
    '7:Sublet PDR':                        'Sublet',
    '7:Sublet Spray-In Bedliner':          'Sublet',
    '7:Sublet Wheel':                      'Sublet',
    '7:In House Mechanical':               'Sublet',

    # Phase 8: Parts
    '8: parts: take to annex':             'Waiting on Parts',
    '8:parts:CHECK':                       'Waiting on Parts',
    '8:Parts on Order':                    'Waiting on Parts',
    '8:parts: BACK ORDERED PARTS':         'Waiting on Parts',
    '8:parts: Waiting for parts delivery': 'Waiting on Parts',
    '8:parts:Paint Delay':                 'Waiting on Parts',
    '8:parts:Reassy Delay':                'Waiting on Parts',
    '8:parts:Repair Delay':                'Waiting on Parts',
    '8:parts:See Notes':                   'Waiting on Parts',
    '8:parts:Sublet Delay':                'Waiting on Parts',
    '8:parts:Dispatch Delay':              'Waiting on Parts',  # Added May 28 — not in current CCC phase list but confirmed real

    # Phase 9: Holds & Total Loss (Total Loss = special, always overwrites)
    '9:1st Suppl Hold':                    'Supp-Estimator',
    '9:2nd Suppl Hold':                    'Supp-Estimator',
    '9:Supplement Hold':                   'Supp-Estimator',
    '9:Insurance Prelim':                  'Supp-Insurance',
    '9:Second Waiting on Auth':            'Supp-Insurance',
    '9:Waiting on Authorization':          'Supp-Insurance',
    '9:Possible Total Loss':               'Total Loss',
    '9:TL Needs Release':                  'Total Loss',
    '9:TL Car Has Released':               'Total Loss',
    '9:Admin Delay 1 (see notes)':         None,
    '9:Admin Delay 2 (see notes)':         None,
    '9:Production Delay (see notes)':      None,

    # Phase 10
    '10: sublet detail':                   'Sublet',
    '10:Detail':                           'QC',  # Added May 28 — same logic as 5:Detail
}

PHASES_ALWAYS_OVERWRITE = {'Sublet', 'Total Loss'}

# ─── EMS helpers (unchanged) ──────────────────────────────────────

def read_dbf(path):
    try:
        table = DBF(path, ignore_missing_memofile=True)
        records = [dict(r) for r in table]
        return records
    except:
        return []

def get_val(records, field):
    for r in records:
        v = r.get(field)
        if v and str(v).strip() not in ('', '0', 'False'):
            return str(v).strip()
    return ''

# ─── Input coercion helper (defensive parsing) ────────────────────

def coerce_sharepoint_items(raw):
    """Coerce sharepoint_items input into a list of dicts.

    Power Automate's HTTP Body field can serialize an array reference as either
    a real JSON array OR a JSON-encoded string depending on how the chip/
    expression was configured. This helper accepts both shapes so the API
    is robust to PA quirks.

    Returns (items_list, error_message). If error_message is set, items_list
    is None and the caller should return a 400 with that message.
    """
    if raw is None:
        return [], None  # missing field is treated as empty list, not an error
    # If PA sent it as a JSON-encoded string, parse it back.
    if isinstance(raw, str):
        stripped = raw.strip()
        if not stripped:
            return [], None
        try:
            raw = json.loads(stripped)
        except json.JSONDecodeError as e:
            return None, f'sharepoint_items is a string but not valid JSON: {str(e)}. First 200 chars: {stripped[:200]}'
    if not isinstance(raw, list):
        return None, f'sharepoint_items must be a list, got {type(raw).__name__}. Sample: {repr(raw)[:200]}'
    # PA sometimes wraps items in an extra array layer in various ways:
    #   Shape A: [[{dict}, {dict}, ...]]            — entire array wrapped once
    #   Shape B: [[{dict}], [{dict}], [{dict}], ...] — each dict wrapped individually
    #   Shape C: mix of bare dicts and wrapped lists
    # Solution: flatten any list elements one level deep, keep dicts as-is.
    flattened = []
    for entry in raw:
        if isinstance(entry, list):
            flattened.extend(entry)
        else:
            flattened.append(entry)
    raw = flattened
    for i, item in enumerate(raw):
        if not isinstance(item, dict):
            return None, f'sharepoint_items[{i}] must be a dict, got {type(item).__name__}. Sample: {repr(item)[:200]}'
    return raw, None

# ─── Insurance Lookup helper (Phase 6 — insurance normalization) ──

def normalize_insurance_name(raw_name, lookup_list):
    """Match a raw insurance carrier name against the Insurance Lookup list.

    Args:
        raw_name: Carrier name from CCC report or EMS export (e.g., 'STATE FARM').
        lookup_list: List of dicts with at least 'Title' and 'DisplayName' keys,
                     OR an empty list if the caller has none (see Option B fallback below).

    Returns:
        - If raw_name is blank/None: empty string (sync flow will write null/skip).
        - If lookup_list is empty: raw_name unchanged (Option B — graceful fallback
          so PA flows that accidentally send an empty lookup don't flood SP with
          ⚠️ markers; reverts to pre-Phase-6 behavior for that run).
        - If a match is found (case-insensitive Title equality): the DisplayName.
        - If no match: '⚠️ <raw_name>' so the value surfaces in the weekly
          Insurance Pending Review email (Option X — keeps the bug visible).

    Matching is case-insensitive on the Title field. Both sides are trimmed
    of whitespace. Same logic Flow 6 (EMS Parser) uses for its create/update
    insurance writes.
    """
    if not raw_name or not str(raw_name).strip():
        return ''
    raw = str(raw_name).strip()

    # Option B — empty lookup → skip normalization, return raw passthrough.
    # Protects against PA "Get Insurance Lookup" returning [] on transient errors.
    if not lookup_list:
        return raw

    raw_lower = raw.lower()
    for entry in lookup_list:
        if not isinstance(entry, dict):
            continue
        title = entry.get('Title')
        if title is None:
            continue
        if str(title).strip().lower() == raw_lower:
            display = entry.get('DisplayName')
            if display is not None and str(display).strip():
                return str(display).strip()
            # Title matched but DisplayName is blank — treat as no usable match.
            break

    # No match — return ⚠️-prefixed raw name to surface in the digest email.
    return f'⚠️ {raw}'

# ─── Change Diff Helper (May 18, 2026 — email visibility feature) ─────

def is_blank_for_diff(v):
    """Treat None, empty string, and whitespace-only strings as blank.
    Boolean False is NOT blank — Yes/No=No is a real value worth showing in diffs.
    """
    if v is None:
        return True
    if isinstance(v, str) and v.strip() == '':
        return True
    return False

def format_value_for_diff(v, value_type):
    """Format a value for human-readable display in the email's Changes column.

    value_type: 'bool' | 'date' | 'text' | 'percent'
    """
    if is_blank_for_diff(v):
        return '(blank)'
    if value_type == 'bool':
        # PA may serialize Yes/No as Python True/False, or as 'true'/'false' strings
        if v is True or (isinstance(v, str) and v.lower() in ('true', 'yes', '1')):
            return 'Yes'
        if v is False or (isinstance(v, str) and v.lower() in ('false', 'no', '0')):
            return 'No'
        return str(v)
    if value_type == 'date':
        # Trim time portion if present — show just YYYY-MM-DD
        s = str(v).strip()
        if 'T' in s:
            return s.split('T')[0]
        if ' ' in s and len(s) > 10:
            return s.split(' ')[0]
        return s[:10] if len(s) >= 10 else s
    if value_type == 'percent':
        # Rounded to 4 decimals — strips float-precision noise like 1.0 vs 1.000000000
        try:
            return f"{round(float(v), 4):.4f}"
        except (ValueError, TypeError):
            return str(v).strip()
    # text — return as-is, trimmed
    return str(v).strip()

def values_equal_for_diff(old, new, value_type):
    """Compare old SP value vs new value with blank-equivalence and type awareness.
    Returns True if they should be considered "the same" (no change to report).
    """
    old_blank = is_blank_for_diff(old)
    new_blank = is_blank_for_diff(new)
    if old_blank and new_blank:
        return True
    if old_blank != new_blank:
        return False
    # Both non-blank
    if value_type == 'bool':
        return format_value_for_diff(old, 'bool') == format_value_for_diff(new, 'bool')
    if value_type == 'date':
        return format_value_for_diff(old, 'date') == format_value_for_diff(new, 'date')
    if value_type == 'percent':
        # Compare rounded-to-4-decimal representations
        return format_value_for_diff(old, 'percent') == format_value_for_diff(new, 'percent')
    # text — case-insensitive trim compare
    return str(old).strip().lower() == str(new).strip().lower()

def compute_changes(sp_item, new_values, field_specs):
    """Compute a list of changes between SP's current values and what would be written.

    Args:
        sp_item: dict of current SP values (from Project SP Items projection).
        new_values: dict keyed by sp_key with the value the flow would write.
                    Only fields present here are compared.
        field_specs: ordered list of (sp_key, display_name, value_type) tuples.
                     Order determines email row order (importance-ranked).

    Returns:
        List of {"field": display_name, "old": str, "new": str} dicts.
        Empty list if nothing changed.
    """
    changes = []
    for sp_key, display_name, value_type in field_specs:
        if sp_key not in new_values:
            continue
        old = sp_item.get(sp_key)
        new = new_values.get(sp_key)
        if values_equal_for_diff(old, new, value_type):
            continue
        changes.append({
            'field': display_name,
            'old':   format_value_for_diff(old, value_type),
            'new':   format_value_for_diff(new, value_type),
        })
    return changes

def format_changes_text(changes):
    """Pre-format the changes list as a human-readable string for the email.

    Returns '(no changes)' for empty list, or '; '-joined entries like
    'Done: No → Yes; Actual Delivery: (blank) → 2026-05-01'.

    PA can just insert this string directly — no nested loops or xpath tricks
    needed on the flow side.
    """
    if not changes:
        return '(no changes)'
    return '; '.join(f"{c['field']}: {c['old']} → {c['new']}" for c in changes)

# Field specs per sync flow — ordered by importance (Done/Closed/status first,
# then dates, then identifiers, then insurance/estimator). Display names match
# the SP column display names for readability in the email.

# Flow 10 (RO Report Sync) — writes: Title, WorkfileID, CCCPromisDate,
# ActualDelivery, Done, Closed (hardcoded No), TotalLoss, Insurance, Estimator
RO_SYNC_DIFF_FIELDS = [
    ('done',                  'Done',                  'bool'),
    ('total_loss',            'Total Loss',            'bool'),
    ('cccpromisdate',         'CCC Promise Date',      'date'),
    ('actual_delivery',       'Actual Delivery',       'date'),
    ('ro_number',             'RO #',                  'text'),
    ('workfile_id',           'Workfile ID',           'text'),
    ('insurance',             'Insurance',             'text'),
    ('estimator',             'Estimator',             'text'),
]

# Flow 10a (Production Sync) — writes: Title, WorkfileID, CCCPromisDate,
# DropDate, RepairStatus, Tech, Painter, Closed (hardcoded No), TotalLoss,
# Insurance, Estimator
PRODUCTION_SYNC_DIFF_FIELDS = [
    ('repair_status',         'Repair Status',         'text'),
    ('tech',                  'Tech',                  'text'),
    ('painter',               'Painter',               'text'),
    ('total_loss',            'Total Loss',            'bool'),
    ('cccpromisdate',         'CCC Promise Date',      'date'),
    ('drop_date',             'Drop Date',             'date'),
    ('ro_number',             'RO #',                  'text'),
    ('workfile_id',           'Workfile ID',           'text'),
    ('insurance',             'Insurance',             'text'),
    ('estimator',             'Estimator',             'text'),
# Production metrics — added June 4, 2026
    ('parts_received_pct',    'Parts Received %',      'percent'),
    ('labor_assigned_pct',    'Labor Assigned %',      'percent'),
    ('repair_plan_comments',  'Repair Plan Notes',     'text'),
# Done semantics — added June 29 2026 (Batch 4 / Item 4)
# Production Sync now owns the Done milestone (production-complete, NOT
# delivered). DoneStatusTime stamped from CCC's real repair_completed_datetime
# rather than utcNow() to avoid a fake MTD spike when ~35 stale rows flip.
    ('done',                  'Done',                  'bool'),
    ('donestatustime',        'Done Status Time',      'datetime'),
]

# Flow 10b (Cleanup Sync) — writes: Title, WorkfileID, CCCPromisDate, Done,
# Closed, ActualDelivery, TotalLoss, Insurance, Estimator
CLEANUP_SYNC_DIFF_FIELDS = [
    ('done',                  'Done',                  'bool'),
    ('closed',                'Closed',                'bool'),
    ('actual_delivery',       'Actual Delivery',       'date'),
    ('total_loss',            'Total Loss',            'bool'),
    ('cccpromisdate',         'CCC Promise Date',      'date'),
    ('ro_number',             'RO #',                  'text'),
    ('workfile_id',           'Workfile ID',           'text'),
    ('insurance',             'Insurance',             'text'),
    ('estimator',             'Estimator',             'text'),
]

# Phase 7 (June 30 2026) — Closed Report Sync (Flow 10e).
# Authoritative source for Closed=Yes and ClosedStatusTime, replacing
# Flow 13b (utcNow stamp on Closed flip) and Flow 14c (one-time backfill).
# Writes Closed=True + ClosedStatusTime sourced from CCC's real closed_date.
# Also writes Done=True monotonic (a closed file must be done).
CLOSED_SYNC_DIFF_FIELDS = [
    ('done',                  'Done',                  'bool'),
    ('closed',                'Closed',                'bool'),
    ('closed_status_time',    'Closed Status Time',    'date'),
    ('total_loss',            'Total Loss',            'bool'),
    ('ro_number',             'RO #',                  'text'),
    ('workfile_id',           'Workfile ID',           'text'),
]

# ─── RO Report helpers ────────────────────────────────────────────

def _xml_text(parent, tag):
    el = parent.find(tag)
    return (el.text or '').strip() if el is not None and el.text else ''

def parse_ro_report_xml(xml_bytes):
    """Parse CCC ONE native XML reportResponse 'Repair Orders Created' export."""
    root = ET.fromstring(xml_bytes)
    results = []
    for o in root.findall('.//repairOrder'):
        ro_number = _xml_text(o, 'repair_order_number')
        if not ro_number:
            continue
        results.append({
            'ro_number':         ro_number,
            'workfile_id':       _xml_text(o, 'workfile_id'),
            'owner':             _xml_text(o, 'owner_name'),
            'vehicle':           _xml_text(o, 'vehicle_year_make_model'),
            'estimator':         _xml_text(o, 'service_writer_display_name'),
            'insurance_company': _xml_text(o, 'carrier_name'),
            'vehicle_out':       _xml_text(o, 'vehicle_out_datetime'),
            'ro_status':         _xml_text(o, 'file_status_name'),
            'estimate_total':    _xml_text(o, 'estimate_gross_amount'),
            'total_loss':        _xml_text(o, 'is_total_loss').lower() == 'true',
        })
    return results

def parse_production_schedule_xml(xml_bytes):
    """Parse CCC ONE native XML 'Production Schedule' export."""
    root = ET.fromstring(xml_bytes)
    results = []
    for o in root.findall('.//repairOrder'):
        ro_number = _xml_text(o, 'repair_order_number')
        if not ro_number:
            continue
        results.append({
            'ro_number':         ro_number,
            'workfile_id':       _xml_text(o, 'workfile_id'),
            'owner':             _xml_text(o, 'owner_name'),
            'vehicle':           _xml_text(o, 'vehicle_year_make_model'),
            'estimator':         _xml_text(o, 'service_writer_display_name'),
            'insurance_company': _xml_text(o, 'carrier_name'),
            'vehicle_in':        _xml_text(o, 'vehicle_in_datetime'),
            'vehicle_out':       _xml_text(o, 'vehicle_out_datetime'),
            'repair_phase':      _xml_text(o, 'repair_phase_name'),
            'body_tech':         _xml_text(o, 'body_technician_display_name'),
            'paint_tech':        _xml_text(o, 'paint_technician_display_name'),
            'mechanical_tech':   _xml_text(o, 'mechanical_technician_display_name'),
            'days_in_shop':       _xml_text(o, 'days_in_shop'),
            'parts_received_pct': _xml_text(o, 'parts_received_percent'),
            'labor_assigned_pct': _xml_text(o, 'labor_assigned_percent'),
            'repair_plan_comments': _xml_text(o, 'repair_plan_comments'),
            'total_loss':        _xml_text(o, 'is_total_loss').lower() == 'true',
            # Path B signal fields (May 20 2026 — dollar/weak signal verification)
            'estimate_total':    _xml_text(o, 'estimate_gross_amount'),
            'drop_date':         _xml_text(o, 'vehicle_in_datetime'),
            'promise_date':      _xml_text(o, 'repair_completed_datetime'),
            # Batch 4 (June 29 2026): same XML element as promise_date above,
            # but exposed under its real CCC name so callers using it as a
            # "real completion date" don't have to know about the legacy
            # alias. Per Lesson 108, repair_completed_datetime is a real
            # human-entered event timestamp on completed phases (not a
            # placeholder) — safe to stamp DoneStatusTime from it.
            'repair_completed_datetime': _xml_text(o, 'repair_completed_datetime'),
            'color':             _xml_text(o, 'vehicle_exterior_paint_color'),
        })
    # Batch 4 (June 29 2026): strip known placeholder names from tech fields at
    # the parser boundary so every downstream consumer sees clean data.
    # Filtering here (rather than in selection logic) means: a single place to
    # maintain the list, no risk of drift, and any future reporting on raw
    # parsed data also gets the cleanup for free. See TECH_PLACEHOLDERS.
    for r in results:
        for fld in ('body_tech', 'paint_tech', 'mechanical_tech'):
            if r.get(fld, '').strip() in TECH_PLACEHOLDERS:
                r[fld] = ''
    return results

def parse_vehicles_scheduled_out_xml(xml_bytes):
    """Parse CCC ONE native XML 'Vehicles Scheduled Out' export."""
    root = ET.fromstring(xml_bytes)
    results = []
    for o in root.findall('.//repairOrder'):
        ro_number = _xml_text(o, 'repair_order_number')
        if not ro_number:
            continue
        results.append({
            'ro_number':         ro_number,
            'workfile_id':       _xml_text(o, 'workfile_id'),
            'owner':             _xml_text(o, 'owner_name'),
            'vehicle':           _xml_text(o, 'vehicle_year_make_model'),
            'estimator':         _xml_text(o, 'service_writer_display_name'),
            'insurance_company': _xml_text(o, 'carrier_name'),
            'vehicle_out':       _xml_text(o, 'vehicle_out_datetime'),
            'is_delivered':      _xml_text(o, 'is_delivered').lower() == 'true',
            'total_loss':        _xml_text(o, 'is_total_loss').lower() == 'true',
            'file_status':       _xml_text(o, 'file_status_name'),
            # Path B signal fields (May 20 2026 — dollar/weak signal verification)
            'estimate_total':    _xml_text(o, 'estimate_gross_amount'),
            'color':             _xml_text(o, 'vehicle_exterior_paint_color'),
        })
    return results

def parse_closed_report_xml(xml_bytes):
    """Parse CCC ONE native XML 'Repair Orders Closed' export (Phase 7).

    This report is authoritative for closed state. Unlike the Cleanup Sync
    report (which carries `vehicle_out_datetime` + `file_status_name` but
    not a close date), this one carries `closed_date` — the true CCC close
    timestamp. That field is what Flow 10e stamps as ClosedStatusTime,
    eliminating the utcNow() drift that Flow 13b imposed on backdated
    closes.

    Field shape mirrors the Cleanup Sync parser so run_match_engine works
    on the output without modification.
    """
    root = ET.fromstring(xml_bytes)
    results = []
    for o in root.findall('.//repairOrder'):
        ro_number = _xml_text(o, 'repair_order_number')
        if not ro_number:
            continue
        results.append({
            'ro_number':         ro_number,
            'workfile_id':       _xml_text(o, 'workfile_id'),
            'owner':             _xml_text(o, 'owner_name'),
            'vehicle':           _xml_text(o, 'vehicle_year_make_model'),
            'estimator':         _xml_text(o, 'service_writer_display_name'),
            'insurance_company': _xml_text(o, 'carrier_name'),
            # Authoritative close timestamp — ISO 8601 like '2026-06-16T00:00:00'.
            # 10e stamps this as ClosedStatusTime (formatted T18:00:00Z to match
            # the noon-Mountain convention used elsewhere).
            'closed_date':       _xml_text(o, 'closed_date'),
            'file_status':       _xml_text(o, 'file_status_name'),
            'total_loss':        _xml_text(o, 'is_total_loss').lower() == 'true',
            # Path B signal fields (parity with Cleanup Sync for matcher use)
            'estimate_total':    _xml_text(o, 'estimate_gross_amount'),
            # vehicle_out exposed for compatibility — Closed report does
            # carry it in some rows but it's not the authoritative timestamp.
            'vehicle_out':       _xml_text(o, 'vehicle_out_datetime'),
        })
    return results

def normalize_owner(owner):
    """Both EMS parser and CCC reports now produce 'Last, First' format
    for human customers. This helper just trims and standardizes whitespace.
    Function preserved (not deleted) so all existing run_match_engine call
    sites continue to work without modification.

    Edge case: dealer/company names ('JAGUAR LANDROVER DOWNTOWN SALT LAKE')
    have no comma — pass through unchanged.
    """
    if not owner:
        return ''
    return owner.strip()

def normalize_year_4to2(vehicle):
    """Convert '2023 RANG Discovery Sport' to '23 RANG Discovery Sport'."""
    if not vehicle:
        return ''
    m = re.match(r'^\s*(\d{4})(\s+.*)$', vehicle)
    if m:
        return f"{m.group(1)[2:]}{m.group(2)}"
    return vehicle

def estimator_first_name_match(report_estimator, list_estimator):
    """Tiebreaker: list 'Dana' matches report 'Dana Hulse'."""
    if not report_estimator or not list_estimator:
        return False
    rep = report_estimator.lower()
    lst = list_estimator.lower()
    return rep == lst or rep.startswith(lst + ' ')

def estimator_first_name(full_name):
    """Extract first word from 'Cordale Briggs' → 'Cordale'."""
    if not full_name:
        return ''
    return full_name.strip().split()[0] if full_name.strip() else ''

def insurance_needs_correction(sp_insurance):
    """True if the SharePoint insurance value starts with the truncation/missing marker."""
    if not sp_insurance:
        return False
    return sp_insurance.strip().startswith('⚠️')

# ─── Phase 3 helpers ──────────────────────────────────────────────

def normalize_phase_key(phase):
    """Normalize CCC phase string for mapping lookup.
    Handles: '2:X-Ray' / '2: X-Ray' / ' 2:X-Ray ' all → '2:X-Ray'
    """
    if not phase:
        return ''
    # Collapse internal whitespace, then remove any space directly after a colon
    collapsed = re.sub(r'\s+', ' ', phase.strip())
    return re.sub(r':\s+', ':', collapsed)

def map_phase_to_status(ccc_phase):
    """Map a CCC repair_phase_name to a DTBS RepairStatus value.
    Returns None if the phase shouldn't trigger a write."""
    if not ccc_phase:
        return None
    key = normalize_phase_key(ccc_phase)
    return PHASE_MAPPING.get(key)

def should_write_status(new_status, current_status):
    """CCC Production Sync is authoritative — any mapped status overwrites.
    
    Policy changed May 28, 2026: removed rank-based progression rule. CCC is now
    the source of truth for RepairStatus. Previous rule blocked backward moves
    (e.g., kick-backs to re-work or parts holds) which were silently leaving SP
    out of sync with reality. Only exception is None mappings — phases explicitly
    flagged as non-production (Admin Delay, Production Delay, etc.) still don't write.
    
    PHASES_ALWAYS_OVERWRITE is preserved as a no-op set for now (Sublet, Total Loss)
    since they're naturally included by the always-overwrite policy. The constant
    is retained for documentation value and potential future use if a more nuanced
    policy returns.
    """
    if new_status is None:
        return False
    return True

def map_tech(ccc_full_name):
    """Map CCC body tech full name to DTBS Tech choice value.
    Returns empty string if no mapping (don't write).

    This is a pure name-translation lookup — see select_tech() below for
    the priority logic that decides WHICH source field to read.
    """
    if not ccc_full_name:
        return ''
    return TECH_MAPPING.get(ccc_full_name.strip(), '')

def select_tech(row):
    """Pick the DTBS Tech value for a Production Schedule row using
    priority-ordered selection (Batch 4, June 29 2026).

    Background: SP "Tech" = body technician only. CCC's data places real
    techs in different fields depending on the job, and has no glass-tech
    field at all. The June 19 reconciliation found three failure modes
    the prior single-field `map_tech(row['body_tech'])` approach missed:
      - Body techs hiding in `mechanical_technician_display_name` on
        special-rate jobs (aluminum, EV rate, JLR rate). Confirmed
        examples: CCC-1173/1092/1123 → Jason / Kyle / Kyle in mechanical.
      - Glass techs (Tyler Evans, Nic Moffitt) scattered across body OR
        mechanical depending on the file. CCC has no glass field.
      - Placeholder names (Jesus Zavala / David / Mike Ford) appearing
        in tech fields — already stripped at the parser boundary.

    Priority (first match wins):
      1. KNOWN GLASS TECH present in any tech field → use them. Captures
         Tyler E / Nic wherever they land. Most glass files are estimated
         by Jennie, but rare exceptions exist (Cordale-estimated glass on
         CCC-1162), so the check is identity-based, not estimator-gated.
      2. BODY tech present → use it. The default, post-placeholder-strip.
      3. MECHANICAL tech present AND estimator is NOT Jennie → use it.
         This is the special-rate body-tech-in-mechanical case. The
         Jennie guard prevents stamping a support/calibration tech (e.g.
         Uriah) as the tech on a glass file where the real glass tech
         (e.g. Nic) wasn't included in the export. Confirmed safe by
         the prompt's data review: Jesus never appears in mechanical-
         when-body-is-blank, so this fallback won't grab him even if he
         somehow slipped past the parser strip.
      4. ELSE → blank (manual entry). Specifically: a Jennie file where
         only a support tech is present, e.g. CCC-1286.

    Returns the translated DTBS value (e.g. 'Kyle', 'Tyler E') or ''.
    """
    body = (row.get('body_tech') or '').strip()
    mech = (row.get('mechanical_tech') or '').strip()
    estimator = (row.get('estimator') or '').strip()
    estimator_first = estimator.split()[0] if estimator else ''

    # 1. Glass tech in ANY field — highest priority, captures them wherever placed
    for candidate in (body, mech):
        if candidate in GLASS_TECHS:
            return map_tech(candidate)

    # 2. Body tech present (default path)
    if body:
        mapped = map_tech(body)
        if mapped:
            return mapped

    # 3. Mechanical fallback, but never on Jennie files (estimator first-name guard)
    if mech and estimator_first.lower() != 'jennie':
        mapped = map_tech(mech)
        if mapped:
            return mapped

    # 4. Blank — manual entry case
    return ''

def map_painter(ccc_full_name):
    """Map CCC paint tech full name to DTBS Painter choice value."""
    if not ccc_full_name:
        return ''
    return PAINTER_MAPPING.get(ccc_full_name.strip(), '')

def insurance_needs_fix_or_blank(sp_insurance):
    """True if SP insurance is blank or starts with ⚠️ (broken lookup).
    Used by Production Sync and Cleanup Sync — broader than the existing
    insurance_needs_correction() helper which only catches ⚠️."""
    if not sp_insurance or not sp_insurance.strip():
        return True
    return sp_insurance.strip().startswith('⚠️')

def ro_match_type(sp_ro_norm, report_ro_norm):
    """Classify how an SP ro_number relates to a report ro_number.
    Both inputs should be lowercased and trimmed.

    Returns:
      'exact'      — SP equals report exactly
      'compatible' — SP starts with report + '-' (Tekion suffix case;
                     e.g., report 'ccc-0280' vs SP 'ccc-0280-1')
      None         — no relationship

    Asymmetric on purpose: SP is the side that may have a suffix added
    after a Tekion connection break. The report (CCC ONE) side is
    canonical because changing the RO# in CCC severs the Tekion link.
    """
    if not sp_ro_norm or not report_ro_norm:
        return None
    if sp_ro_norm == report_ro_norm:
        return 'exact'
    if sp_ro_norm.startswith(report_ro_norm + '-'):
        return 'compatible'
    return None

def ro_compatible(sp_ro_norm, report_ro_norm):
    """True if SP ro_number is exact or compatible with report's.
    Wrapper around ro_match_type for the boolean case (Path B disqualifier).
    """
    return ro_match_type(sp_ro_norm, report_ro_norm) is not None

# ─── Path B signal scoring (May 20 2026) ─────────────────────────
# Dollar floor RETIRED June 19 2026. Previously 500.0 — the idea was that
# sub-$500 amounts collide easily, so they shouldn't confirm a match. In
# practice, penny-exact + uniqueness-among-candidates already measures
# coincidence directly, and the floor was silently dropping legitimate
# sub-$500 glass/rockchip jobs (e.g. Hoggan CCC-1289, $54.23) into ambiguous.
# Kept at 0.0 so any lingering reference is a harmless no-op; penny-match and
# uniqueness now do all the discrimination work.
PATH_B_DOLLAR_FLOOR = 0.0

def _parse_dollar(value):
    """Parse a dollar string to float. Returns None for blank/invalid input.
    Handles both CCC report format ('4287.31') and SP currency strings.
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    # Strip currency symbols and commas
    s = re.sub(r'[$,\s]', '', s)
    try:
        return float(s)
    except (ValueError, TypeError):
        return None

def dollars_penny_match(report_total, sp_total):
    """True if both amounts parse to a number and match to the penny.
    Returns False if either side is blank/unparseable, or differs by ≥ 1 cent.

    No dollar floor (retired June 19 2026): a $54.23 penny-match is as
    trustworthy as a $5,423.00 one — uniqueness-among-candidates, not
    magnitude, is what guards against coincidental collisions.
    """
    r = _parse_dollar(report_total)
    s = _parse_dollar(sp_total)
    if r is None or s is None:
        return False
    # Compare to the penny — abs difference under half a cent counts as equal
    return abs(r - s) < 0.005

def insurance_signal(report_carrier, sp_insurance, insurance_lookup):
    """Returns one of:
      'agree'      — both non-blank, normalize to same carrier
      'disagree'   — both non-blank, normalize to different carriers
      'skip'       — at least one side blank, or lookup empty (signal unavailable)
    """
    rc = (report_carrier or '').strip()
    si = (sp_insurance or '').strip()
    if not rc or not si or not insurance_lookup:
        return 'skip'
    # Strip ⚠️ markers for fair comparison
    if si.startswith('⚠️'):
        si = si.replace('⚠️', '', 1).strip()
    sp_normalized = normalize_insurance_name(si, insurance_lookup)
    report_normalized = normalize_insurance_name(rc, insurance_lookup)
    sp_compare = sp_normalized.replace('⚠️', '').strip().lower()
    report_compare = report_normalized.replace('⚠️', '').strip().lower()
    if not sp_compare or not report_compare:
        return 'skip'
    return 'agree' if sp_compare == report_compare else 'disagree'

def _date_prefix(value):
    """Get YYYY-MM-DD prefix from any ISO-ish date string. Returns '' on failure."""
    if not value:
        return ''
    s = str(value).strip()
    # Take everything before 'T' or space, fall back to first 10 chars
    if 'T' in s:
        s = s.split('T', 1)[0]
    elif ' ' in s:
        s = s.split(' ', 1)[0]
    return s[:10] if len(s) >= 10 else ''

def _date_within_one_day(date_a, date_b):
    """True if two ISO-prefix dates are within ±1 calendar day.
    Returns False on blank or unparseable input. Pure-string comparison
    using day-of-year math (avoids importing datetime for one helper).
    """
    a = _date_prefix(date_a)
    b = _date_prefix(date_b)
    if not a or not b or len(a) < 10 or len(b) < 10:
        return False
    # Compare year, month, day separately. If year+month match and day differs
    # by ≤ 1, accept. Cross-month/year boundaries are rare enough that we accept
    # only exact match on those (avoids leap-year and month-length pitfalls).
    try:
        ya, ma, da = int(a[0:4]), int(a[5:7]), int(a[8:10])
        yb, mb, db = int(b[0:4]), int(b[5:7]), int(b[8:10])
    except ValueError:
        return False
    if ya == yb and ma == mb:
        return abs(da - db) <= 1
    return a == b

def weak_signal_estimator(report_estimator, sp_estimator):
    """True if estimator first names match (case-insensitive)."""
    return estimator_first_name_match(report_estimator, sp_estimator)

def weak_signal_color(report_color, sp_color):
    """True if vehicle colors match (lowercase, trimmed). False if either blank."""
    rc = (report_color or '').strip().lower()
    sc = (sp_color or '').strip().lower()
    if not rc or not sc:
        return False
    return rc == sc

def weak_signal_drop_date(report_drop, sp_drop):
    """True if drop dates match within ±1 day."""
    return _date_within_one_day(report_drop, sp_drop)

def weak_signal_promise_date(report_promise, sp_promise):
    """True if promise/target dates match within ±1 day."""
    return _date_within_one_day(report_promise, sp_promise)

def score_path_b_signals(report_row, sp_candidate, insurance_lookup):
    """Score a single (report row, SP candidate) pair against all Path B signals.

    Returns a dict with:
      strong_confirmations: list of signal names that strongly confirm
      strong_contradictions: list of signal names that actively contradict
      weak_confirmations: list of signal names that weakly confirm
      total_strong: count of strong confirmations
      total_weak: count of weak confirmations
      has_contradiction: True if any strong signal contradicts
      reason_parts: human-readable signal summary for email
    """
    strong_confirmations = []
    strong_contradictions = []
    weak_confirmations = []
    reason_parts = []

    # Strong signal 1: dollar amount (penny match, above floor)
    report_dollar = report_row.get('estimate_total')
    sp_dollar = sp_candidate.get('estimate_total')
    if dollars_penny_match(report_dollar, sp_dollar):
        strong_confirmations.append('dollar')
        amount = _parse_dollar(report_dollar)
        reason_parts.append(f'dollar ${amount:,.2f}')

    # Strong signal 2: insurance agreement
    ins_state = insurance_signal(
        report_row.get('insurance_company') or report_row.get('carrier_name'),
        sp_candidate.get('insurance'),
        insurance_lookup
    )
    if ins_state == 'agree':
        strong_confirmations.append('insurance')
        reason_parts.append('insurance')
    elif ins_state == 'disagree':
        strong_contradictions.append('insurance')

    # Weak signal 1: estimator first name
    if weak_signal_estimator(report_row.get('estimator', ''), sp_candidate.get('estimator', '')):
        weak_confirmations.append('estimator')

    # Weak signal 2: color
    if weak_signal_color(report_row.get('color', ''), sp_candidate.get('color', '')):
        weak_confirmations.append('color')

    # Weak signal 3: drop date
    if weak_signal_drop_date(report_row.get('drop_date', ''), sp_candidate.get('drop_date', '')):
        weak_confirmations.append('drop_date')

    # Weak signal 4: promise date
    if weak_signal_promise_date(report_row.get('promise_date', ''), sp_candidate.get('promise_date', '')):
        weak_confirmations.append('promise_date')

    return {
        'strong_confirmations': strong_confirmations,
        'strong_contradictions': strong_contradictions,
        'weak_confirmations': weak_confirmations,
        'total_strong': len(strong_confirmations),
        'total_weak': len(weak_confirmations),
        'has_contradiction': len(strong_contradictions) > 0,
        'reason_parts': reason_parts,
    }

def build_match_reason(score, base_type='customer_vehicle'):
    """Build a human-readable match_type label from signal scoring.

    Examples:
      base 'customer_vehicle', strong=['dollar']        → 'customer_vehicle_dollar'
      base 'customer_vehicle', strong=['insurance']     → 'customer_vehicle_insurance'
      base 'customer_vehicle', strong=['dollar', 'insurance']
                                                        → 'customer_vehicle_dollar+insurance'
      base 'customer_vehicle', weak=['estimator', 'color']
                                                        → 'customer_vehicle_weak_estimator+color'
    """
    if score['total_strong'] > 0:
        return base_type + '_' + '+'.join(score['strong_confirmations'])
    if score['total_weak'] > 0:
        return base_type + '_weak_' + '+'.join(score['weak_confirmations'])
    return base_type

# ─── Stale-delivered suppression (Batch 1b — June 19 2026) ──────────
# A report row whose vehicle_out date is older than this many days, AND
# which matches nothing live in SP, is a delivered/historical car. Such
# rows were landing in the "ambiguous — needs manual review" bucket
# (especially repeat-customer / dealer source-duplicate clusters) and
# inflating the count with cars that left the shop weeks ago and can
# never match anything. We suppress them silently into `unmatched`.
#
# IMPORTANT: this only ever reclassifies rows that had no live SP match
# anyway — it can never hide a row that would otherwise have matched.
# Start at 60; revisit after a few weeks of real-world running.
#
# NOTE (cross-batch): Item 1 (dead-file cleanup) has its own aging gates
# (FirstSeen stamp, cancelled-opp). When that ships, reconcile this
# constant with those so there's one coherent definition of "stale,"
# rather than two competing aging thresholds.
STALE_DELIVERED_DAYS = 60

def _vehicle_out_age_days(vehicle_out_raw):
    """Days since a report row's vehicle_out_datetime, or None if blank/unparseable.

    CCC RO Bulk emits ISO 8601 with offset, e.g. '2026-05-29T17:00:00-05:00'.
    Blank cells arrive as whitespace. Returns a non-negative int day count
    (0 if the date is in the future), or None when there's no usable date —
    None means "don't suppress on age" (we can't prove it's stale).
    """
    if not vehicle_out_raw or not str(vehicle_out_raw).strip():
        return None
    raw = str(vehicle_out_raw).strip()
    try:
        # Python's fromisoformat handles the '-05:00' offset form natively.
        dt = datetime.fromisoformat(raw)
    except (ValueError, TypeError):
        # Fall back: strip the time/offset and try a plain date parse.
        try:
            dt = datetime.fromisoformat(raw[:10])
        except (ValueError, TypeError):
            return None
    # Normalize to naive UTC-ish comparison: drop tzinfo, compare to utcnow.
    if dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    delta_days = (datetime.utcnow() - dt).days
    return max(delta_days, 0)

def run_match_engine(report_rows, sharepoint_items, insurance_lookup=None):
    """Shared matching engine. Returns (matched_pairs, unmatched, ambiguous).

    matched_pairs is list of (report_row, sp_item, match_type) tuples.
    Caller is responsible for building the response shape from these pairs.

    Match precedence:
      Path A    — workfile_id           (CCC-internal unique ID, exact)
      Path A.5  — ro_number             (exact, case-insensitive, trimmed)
                  ro_number_compatible  (Tekion suffix case: SP has extra '-N')
      Path B    — customer + vehicle    (signal-scored fallback)

    Path B includes a disqualifier: any SP row whose workfile_id or
    ro_number contradicts the report row's IDs is skipped from candidates.
    This prevents already-claimed SP rows from being magnet-matched on
    shared customer + vehicle prefix (e.g., dealer accounts with many ROs).

    Path B signal scoring (May 20 2026 refactor; dollar-override June 19 2026):
      Strong signals (any 1 confirms):
        - Dollar amount, penny-match, unique (no floor — retired June 19 2026)
        - Insurance agreement (both non-blank, normalize to same carrier)
      Weak signals (need 2+ to confirm):
        - Estimator first name match
        - Vehicle color exact match
        - Drop date within ±1 day
        - Promise/target date within ±1 day

      Single-candidate decision:
        - Strong signal CONTRADICTS (e.g. insurance disagrees) → ambiguous,
          UNLESS a penny-exact dollar also confirms → dollar overrides, matched
        - ≥1 strong signal confirms (no unresolved contradiction) → matched
        - 0 strong + ≥2 weak signals → matched
        - Otherwise → ambiguous (insufficient confirmation)

      Multi-candidate decision (2+ by name+vehicle):
        - Exactly 1 candidate has ≥1 strong confirmation that is either
          contradiction-free OR dollar-overridden → matched
          (if 2+ share a penny-dollar, uniqueness fails → ambiguous)
        - Exactly 1 candidate has ≥2 weak confirmations → matched
        - Otherwise → ambiguous with per-candidate signal breakdown

      Source-side duplicate handling:
        - If report has 2+ rows for same owner+vehicle, attempt dollar
          disambiguation: if this report row's dollar uniquely matches one
          SP candidate (and is unique among the duplicates) → matched.
          Otherwise → ambiguous.

    The match_type label flowing into the response encodes which signal(s)
    confirmed the match, e.g. 'customer_vehicle_dollar+insurance' or
    'customer_vehicle_weak_estimator+color'. This surfaces in the email
    matched table for audit visibility.

    Args:
        insurance_lookup: optional Insurance Lookup list for normalization.
            If None or empty, insurance check is skipped (pre-May-20 behavior).
    """
    if insurance_lookup is None:
        insurance_lookup = []

    # Pre-scan: count report rows by normalized owner+vehicle. Used to refuse
    # Path B matches when the source has duplicates (e.g., customer brought
    # car back for a second job — two CCC files, same customer, same vehicle).
    owner_vehicle_counts = {}
    for r in report_rows:
        no = normalize_owner(r.get('owner', '') or '').lower()
        nv = normalize_year_4to2(r.get('vehicle', '') or '').lower()
        if no and nv:
            key = (no, nv)
            owner_vehicle_counts[key] = owner_vehicle_counts.get(key, 0) + 1

    # Workfile_id index for Path A
    wf_index = {}
    for item in sharepoint_items:
        wf = (item.get('workfile_id') or '').strip()
        if wf:
            wf_index.setdefault(wf, []).append(item)

    provisional = []
    unmatched = []
    ambiguous = []

    for row in report_rows:
        ro_number = row['ro_number']
        report_wf = (row.get('workfile_id') or '').strip()
        report_ro = (ro_number or '').strip().lower()
        norm_owner = normalize_owner(row['owner']).lower()
        norm_vehicle = normalize_year_4to2(row['vehicle']).lower()
        report_estimator = row.get('estimator', '')

        # Path A: workfile_id (exact match in index)
        if report_wf and report_wf in wf_index:
            wf_candidates = wf_index[report_wf]
            if len(wf_candidates) == 1:
                provisional.append((row, wf_candidates[0], 'workfile_id'))
                continue
            ambiguous.append({
                'ro_number': ro_number,
                'owner': row['owner'],
                'vehicle': row['vehicle'],
                'candidate_ids': [c.get('id') for c in wf_candidates],
                'reason': 'multiple SharePoint items share this workfile_id'
            })
            continue

        # Path A.5: ro_number — scan for exact and compatible (suffix) matches.
        # Prefer exact over compatible. If only compatible matches exist, use those.
        if report_ro:
            exact_matches = []
            compat_matches = []
            for item in sharepoint_items:
                sp_ro = (item.get('ro_number') or '').strip().lower()
                mt = ro_match_type(sp_ro, report_ro)
                if mt == 'exact':
                    exact_matches.append(item)
                elif mt == 'compatible':
                    compat_matches.append(item)

            if exact_matches:
                if len(exact_matches) == 1:
                    provisional.append((row, exact_matches[0], 'ro_number'))
                    continue
                ambiguous.append({
                    'ro_number': ro_number,
                    'owner': row['owner'],
                    'vehicle': row['vehicle'],
                    'candidate_ids': [c.get('id') for c in exact_matches],
                    'reason': 'multiple SharePoint items share this RO#'
                })
                continue
            elif compat_matches:
                if len(compat_matches) == 1:
                    provisional.append((row, compat_matches[0], 'ro_number_compatible'))
                    continue
                ambiguous.append({
                    'ro_number': ro_number,
                    'owner': row['owner'],
                    'vehicle': row['vehicle'],
                    'candidate_ids': [c.get('id') for c in compat_matches],
                    'reason': 'multiple SharePoint items have suffix variants of this RO#'
                })
                continue

        # Path B: customer + vehicle prefix (with disqualifier + signal scoring)
        if not norm_owner or not norm_vehicle:
            unmatched.append({
                'ro_number': ro_number,
                'owner': row['owner'],
                'vehicle': row['vehicle'],
                'drop_date': row.get('drop_date', '') or row.get('vehicle_in', ''),
                'reason': 'report row missing customer or vehicle'
            })
            continue

        # Filter SP candidates by customer + vehicle prefix, applying disqualifiers
        candidates = []
        for item in sharepoint_items:
            item_customer = (item.get('customer_name') or '').lower()
            item_vehicle = normalize_year_4to2(item.get('vehicle') or '').lower()
            if not item_customer or not item_vehicle:
                continue

            # Disqualifier: skip SP rows already positively identified by
            # a contradicting workfile_id or ro_number. Prevents the magnet
            # effect where one SP row attracts many report rows via shared
            # customer + vehicle prefix (e.g., JLR dealer account).
            sp_wf = (item.get('workfile_id') or '').strip()
            if sp_wf and sp_wf != report_wf:
                continue
            sp_ro = (item.get('ro_number') or '').strip().lower()
            if sp_ro and not ro_compatible(sp_ro, report_ro):
                continue

            if item_customer == norm_owner and norm_vehicle.startswith(item_vehicle):
                candidates.append(item)

        # Source-side duplicate handling (May 20 2026 — refined design).
        # If this report has 2+ rows with the same owner+vehicle, we can't
        # immediately trust loose matches. New design: try dollar
        # disambiguation first. If a unique penny-match exists between this
        # specific report row and one SP candidate, allow it. Otherwise
        # fall through to ambiguous.
        is_source_duplicate = owner_vehicle_counts.get((norm_owner, norm_vehicle), 0) > 1

        if is_source_duplicate:
            # Try dollar disambiguation: does this report row uniquely penny-match
            # exactly one candidate? AND is the report row's dollar unique among
            # all source duplicates? Both conditions required to safely match.
            report_dollar = _parse_dollar(row.get('estimate_total'))
            if report_dollar is not None:
                # Check uniqueness among source duplicates
                duplicate_dollars = []
                for other in report_rows:
                    other_owner = normalize_owner(other.get('owner', '') or '').lower()
                    other_vehicle = normalize_year_4to2(other.get('vehicle', '') or '').lower()
                    if (other_owner, other_vehicle) == (norm_owner, norm_vehicle):
                        od = _parse_dollar(other.get('estimate_total'))
                        if od is not None:
                            duplicate_dollars.append(od)
                # This row's dollar must appear exactly once among source duplicates
                this_row_count = sum(1 for d in duplicate_dollars if abs(d - report_dollar) < 0.005)
                if this_row_count == 1:
                    # Now find candidates whose dollar penny-matches
                    dollar_matched = [c for c in candidates if dollars_penny_match(report_dollar, c.get('estimate_total'))]
                    if len(dollar_matched) == 1:
                        provisional.append((row, dollar_matched[0], f'customer_vehicle_dollar_source_disambig'))
                        continue

            # Dollar disambiguation failed (or unavailable). Before flagging as
            # ambiguous, two silent-suppression guards (Batch 1b — June 19 2026):
            #
            # (B) Zero-candidate guard: a source-duplicate report row with NO
            #     live SP candidates can't possibly be matched — flagging it
            #     "needs manual review" is pure noise. Route to unmatched,
            #     exactly like the standard no-candidate path does below.
            if len(candidates) == 0:
                unmatched.append({
                    'ro_number': ro_number,
                    'owner': row['owner'],
                    'vehicle': row['vehicle'],
                    'drop_date': row.get('drop_date', '') or row.get('vehicle_in', ''),
                    'reason': 'source-side duplicate with no matching SharePoint item (delivered/historical)'
                })
                continue

            # (A) Stale-delivered guard: if this car's vehicle_out is older than
            #     STALE_DELIVERED_DAYS and dollar couldn't uniquely match it to a
            #     live SP row, it's a delivered/historical car surfacing as noise.
            #     Suppress into unmatched. Only fires when dollar disambiguation
            #     already failed above, so it never hides a resolvable match.
            out_age = _vehicle_out_age_days(row.get('vehicle_out'))
            if out_age is not None and out_age > STALE_DELIVERED_DAYS:
                unmatched.append({
                    'ro_number': ro_number,
                    'owner': row['owner'],
                    'vehicle': row['vehicle'],
                    'drop_date': row.get('drop_date', '') or row.get('vehicle_in', ''),
                    'reason': f'source-side duplicate, delivered {out_age} days ago (stale, >{STALE_DELIVERED_DAYS}d)'
                })
                continue

            # Genuine ambiguous: candidates exist (or recent), but dollar
            # couldn't uniquely pick one. This is the real "please look at it" case.
            ambiguous.append({
                'ro_number': ro_number,
                'owner': row['owner'],
                'vehicle': row['vehicle'],
                'candidate_ids': [c.get('id') for c in candidates],
                'reason': f'source-side duplicate: this report has {owner_vehicle_counts[(norm_owner, norm_vehicle)]} rows with the same customer + vehicle, and dollar disambiguation could not uniquely identify a match.'
            })
            continue

        # Standard Path B (not a source-duplicate): score candidates by signals
        if len(candidates) == 0:
            unmatched.append({
                'ro_number': ro_number,
                'owner': row['owner'],
                'vehicle': row['vehicle'],
                'drop_date': row.get('drop_date', '') or row.get('vehicle_in', ''),
                'reason': 'no SharePoint item with matching customer + vehicle'
            })
            continue

        if len(candidates) == 1:
            # Single candidate path: need 1 strong OR 2 weak to confirm.
            # A strong contradiction (e.g. insurance disagrees) normally routes
            # to ambiguous — the Leedy-bug safeguard. EXCEPTION (June 19 2026):
            # a penny-exact dollar confirmation OVERRIDES an insurance
            # contradiction. Dollar is the harder-to-fake signal; the Leedy
            # cross-file pair had DIFFERENT dollars, so a real cross-file match
            # never penny-matches and stays correctly blocked. With one
            # candidate, uniqueness is trivially satisfied, so 'dollar' in
            # strong_confirmations is sufficient to trust the override.
            score = score_path_b_signals(row, candidates[0], insurance_lookup)

            dollar_overrides = 'dollar' in score['strong_confirmations']
            if score['has_contradiction'] and not dollar_overrides:
                # Insurance disagrees and no penny-dollar to vouch for it →
                # preserve Leedy protection, route to ambiguous.
                ambiguous.append({
                    'ro_number': ro_number,
                    'owner': row['owner'],
                    'vehicle': row['vehicle'],
                    'candidate_ids': [candidates[0].get('id')],
                    'reason': f'1 candidate but strong signal contradicts: {", ".join(score["strong_contradictions"])}. Likely cross-file match — verify before allowing.'
                })
                continue

            if score['total_strong'] >= 1 or score['total_weak'] >= 2:
                match_type = build_match_reason(score, base_type='customer_vehicle')
                provisional.append((row, candidates[0], match_type))
                continue

            # Insufficient confirmation: 1 candidate but no strong signals
            # and ≤1 weak signal. Send to ambiguous so estimator can verify.
            available = score['weak_confirmations']
            ambiguous.append({
                'ro_number': ro_number,
                'owner': row['owner'],
                'vehicle': row['vehicle'],
                'candidate_ids': [candidates[0].get('id')],
                'reason': f'1 candidate but insufficient confirmation (need 1 strong or 2 weak signals; got {len(available)} weak: {", ".join(available) or "none"}).'
            })
            continue

        # Multi-candidate path (2+ candidates by name + vehicle prefix):
        # Score every candidate. Use signals to narrow to exactly one.
        scored = [(c, score_path_b_signals(row, c, insurance_lookup)) for c in candidates]

        # Try strong signals first: candidates with ≥1 strong confirmation that
        # are either contradiction-free OR carry a penny-exact dollar override
        # (June 19 2026 — same dollar-beats-insurance rule as the single path).
        # Crucially, the `== 1` uniqueness check below still does the safety
        # work: if two candidates both penny-match the same dollar, BOTH become
        # winners, the count is 2, and we correctly fall through to ambiguous —
        # so a non-unique dollar can never force a match.
        strong_winners = [
            (c, s) for c, s in scored
            if s['total_strong'] >= 1
            and (not s['has_contradiction'] or 'dollar' in s['strong_confirmations'])
        ]
        if len(strong_winners) == 1:
            winner_candidate, winner_score = strong_winners[0]
            match_type = build_match_reason(winner_score, base_type='customer_vehicle')
            provisional.append((row, winner_candidate, match_type))
            continue
        # If multiple candidates have strong confirmations, the strong signal isn't
        # discriminating — fall through to weak signals (don't immediately ambiguous)

        # Try weak signals: candidates with ≥2 weak confirmations and no strong contradiction
        weak_winners = [(c, s) for c, s in scored if s['total_weak'] >= 2 and not s['has_contradiction']]
        if len(weak_winners) == 1:
            winner_candidate, winner_score = weak_winners[0]
            match_type = build_match_reason(winner_score, base_type='customer_vehicle')
            provisional.append((row, winner_candidate, match_type))
            continue

        # Could not narrow to one. Compute what we learned for the email reason.
        sig_summary = []
        for c, s in scored:
            cid = c.get('id')
            parts = []
            if s['strong_confirmations']:
                parts.append('strong: ' + '+'.join(s['strong_confirmations']))
            if s['weak_confirmations']:
                parts.append('weak: ' + '+'.join(s['weak_confirmations']))
            if s['strong_contradictions']:
                parts.append('contradicts: ' + '+'.join(s['strong_contradictions']))
            sig_summary.append(f'ID {cid} ({"; ".join(parts) or "no signals"})')

        ambiguous.append({
            'ro_number': ro_number,
            'owner': row['owner'],
            'vehicle': row['vehicle'],
            'candidate_ids': [c.get('id') for c in candidates],
            'reason': f'{len(candidates)} candidates by customer + vehicle prefix; signals did not uniquely identify one. Per-candidate signals: {" | ".join(sig_summary)}'
        })

    # Duplicate SP detection: collapse SP items that match multiple report rows
    sp_id_to_rows = {}
    for row, sp, mtype in provisional:
        sp_id_to_rows.setdefault(sp.get('id'), []).append((row, sp, mtype))

    matched_pairs = []
    for sp_id, hits in sp_id_to_rows.items():
        if len(hits) == 1:
            matched_pairs.append(hits[0])
        else:
            for row, sp, mtype in hits:
                ambiguous.append({
                    'ro_number': row['ro_number'],
                    'owner': row['owner'],
                    'vehicle': row['vehicle'],
                    'candidate_ids': [sp.get('id')],
                    'reason': f"multiple report rows ({len(hits)}) matched the same SharePoint item — manual review needed"
                })

    return matched_pairs, unmatched, ambiguous

# ─── Phase 4: Cancelled Opportunities Cleanup ─────────────────────

def parse_opportunities_xml(xml_bytes):
    """Parse CCC ONE native XML 'Opportunities' export.

    Returns ALL rows (caller filters via is_cancelled_opportunity).
    Note: this report uses <work_files> elements, not <repairOrder>.
    """
    root = ET.fromstring(xml_bytes)
    results = []
    for o in root.findall('.//work_files'):
        results.append({
            'workfile_id':         _xml_text(o, 'workfile_id'),
            'ro_number':           _xml_text(o, 'repair_order_number'),
            'owner':               _xml_text(o, 'owner_name'),
            'vehicle':             _xml_text(o, 'vehicle_year_make_model'),
            'estimator':           _xml_text(o, 'service_writer_display_name'),
            'cancel_date':         _xml_text(o, 'cancel_date'),
            'cancel_reason':       _xml_text(o, 'cancel_reason_name'),  # display only
            'workfile_status':     _xml_text(o, 'workfile_status'),
            'converted_datetime':  _xml_text(o, 'converted_datetime'),
            'visit_stage_id':      _xml_text(o, 'customer_visit_stage_id'),
            # Path B signal fields (May 20 2026 — dollar verification only;
            # Opportunities XML lacks color, drop_date, promise_date)
            'estimate_total':      _xml_text(o, 'opportunity_amount'),
        })
    return results

def is_cancelled_opportunity(row):
    """Filter: row represents a workfile that was EMS-exported but never
    became an active RO, and is now safe to delete from DTBS.

    Criteria (Batch 2 — June 29 2026):
      - converted_datetime is blank (never became an RO), AND
      - cancel_reason is populated (the reliable cancellation signal)

    Rationale for the June 29 rewrite:
      Previous logic deleted on `cancel_date` populated OR
      `workfile_status == 'Closed'`. The Closed branch was a real bug:
      Closed-and-billed files (normal lifecycle end-state) would hit the
      delete bucket even though they're the OPPOSITE of dead — they're
      completed work that must be KEPT. `cancel_reason_name` (stored under
      key `cancel_reason` here per parser convention) is in fact the
      reliable signal: estimators populate it on actual cancellation, and
      it doesn't appear spuriously on non-cancelled files. Date alone was
      a weaker proxy than the reason field. See Lesson 138.
    """
    if row.get('converted_datetime'):
        return False  # became an RO at some point — leave alone
    if (row.get('cancel_reason') or '').strip():
        return True   # actually cancelled
    return False      # still open or in some other terminal state — leave alone

def _vehicle_token_match(opp_vehicle, sp_vehicle):
    """Conservative vehicle-agreement check for cancelled-opp delete guard.

    Returns (is_match, reason). True only when Year, Make, and the FIRST model
    token all agree between the matched Opp row and the SP row. Used by
    cancelled_opp_safety_guards (Batch 2 — June 29 2026) to plug the
    "dealer hole": a cancelled Opp for one car must NOT trigger deletion of
    an SP row for a DIFFERENT same-customer car (e.g. JLR has many vehicles
    under one customer name; cancelling one must not delete another).

    Examples (both inputs as CCC writes them, e.g. "2018 NISS Pathfinder SL 4WD"):
      ("2018 NISS Pathfinder SL 4WD", "2018 NISS Pathfinder SL 4WD") → True
      ("2018 NISS Pathfinder SL",     "2018 NISS Leaf SL")           → False (model)
      ("2018 NISS Pathfinder",        "2019 NISS Pathfinder")        → False (year)
      ("2018 NISS Pathfinder",        "2018 TOYO Pathfinder")        → False (make)
      ("",                            "2018 NISS Pathfinder")        → False (blank either side → unsafe)
    """
    opp = (opp_vehicle or '').strip()
    sp = (sp_vehicle or '').strip()
    if not opp or not sp:
        return False, 'vehicle blank on Opp or SP side — cannot verify same car'
    opp_tokens = opp.split()
    sp_tokens = sp.split()
    if len(opp_tokens) < 3 or len(sp_tokens) < 3:
        return False, f'vehicle string too short to verify (Opp="{opp}", SP="{sp}")'
    # Year + Make + first model token — case-insensitive
    o_year, o_make, o_model = opp_tokens[0].lower(), opp_tokens[1].lower(), opp_tokens[2].lower()
    s_year, s_make, s_model = sp_tokens[0].lower(), sp_tokens[1].lower(), sp_tokens[2].lower()
    if (o_year, o_make, o_model) == (s_year, s_make, s_model):
        return True, ''
    return False, f'vehicle mismatch — Opp="{opp_tokens[0]} {opp_tokens[1]} {opp_tokens[2]}" vs SP="{sp_tokens[0]} {sp_tokens[1]} {sp_tokens[2]}"'


def cancelled_opp_safety_guards(sp_item, opp_row=None):
    """Check if a matched SP row has any indicators of human/sync activity
    that should prevent auto-deletion. Returns (is_safe, reason).

    Failed guards route the match to ambiguous instead of delete:
    - ro_number (Title) populated — sync or human claimed the row
    - Tech, Painter, ProductionNotes, PartsNotes, PartsStatus populated
    - RepairStatus is anything other than blank or 'Prelim'
    - (Batch 2, June 29 2026) Vehicle disagreement between Opp row and SP row —
      Year + Make + first-model-token must match. Plugs the "dealer hole" where
      a cancelled Opp for one car would otherwise trigger deletion of a
      same-customer SP row for a DIFFERENT car. Only enforced when opp_row is
      provided (callers must pass it; the parameter defaults to None for
      backward compatibility, but a None opp_row SKIPS the vehicle check —
      which is the legacy behavior, not the safe behavior, so all callers
      should pass opp_row going forward).

    SP item dict keys expected: ro_number, tech, painter, production_notes,
    parts_notes, parts_status, repair_status, vehicle. Opp row needs: vehicle.
    """
    ro_number = (sp_item.get('ro_number') or '').strip()
    if ro_number:
        return False, f"SP row has RO# '{ro_number}' — manual review"

    if (sp_item.get('tech') or '').strip():
        return False, "SP row has Tech assigned — manual review"
    if (sp_item.get('painter') or '').strip():
        return False, "SP row has Painter assigned — manual review"
    if (sp_item.get('production_notes') or '').strip():
        return False, "SP row has Production Notes — manual review"
    if (sp_item.get('parts_notes') or '').strip():
        return False, "SP row has Parts Notes — manual review"
    if (sp_item.get('parts_status') or '').strip():
        return False, "SP row has Parts Status — manual review"

    repair_status = (sp_item.get('repair_status') or '').strip()
    if repair_status and repair_status != 'Prelim':
        return False, f"SP row RepairStatus is '{repair_status}' — manual review"

    # Vehicle agreement guard — Batch 2 (June 29 2026). See Lesson 139.
    if opp_row is not None:
        veh_ok, veh_reason = _vehicle_token_match(opp_row.get('vehicle'), sp_item.get('vehicle'))
        if not veh_ok:
            return False, veh_reason

    return True, ''

# ─── /parse endpoint ──────────────────────────────────────────────

@app.route('/parse', methods=['POST'])
def parse():
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No JSON body received'}), 400

    with tempfile.TemporaryDirectory() as tmpdir:
        parsed = {}
        for ext in ['env', 'ad1', 'ad2', 'veh', 'ttl', 'stl']:
            if ext in data:
                try:
                    file_bytes = base64.b64decode(data[ext])
                    path = os.path.join(tmpdir, f'file.{ext}')
                    with open(path, 'wb') as f:
                        f.write(file_bytes)
                    parsed[ext] = read_dbf(path)
                except Exception as e:
                    parsed[ext] = []
            else:
                parsed[ext] = []

        env = parsed['env']
        ad1 = parsed['ad1']
        ad2 = parsed['ad2']
        veh = parsed['veh']
        ttl = parsed['ttl']
        stl = parsed['stl']

        # Sync race detection: if env or ad1 came back empty (file missing,
        # not yet synced from OneDrive, or unparseable), return 503 so PA's
        # retry policy fires. Without this check, /parse returns 200 with
        # blank fields and SP silently gets a half-empty row. The env file
        # holds the unique_id and the ad1 holds customer/insurance data —
        # if either is missing we can't usefully populate the row.
        #
        # Other files (ad2/veh/ttl/stl) are nice-to-have but not essential
        # enough to warrant a retry. ad2 has estimator/dates, veh has
        # vehicle string, ttl/stl have $ and hours. Missing those is
        # uncommon and PA's update branch null-guards will preserve
        # existing values on the next supplement.
        if not env or not ad1:
            missing = []
            if not env:
                missing.append('env')
            if not ad1:
                missing.append('ad1')
            return jsonify({
                'error': f"Essential EMS file(s) empty or missing: {', '.join(missing)}. Likely OneDrive sync race — PA should retry."
            }), 503

        unique_id = get_val(env, 'UNQFILE_ID')
        supp_no = get_val(env, 'SUPP_NO')
        trans_type = get_val(env, 'TRANS_TYPE')

        ins_raw = get_val(ad1, 'INS_CO_NM')
        claim_no = get_val(ad1, 'CLM_NO')
        policy_no = get_val(ad1, 'POLICY_NO')
        ded_amt = get_val(ad1, 'DED_AMT')
        loss_date = get_val(ad1, 'LOSS_DATE')
# ── Customer name: VEHICLE OWNER always wins ──────────────────
        # Shop rule (confirmed): the name on the tracker is always the
        # vehicle owner — the person who drops the car off and picks it up.
        # OWNR_ is preferred whenever it is populated; INSD_ is only a
        # fallback for the rare file where OWNR_ is entirely blank.
        #
        # This deliberately RETIRES the old CUST_PR branching. CUST_PR was
        # used to decide owner-vs-insured (P=policyholder->INSD, C=claimant
        # ->OWNR), but CCC emits other values too (e.g. 'I' seen on dealer
        # files) that fell through to the wrong default. "Owner first" needs
        # no knowledge of the flag and is immune to undocumented values.
        # Third-party claims (Schiffman/Kalden) still resolve correctly
        # because the Ken Garff customer is the OWNER in those files.
        #
        # "Last, First" format matches how shop staff reference cars and how
        # CCC ONE reports come in for human customers. Dealer/company names
        # (no first/last) pass through unchanged.
        cust_first = get_val(ad1, 'INSD_FN')
        cust_last = get_val(ad1, 'INSD_LN')
        cust_co = get_val(ad1, 'INSD_CO_NM')
        ownr_first = get_val(ad1, 'OWNR_FN')
        ownr_last = get_val(ad1, 'OWNR_LN')
        ownr_co = get_val(ad1, 'OWNR_CO_NM')

        if ownr_first or ownr_last:
            # Owner is a person — preferred.
            customer_name = f"{ownr_last}, {ownr_first}".strip(', ').strip()
        elif ownr_co:
            # Owner is a company (dealer work, fleet, etc.).
            customer_name = ownr_co
        elif cust_first or cust_last:
            # Fallback: no owner on file, insured is a person.
            customer_name = f"{cust_last}, {cust_first}".strip(', ').strip()
        else:
            # Last resort: insured company name (or blank if truly empty).
            customer_name = cust_co

        est_first = get_val(ad2, 'EST_CT_FN')
        est_last = get_val(ad2, 'EST_CT_LN')
        estimator = est_first.strip() if est_first else est_last.strip()
        drop_date = get_val(ad2, 'RO_IN_DATE')
        promise_date = get_val(ad2, 'TAR_DATE')

        year = get_val(veh, 'V_MODEL_YR')
        make_code = get_val(veh, 'V_MAKECODE')
        model_full = get_val(veh, 'V_MODEL')
        # Take first 3 model words to give Path B's prefix matcher more specificity.
        # Reduces magnet effect on dealer accounts (JLR, Hyundai Southtowne, etc.)
        # where many ROs share the same year + make + base model. Short model names
        # (e.g., 'Civic') stay short — Python slicing past the end is a no-op.
        model_short = ' '.join(model_full.split()[:3]) if model_full else ''
        color = get_val(veh, 'V_COLOR')
        vin = get_val(veh, 'V_VIN')

        vehicle = f"{year} {make_code} {model_short}".strip()

        estimate_total = get_val(ttl, 'G_TTL_AMT')

        body_hrs = '0'
        paint_hrs = '0'
        other_hrs = 0
        total_hrs = '0'

        other_types = ['LAM', 'LAG', 'LA1', 'LA2', 'LA3', 'LA4', 'LAU']

        for r in stl:
            tc = str(r.get('TTL_TYPECD', '')).strip()
            hrs = r.get('T_HRS', 0) or 0
            if tc == 'LAT':
                total_hrs = str(hrs)
            elif tc == 'LAB':
                body_hrs = str(hrs)
            elif tc == 'LAR':
                paint_hrs = str(hrs)
            elif tc in other_types:
                other_hrs += float(hrs)

        # EMS gate (June 9, 2026) — workfiles exported before the VIN scan was
        # performed produce garbage SP rows: customer name with no vehicle and
        # no VIN. Estimator manually picks vehicle from a dropdown at intake,
        # then a VIN scan later decodes year/make/model/submodel. EMS exports
        # happening between intake and scan produce incomplete workfiles.
        # We surface an 'incomplete: true' flag so Flow 6 can skip SP create.
        # Once a later supplement adds VIN, that export triggers SP creation.
        incomplete = not (vin or '').strip()
        incomplete_reason = 'missing_vin' if incomplete else ''

        result = {
            'unique_id': unique_id,
            'supp_no': supp_no,
            'trans_type': trans_type,
            'customer_name': customer_name,
            'estimator': estimator,
            'vehicle': vehicle,
            'color': color,
            'vin': vin,
            'insurance_raw': ins_raw,
            'claim_no': claim_no,
            'policy_no': policy_no,
            'deductible': ded_amt,
            'loss_date': loss_date,
            'drop_date': drop_date,
            'promise_date': promise_date,
            'estimate_total': estimate_total,
            'total_hrs': total_hrs,
            'body_hrs': body_hrs,
            'paint_hrs': paint_hrs,
            'other_hrs': str(other_hrs),
            'incomplete': incomplete,
            'incomplete_reason': incomplete_reason
        }

        return jsonify(result)

# ─── /match-ro-report endpoint (Phase 2 — RO Sync / bulk historical fill) ──

@app.route('/match-ro-report', methods=['POST'])
def match_ro_report():
    """Phase 2 — RO Report Sync.

    Matches CCC ONE 'Repair Orders Created' report rows against open SP items.
    Used by Flow 10 (DTBS RO Report Sync) for bulk historical fill — writes
    RO#, WorkfileID, vehicle out datetime, total loss flag, etc.

    Refactored to use shared run_match_engine. Same response shape as before
    (matched / unmatched_report_rows / ambiguous / summary). PA flow needs
    no changes to keep working.

    Phase 2 specifics:
    - insurance_needs_fix uses the narrower insurance_needs_correction()
      helper (⚠️-only). Phase 2's PA flow only overwrites Insurance when
      the SP value starts with ⚠️, per the existing field map.
    - Path A.5 (ro_number) won't fire until Flow 10's Project SP Items
      Select action is updated to include ro_number in its output. Until
      then, RO Sync benefits from the workfile_id disqualifier (the more
      important of the two — workfile_id is unique) but not the ro_number
      disqualifier or the ro_number match path itself. Pending PA edit.

    Phase 6 (insurance normalization, May 8 evening):
    - Accepts optional `insurance_lookup` array (list of {Title, DisplayName}
      dicts) in the request body. If present, each matched row's
      `normalized_insurance` field returns the lookup's DisplayName when the
      CCC carrier_name matches a Title (case-insensitive), or `⚠️ <raw name>`
      when no match. If absent or empty, `normalized_insurance` equals the
      raw carrier_name (Option B — preserves pre-Phase-6 behavior).
    """
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No JSON body received'}), 400
    if 'xml' not in data:
        return jsonify({'error': 'Missing xml field (base64 of report XML)'}), 400

    sharepoint_items, err = coerce_sharepoint_items(data.get('sharepoint_items'))
    if err:
        return jsonify({'error': err}), 400

    # Phase 6: optional insurance_lookup from the caller. Empty/missing → fallback.
    insurance_lookup, err = coerce_sharepoint_items(data.get('insurance_lookup'))
    if err:
        return jsonify({'error': f'insurance_lookup parse error: {err}'}), 400

    try:
        xml_bytes = base64.b64decode(data['xml'])
        report_rows = parse_ro_report_xml(xml_bytes)
    except Exception as e:
        return jsonify({'error': f'Failed to parse XML report: {str(e)}'}), 400

    matched_pairs, unmatched, ambiguous = run_match_engine(report_rows, sharepoint_items, insurance_lookup)

    matched = []
    for row, sp, mtype in matched_pairs:
        sp_insurance_now = sp.get('insurance', '') or ''
        raw_carrier = row.get('insurance_company', '')
        matched.append({
            'list_item_id':           sp.get('id'),
            'ro_number':              row['ro_number'],
            'workfile_id':            row.get('workfile_id', ''),
            'customer_name':          sp.get('customer_name'),
            'vehicle':                sp.get('vehicle'),
            'match_type':             mtype,
            # Phase 2 source fields
            'vehicle_out_datetime':   row.get('vehicle_out', ''),
            'ro_status':              row.get('ro_status', ''),
            'is_completed':           row.get('ro_status', '').strip().lower() == 'completed',
            'is_total_loss':          row.get('total_loss', False),
            'carrier_name':           raw_carrier,
            'normalized_insurance':   normalize_insurance_name(raw_carrier, insurance_lookup),
            'estimator_first_name':   estimator_first_name(row.get('estimator', '')),
            'insurance_needs_fix':    insurance_needs_correction(sp_insurance_now),
        })

    # Compute changes per matched row (May 18 — email visibility feature).
    # Encodes the same logic the PA flow uses to decide what to write per field.
    for m in matched:
        sp = next((s for s in sharepoint_items if s.get('id') == m['list_item_id']), {})
        vehicle_out = m.get('vehicle_out_datetime', '')
        is_completed = m.get('is_completed', False)
        # Build dict of values the flow would write to SP for this row.
        new_values = {
            'ro_number':       m.get('ro_number', ''),
            'workfile_id':     m.get('workfile_id', ''),
           # CCCPromisDate: write Vehicle Out only when CCC has it; else preserve SP
            'cccpromisdate':   vehicle_out if vehicle_out else sp.get('cccpromisdate', ''),
            # actual_delivery and done REMOVED July 2 2026 — the RO Bulk
            # report carries no is_delivered element and blank file_status
            # on delivered-not-closed cars, so the flow was wiping delivery
            # dates and stomping Done=False. Flow 10's Update item no longer
            # writes either field (Cleanup owns delivery; Production owns
            # Done), so diffing them here would report changes nobody makes.
            'total_loss':      m.get('is_total_loss', False),
            'insurance':       m.get('normalized_insurance', ''),
            'estimator':       m.get('estimator_first_name', ''),
        }
        m['changes'] = compute_changes(sp, new_values, RO_SYNC_DIFF_FIELDS)
        m['changes_text'] = format_changes_text(m['changes'])

    return jsonify({
        'matched': matched,
        'unmatched_report_rows': unmatched,
        'ambiguous': ambiguous,
        'summary': {
            'report_rows_total': len(report_rows),
            'sharepoint_items': len(sharepoint_items),
            'insurance_lookup_entries': len(insurance_lookup),
            'matched': len(matched),
            'unmatched': len(unmatched),
            'ambiguous': len(ambiguous)
        }
    })

# ─── /match-production-schedule endpoint (Phase 3 — Production Sync) ──

@app.route('/match-production-schedule', methods=['POST'])
def match_production_schedule():
    """Phase 3 — Production Sync.
    Matches Production Schedule report rows against open SP items.
    Writes phase, tech, painter, dates. Never writes Done/ActualDelivery.

    Phase 6 (insurance normalization, May 8 evening):
    - Accepts optional `insurance_lookup` array; populates `normalized_insurance`
      on each matched row. See /match-ro-report docstring for details.
    """
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No JSON body received'}), 400
    if 'xml' not in data:
        return jsonify({'error': 'Missing xml field (base64 of report XML)'}), 400

    sharepoint_items, err = coerce_sharepoint_items(data.get('sharepoint_items'))
    if err:
        return jsonify({'error': err}), 400

    insurance_lookup, err = coerce_sharepoint_items(data.get('insurance_lookup'))
    if err:
        return jsonify({'error': f'insurance_lookup parse error: {err}'}), 400

    try:
        xml_bytes = base64.b64decode(data['xml'])
        report_rows = parse_production_schedule_xml(xml_bytes)
    except Exception as e:
        return jsonify({'error': f'Failed to parse XML report: {str(e)}'}), 400

    matched_pairs, unmatched, ambiguous = run_match_engine(report_rows, sharepoint_items, insurance_lookup)

    matched = []
    for row, sp, mtype in matched_pairs:
        sp_insurance_now = sp.get('insurance', '') or ''
        sp_status_now = sp.get('repair_status', '') or ''

        # Repair Status decision (rank-based)
        new_status = map_phase_to_status(row.get('repair_phase', ''))
        write_status = should_write_status(new_status, sp_status_now)

        # Tech & Painter mapping (skip if no mapping)
        # Tech uses priority-ordered select_tech (Batch 4, June 29 2026) —
        # see select_tech docstring for the four-step rule.
        new_tech = select_tech(row)
        new_painter = map_painter(row.get('paint_tech', ''))

        raw_carrier = row.get('insurance_company', '')

        matched.append({
            'list_item_id':           sp.get('id'),
            'ro_number':              row['ro_number'],
            'workfile_id':            row.get('workfile_id', ''),
            'customer_name':          sp.get('customer_name'),
            'vehicle':                sp.get('vehicle'),
            'match_type':             mtype,
            # Always-overwrite source fields
            'vehicle_out_datetime':   row.get('vehicle_out', ''),
            'vehicle_in_datetime':    row.get('vehicle_in', ''),
            'repair_phase_raw':       row.get('repair_phase', ''),
            'is_total_loss':          row.get('total_loss', False),
            'carrier_name':           raw_carrier,
            'normalized_insurance':   normalize_insurance_name(raw_carrier, insurance_lookup),
            'estimator_first_name':   estimator_first_name(row.get('estimator', '')),
            # Conditional fields — caller checks the should_write flag
            'new_repair_status':      new_status or '',
            'should_write_status':    write_status,
            'new_tech':               new_tech,
            'new_painter':            new_painter,
            'insurance_needs_fix':    insurance_needs_fix_or_blank(sp_insurance_now),
            # Production metrics — written to SP as of June 4, 2026
            # (except days_in_shop which is computed on display side from DropDate)
            'days_in_shop':           row.get('days_in_shop', ''),
            'parts_received_pct':     row.get('parts_received_pct', ''),
            'labor_assigned_pct':     row.get('labor_assigned_pct', ''),
            'repair_plan_comments':   row.get('repair_plan_comments', ''),
            # Done semantics — Batch 4 (June 29 2026 / Item 4)
            # Production Sync owns the Done milestone now: True when the
            # current repair phase is one of PRODUCTION_DONE_PHASES, False
            # otherwise. CCC removes closed files from this XML entirely
            # (delivered/closed files fall off the "actively in the shop"
            # report), so Production Sync can never wrongly un-Done a
            # closed row — those rows simply aren't in our input. Cleanup
            # Sync owns Closed and also stamps Done=True alongside it.
            # No explicit Closed-pin guard is needed for that reason.
            'is_production_done':     row.get('repair_phase', '').strip() in PRODUCTION_DONE_PHASES,
            # Real completion timestamp from CCC — stamped to DoneStatusTime
            # when Done flips True, instead of utcNow(). Avoids a fake MTD
            # spike when this change first deploys and ~35 already-completed
            # rows flip to Done at once (their real completion dates fall
            # in prior months, so MTD reflects truth, not deploy-day). See
            # Lesson 108 for why this field is a reliable real timestamp.
            'repair_completed_datetime': row.get('repair_completed_datetime', ''),
        })

    # Compute changes per matched row (May 18 — email visibility feature).
    for m in matched:
        sp = next((s for s in sharepoint_items if s.get('id') == m['list_item_id']), {})
        vehicle_out = m.get('vehicle_out_datetime', '')
        vehicle_in = m.get('vehicle_in_datetime', '')
        # Batch 4 (June 29 2026) — Done logic:
        # is_production_done is True/False based on the current CCC phase.
        # If it's True, also stamp DoneStatusTime from repair_completed_datetime
        # (the real human-entered completion timestamp, NOT utcNow). If False,
        # the field write is None (PA flow skips writes when the new value is
        # None or unchanged), so Done flips back to False on the SP row by
        # virtue of the diff engine — supplement-rework / drop-on-return case.
        is_done = m.get('is_production_done', False)
        real_completion = m.get('repair_completed_datetime', '')
        done_status_time = real_completion if is_done and real_completion else None
        new_values = {
            'ro_number':       m.get('ro_number', ''),
            'workfile_id':     m.get('workfile_id', ''),
            'cccpromisdate':   vehicle_out if vehicle_out else sp.get('cccpromisdate', ''),
            'drop_date':       vehicle_in if vehicle_in else sp.get('drop_date', ''),
            # RepairStatus only writes when should_write_status is True
            'repair_status':   m.get('new_repair_status', '') if m.get('should_write_status') else sp.get('repair_status', ''),
            'tech':            m.get('new_tech', '') if m.get('new_tech', '') else sp.get('tech', ''),
            'painter':         m.get('new_painter', '') if m.get('new_painter', '') else sp.get('painter', ''),
            'total_loss':      m.get('is_total_loss', False),
            'insurance':       m.get('normalized_insurance', ''),
            'estimator':       m.get('estimator_first_name', ''),
            # Production metrics — read-only mirror from CCC (June 4, 2026)
            'parts_received_pct':   m.get('parts_received_pct', ''),
            'labor_assigned_pct':   m.get('labor_assigned_pct', ''),
            'repair_plan_comments': m.get('repair_plan_comments', ''),
            # Done semantics (Batch 4, June 29 2026)
            'done':            is_done,
            'donestatustime':  done_status_time,
        }
        m['changes'] = compute_changes(sp, new_values, PRODUCTION_SYNC_DIFF_FIELDS)
        m['changes_text'] = format_changes_text(m['changes'])
        m['has_real_changes'] = len(m['changes']) > 0

    # Compute report-row age for unmatched (helps the email bucket old vs recent).
    today = datetime.utcnow().date()
    for u in unmatched:
        drop_raw = u.get('drop_date', '')
        try:
            drop_date = datetime.fromisoformat(drop_raw.replace('Z', '').split('.')[0]).date() if drop_raw else None
        except Exception:
            drop_date = None
        u['age_days'] = (today - drop_date).days if drop_date else None

    # Stale tracker detection: SP items that aren't in any matched pair
    matched_sp_ids = {p[1].get('id') for p in matched_pairs}
    today = datetime.utcnow().date()
    stale_sp_rows = []
    for item in sharepoint_items:
        if item.get('id') not in matched_sp_ids:
            created_raw = item.get('created') or item.get('Created') or ''
            try:
                created_date = datetime.fromisoformat(created_raw.replace('Z', '').split('.')[0]).date() if created_raw else None
            except Exception:
                created_date = None
            age_days = (today - created_date).days if created_date else None
            stale_sp_rows.append({
                'list_item_id': item.get('id'),
                'customer_name': item.get('customer_name', ''),
                'vehicle': item.get('vehicle', ''),
                'workfile_id': item.get('workfile_id', ''),
                'ro_number': item.get('ro_number', ''),
                'created_date': created_raw,
                'age_days': age_days,
            })

    return jsonify({
        'matched': matched,
        'unmatched_report_rows': unmatched,
        'ambiguous': ambiguous,
        'stale_sp_rows': stale_sp_rows,
        'summary': {
            'report_rows_total': len(report_rows),
            'sharepoint_items': len(sharepoint_items),
            'insurance_lookup_entries': len(insurance_lookup),
            'matched': len(matched),
            'unmatched': len(unmatched),
            'ambiguous': len(ambiguous),
            'stale': len(stale_sp_rows),
        }
    })

# ─── /match-vehicles-scheduled-out endpoint (Phase 3 — Cleanup Sync) ──

@app.route('/match-vehicles-scheduled-out', methods=['POST'])
def match_vehicles_scheduled_out():
    """Phase 3 — Cleanup Sync.
    Matches Vehicles Scheduled Out report rows against open SP items.
    Writes Done/ActualDelivery for delivered vehicles. Final field sync.

    Phase 6 (insurance normalization, May 8 evening):
    - Accepts optional `insurance_lookup` array; populates `normalized_insurance`
      on each matched row. See /match-ro-report docstring for details.
    """
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No JSON body received'}), 400
    if 'xml' not in data:
        return jsonify({'error': 'Missing xml field (base64 of report XML)'}), 400

    sharepoint_items, err = coerce_sharepoint_items(data.get('sharepoint_items'))
    if err:
        return jsonify({'error': err}), 400

    insurance_lookup, err = coerce_sharepoint_items(data.get('insurance_lookup'))
    if err:
        return jsonify({'error': f'insurance_lookup parse error: {err}'}), 400

    try:
        xml_bytes = base64.b64decode(data['xml'])
        report_rows = parse_vehicles_scheduled_out_xml(xml_bytes)
    except Exception as e:
        return jsonify({'error': f'Failed to parse XML report: {str(e)}'}), 400

    matched_pairs, unmatched, ambiguous = run_match_engine(report_rows, sharepoint_items, insurance_lookup)

    matched = []
    for row, sp, mtype in matched_pairs:
        sp_insurance_now = sp.get('insurance', '') or ''
        is_delivered = row.get('is_delivered', False)
        # Batch 4 / Item 4 (June 29 2026) — Done/Closed split:
        # Cleanup Sync now owns the Closed milestone exclusively (delivery /
        # file closed in CCC). Done was previously conflated here with
        # delivery (`is_delivered or is_closed`), which fired Done on every
        # delivered car regardless of whether production was actually
        # finished. Done is now PRIMARILY driven by Production Sync (phase-
        # based). Cleanup Sync still sets Done=True alongside Closed=True
        # as a guarantee — a delivered/closed file must show Done=True
        # regardless of what phase it was on when production stopped seeing
        # it. Neither flow writes Done=False from this side; the only
        # source of Done=False is Production Sync flipping on a production
        # phase (drop-on-return for supplement-rework cases).
        is_closed = row.get('file_status', '').strip().lower() == 'closed'

        # July 2 2026 — monotonic Done, enforced server-side. The PA flow's
        # Update item writes if(should_set_done, true, false) — a hard write
        # both directions — so should_set_done=False on a delivered-not-yet-
        # closed car was stomping Done=False (and Flow 13 then cleared
        # DoneStatusTime). Fix: never send False when SP already has True.
        # PA may serialize the SP Yes/No as Python True/False or as
        # 'true'/'false' strings (same caveat as format_value_for_diff).
        sp_done_raw = sp.get('done')
        sp_done = sp_done_raw is True or (
            isinstance(sp_done_raw, str) and sp_done_raw.lower() in ('true', 'yes', '1'))

        raw_carrier = row.get('insurance_company', '')

        matched.append({
            'list_item_id':           sp.get('id'),
            'ro_number':              row['ro_number'],
            'workfile_id':            row.get('workfile_id', ''),
            'customer_name':          sp.get('customer_name'),
            'vehicle':                sp.get('vehicle'),
            'match_type':             mtype,
            # Always-write source fields
            'vehicle_out_datetime':   row.get('vehicle_out', ''),
            'is_delivered':           is_delivered,
            'is_closed':              is_closed,
            # Cleanup Sync writes Done=True whenever the file is Closed,
            # and PRESERVES an existing Done=True otherwise (July 2 2026).
            # The flow hard-writes if(should_set_done, true, false), so
            # monotonicity must be computed here, not assumed there.
            # The Done=False side is owned by Production Sync (Batch 4).
            'should_set_done':        is_closed or sp_done,
            'is_total_loss':          row.get('total_loss', False),
            'carrier_name':           raw_carrier,
            'normalized_insurance':   normalize_insurance_name(raw_carrier, insurance_lookup),
            'estimator_first_name':   estimator_first_name(row.get('estimator', '')),
            'file_status_raw':        row.get('file_status', ''),
            # Conditional fields
            'insurance_needs_fix':    insurance_needs_fix_or_blank(sp_insurance_now),
        })

    # Compute changes per matched row (May 18 — email visibility feature).
    for m in matched:
        sp = next((s for s in sharepoint_items if s.get('id') == m['list_item_id']), {})
        vehicle_out = m.get('vehicle_out_datetime', '')
        is_delivered_now = m.get('is_delivered', False)
        new_values = {
            'ro_number':       m.get('ro_number', ''),
            'workfile_id':     m.get('workfile_id', ''),
            'cccpromisdate':   vehicle_out if vehicle_out else sp.get('cccpromisdate', ''),
            # Done from Cleanup Sync is monotonic True-only (Batch 4):
            # set True alongside Closed=True; never sets False. Production
            # Sync owns the False side via its phase-driven logic.
            'done':            m.get('should_set_done', False),
            'closed':          m.get('is_closed', False),
            'actual_delivery': vehicle_out if is_delivered_now and vehicle_out else None,
            'total_loss':      m.get('is_total_loss', False),
            'insurance':       m.get('normalized_insurance', ''),
            'estimator':       m.get('estimator_first_name', ''),
        }
        m['changes'] = compute_changes(sp, new_values, CLEANUP_SYNC_DIFF_FIELDS)
        m['changes_text'] = format_changes_text(m['changes'])

    return jsonify({
        'matched': matched,
        'unmatched_report_rows': unmatched,
        'ambiguous': ambiguous,
        'summary': {
            'report_rows_total': len(report_rows),
            'sharepoint_items': len(sharepoint_items),
            'insurance_lookup_entries': len(insurance_lookup),
            'matched': len(matched),
            'unmatched': len(unmatched),
            'ambiguous': len(ambiguous),
        }
    })

# ─── /match-closed-report endpoint (Phase 7 — Closed Report Sync) ─

@app.route('/match-closed-report', methods=['POST'])
def match_closed_report():
    """Phase 7 (June 30 2026) — Closed Report Sync (Flow 10e).

    Authoritative source for Closed=Yes and ClosedStatusTime. Replaces:
    - Flow 13b (stamped ClosedStatusTime with utcNow() on Closed flip;
      broke silently on bad triggerBody expressions; even when working
      drifted on backdated closes).
    - Flow 14c (one-time straggler-fill tool that read the same Closed
      report this endpoint now reads natively).

    Why this report and not the Cleanup Sync report:
    The Cleanup Sync report (`/match-vehicles-scheduled-out`) carries
    `file_status_name` and `vehicle_out_datetime` but not `closed_date`.
    Closes that happen without a fresh delivery stamp (total losses,
    customer pickups not entered as deliveries) don't reliably surface
    there. The Repair Orders Closed report is delivered-state-agnostic
    and carries the real `closed_date` per row — exactly what we need
    to land ClosedStatusTime on its true date.

    Match strategy:
    Uses run_match_engine same as Cleanup Sync — Path A (workfile_id
    exact) wins on every closed-report row we've audited, since CCC's
    Closed report and SP both carry the workfile UUID. Path B fallback
    is available but rarely fires.

    Writes:
    - Closed = True (always, every matched row)
    - ClosedStatusTime = closed_date formatted as 'yyyy-MM-ddT18:00:00Z'
      (noon Mountain — matches 14b/14c convention, displays as the
      correct calendar day in SP's Mountain-time views)
    - Done = True (monotonic — a closed file must be done; never writes
      Done=False, that side is owned by Production Sync per Batch 4)
    - Total Loss = is_total_loss from report (correct any drift)

    Does NOT write: insurance, estimator, dates other than CST. Those
    are owned by other syncs.

    Phase 6 (insurance normalization): accepts insurance_lookup for
    parity with sister endpoints but does not write insurance — kept
    in the payload so the matcher's Path B insurance signal works on
    the rare Path B match.
    """
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No JSON body received'}), 400
    if 'xml' not in data:
        return jsonify({'error': 'Missing xml field (base64 of report XML)'}), 400

    sharepoint_items, err = coerce_sharepoint_items(data.get('sharepoint_items'))
    if err:
        return jsonify({'error': err}), 400

    insurance_lookup, err = coerce_sharepoint_items(data.get('insurance_lookup'))
    if err:
        return jsonify({'error': f'insurance_lookup parse error: {err}'}), 400

    try:
        xml_bytes = base64.b64decode(data['xml'])
        report_rows = parse_closed_report_xml(xml_bytes)
    except Exception as e:
        return jsonify({'error': f'Failed to parse XML report: {str(e)}'}), 400

    matched_pairs, unmatched, ambiguous = run_match_engine(report_rows, sharepoint_items, insurance_lookup)

    matched = []
    for row, sp, mtype in matched_pairs:
        # Real CCC close timestamp -> ClosedStatusTime.
        # closed_date arrives like '2026-06-16T00:00:00'. Take the date
        # half, restamp at T18:00:00Z to mirror 14b/14c convention
        # (noon Mountain, displays as the correct calendar day).
        raw_closed = row.get('closed_date', '')
        closed_status_time = (raw_closed[:10] + 'T18:00:00Z') if raw_closed else ''

        matched.append({
            'list_item_id':           sp.get('id'),
            'ro_number':              row['ro_number'],
            'workfile_id':            row.get('workfile_id', ''),
            'customer_name':          sp.get('customer_name'),
            'vehicle':                sp.get('vehicle'),
            'match_type':             mtype,
            # Always-write source fields
            'closed_date_raw':        raw_closed,
            'closed_status_time':     closed_status_time,
            'is_total_loss':          row.get('total_loss', False),
            'file_status_raw':        row.get('file_status', ''),
            # Closed=True for every matched row — by definition, this
            # row is in CCC's Closed report.
            'is_closed':              True,
            # Done=True monotonic (Batch 4 rule, same as Cleanup Sync):
            # a closed file must show Done=True. Never sets Done=False.
            'should_set_done':        True,
        })

    # Compute changes per matched row (email visibility — same shape
    # as Cleanup Sync's email).
    for m in matched:
        sp = next((s for s in sharepoint_items if s.get('id') == m['list_item_id']), {})
        new_values = {
            'ro_number':         m.get('ro_number', ''),
            'workfile_id':       m.get('workfile_id', ''),
            'done':              m.get('should_set_done', False),
            'closed':            m.get('is_closed', False),
            'closed_status_time': m.get('closed_status_time', ''),
            'total_loss':        m.get('is_total_loss', False),
        }
        m['changes'] = compute_changes(sp, new_values, CLOSED_SYNC_DIFF_FIELDS)
        m['changes_text'] = format_changes_text(m['changes'])

    return jsonify({
        'matched': matched,
        'unmatched_report_rows': unmatched,
        'ambiguous': ambiguous,
        'summary': {
            'report_rows_total': len(report_rows),
            'sharepoint_items': len(sharepoint_items),
            'insurance_lookup_entries': len(insurance_lookup),
            'matched': len(matched),
            'unmatched': len(unmatched),
            'ambiguous': len(ambiguous),
        }
    })
    
# ─── /match-cancelled-opportunities endpoint (Phase 4) ────────────

@app.route('/match-cancelled-opportunities', methods=['POST'])
def match_cancelled_opportunities():
    """Phase 4 — Cancelled Opportunities Cleanup.

    Matches Opportunities report rows (filtered to cancelled or
    closed-without-conversion) against open SP items. Returns matches that
    should be DELETED (not updated) — the caller's flow does Delete item
    instead of Update item.

    Safety guards block deletion of any SP row showing human or sync activity:
    populated RO#, Tech, Painter, ProductionNotes, PartsNotes, PartsStatus,
    or RepairStatus advanced past Prelim. Failed guards move the match to
    the ambiguous bucket for manual review.

    History: this endpoint shipped with a `DRY_RUN_CANCELLED_OPPS` toggle
    that routed all safe matches to ambiguous with "DRY RUN — would have
    deleted ..." reason. That toggle was removed May 15, 2026 after dry-run
    output was verified accurate. If you need to dry-run again in the
    future, the cleanest approach is to use the safety guards as your
    test bed (populate the row's Tech or RepairStatus to block deletion
    on a per-row basis) or add the toggle back temporarily.
    """
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No JSON body received'}), 400
    if 'xml' not in data:
        return jsonify({'error': 'Missing xml field (base64 of report XML)'}), 400

    sharepoint_items, err = coerce_sharepoint_items(data.get('sharepoint_items'))
    if err:
        return jsonify({'error': err}), 400

    # Optional insurance_lookup (May 20, 2026) — passed to run_match_engine
    # for Path B insurance verification. Cancelled Opportunities endpoint
    # historically didn't use it, but the engine's safety check needs it.
    # If absent or empty, the insurance check is skipped (still safe — the
    # source-side duplicate check is independent).
    insurance_lookup, _ = coerce_sharepoint_items(data.get('insurance_lookup'))

    try:
        xml_bytes = base64.b64decode(data['xml'])
        all_rows = parse_opportunities_xml(xml_bytes)
    except Exception as e:
        return jsonify({'error': f'Failed to parse XML report: {str(e)}'}), 400

    # Filter to deletion candidates only
    candidate_rows = [r for r in all_rows if is_cancelled_opportunity(r)]

    # The shared run_match_engine uses ro_number as a display identifier in
    # ambiguous/unmatched output. Cancelled opps usually have empty ro_number,
    # so substitute "WF:{workfile_id}" so the email tables show something useful.
    for r in candidate_rows:
        if not r.get('ro_number'):
            r['ro_number'] = f"WF:{r.get('workfile_id', '')}"

    matched_pairs, unmatched, ambiguous = run_match_engine(candidate_rows, sharepoint_items, insurance_lookup)

    # Apply safety guards. Failed guards move matches to ambiguous.
    safe_matches = []
    for row, sp, mtype in matched_pairs:
        is_safe, reason = cancelled_opp_safety_guards(sp, opp_row=row)
        if is_safe:
            safe_matches.append((row, sp, mtype))
        else:
            ambiguous.append({
                'ro_number': row['ro_number'],
                'owner': row['owner'],
                'vehicle': row['vehicle'],
                'candidate_ids': [sp.get('id')],
                'reason': f'safety guard: {reason}'
            })

    # Build matched array from safe matches.
    matched = []
    for row, sp, mtype in safe_matches:
        matched.append({
            'list_item_id':    sp.get('id'),
            'workfile_id':     row.get('workfile_id', ''),
            'customer_name':   sp.get('customer_name'),
            'vehicle':         sp.get('vehicle'),
            'cancel_date':     row.get('cancel_date', ''),
            'cancel_reason':   row.get('cancel_reason', ''),  # display only
            'match_type':      mtype,
        })

    return jsonify({
        'matched': matched,
        'unmatched_report_rows': unmatched,
        'ambiguous': ambiguous,
        'summary': {
            'report_rows_total':    len(all_rows),
            'deletion_candidates':  len(candidate_rows),
            'sharepoint_items':     len(sharepoint_items),
            'matched':              len(matched),
            'unmatched':            len(unmatched),
            'ambiguous':            len(ambiguous),
        }
    })

# ─── /match-opportunities endpoint (Phase 8 — June 2026) ──────────
#
# Replaces /match-cancelled-opportunities for Flow 10c. Does TWO jobs in one
# pass on the Opportunities XML:
#   1. Deletion side — same logic as the old endpoint: SP rows whose workfile
#      is cancelled/closed-without-conversion in CCC get queued for delete.
#      Safety guards block deletion if SP shows human or sync activity.
#   2. Stamping side — NEW. Active (not-cancelled, not-yet-converted) Opps
#      records get matched against SP open items. For SP rows missing
#      WorkfileID, Insurance, or Estimator, those fields are queued for
#      stamping from Opps data.
#
# Why combine: the Opportunities XML carries every active workfile (1000+
# rows). Today, only the cancelled subset is used (for deletion). The active
# subset (typically ~700 rows) contains workfile_id for many SP rows that
# the main sync flows (10/10a/10b) can't match — usually because the car
# hasn't been EMS-exported yet or hasn't started production. Analysis on
# 2026-06-09 showed 89 open SP rows missing RO#/WorkfileID were findable
# ONLY in the Opps report. This endpoint closes that gap.

@app.route('/match-opportunities', methods=['POST'])
def match_opportunities():
    """Phase 8 — Opportunities Sync.

    Matches Opportunities report rows against SP. Routes each matched pair
    into one of two buckets based on the Opps record's cancellation state:
      - cancelled/closed-without-conversion → DELETE bucket (with safety guards)
      - active opportunity → STAMP bucket (fills blank WorkfileID/Insurance/Estimator)
    """
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No JSON body received'}), 400
    if 'xml' not in data:
        return jsonify({'error': 'Missing xml field (base64 of report XML)'}), 400

    sharepoint_items, err = coerce_sharepoint_items(data.get('sharepoint_items'))
    if err:
        return jsonify({'error': err}), 400

    insurance_lookup, _ = coerce_sharepoint_items(data.get('insurance_lookup'))

    try:
        xml_bytes = base64.b64decode(data['xml'])
        all_rows = parse_opportunities_xml(xml_bytes)
    except Exception as e:
        return jsonify({'error': f'Failed to parse XML report: {str(e)}'}), 400

    # Tag each row with intended action — delete vs stamp.
    # Active rows are "not cancelled AND not yet converted to an RO".
    # converted_datetime populated means a Repair Order exists — that flow's
    # syncs (10/10a/10b) own those records, so we DON'T stamp from Opps.
    # We only stamp from rows that are still in opportunity stage.
    delete_candidates = []
    stamp_candidates = []
    for r in all_rows:
        if is_cancelled_opportunity(r):
            delete_candidates.append(r)
        elif not r.get('converted_datetime'):
            # Active opportunity — not yet an RO. Eligible for stamping.
            stamp_candidates.append(r)
        # else: already converted to RO — main sync flows handle it; skip here.

    # For display in emails: cancelled rows often have blank ro_number.
    # Substitute WF:{workfile_id} for both buckets so emails show something.
    for r in delete_candidates + stamp_candidates:
        if not r.get('ro_number'):
            r['ro_number'] = f"WF:{r.get('workfile_id', '')}"

    # === DELETE SIDE ===
    del_matched_pairs, del_unmatched, del_ambiguous = run_match_engine(
        delete_candidates, sharepoint_items, insurance_lookup
    )

    # Apply safety guards. Failed guards → ambiguous bucket.
    safe_deletes = []
    for row, sp, mtype in del_matched_pairs:
        is_safe, reason = cancelled_opp_safety_guards(sp, opp_row=row)
        if is_safe:
            safe_deletes.append((row, sp, mtype))
        else:
            del_ambiguous.append({
                'ro_number': row['ro_number'],
                'owner': row['owner'],
                'vehicle': row['vehicle'],
                'candidate_ids': [sp.get('id')],
                'reason': f'delete-side safety guard: {reason}'
            })

    matched_for_delete = []
    for row, sp, mtype in safe_deletes:
        matched_for_delete.append({
            'list_item_id':    sp.get('id'),
            'workfile_id':     row.get('workfile_id', ''),
            'customer_name':   sp.get('customer_name'),
            'vehicle':         sp.get('vehicle'),
            'cancel_date':     row.get('cancel_date', ''),
            'cancel_reason':   row.get('cancel_reason', ''),
            'match_type':      mtype,
        })

    # === STAMP SIDE ===
    stamp_matched_pairs, stamp_unmatched, stamp_ambiguous = run_match_engine(
        stamp_candidates, sharepoint_items, insurance_lookup
    )

    matched_for_stamp = []
    for row, sp, mtype in stamp_matched_pairs:
        # Build new_values dict — only stamp fields where SP is blank.
        sp_wf = (sp.get('workfile_id') or '').strip()
        sp_ins = (sp.get('insurance') or '').strip()
        sp_est = (sp.get('estimator') or '').strip()

        opps_wf = (row.get('workfile_id') or '').strip()
        opps_carrier = (row.get('carrier_name') or '').strip()
        opps_writer = (row.get('estimator') or '').strip()

        # Normalize estimator first-name only (matches main sync convention).
        # Opps gives full name like "Jennie Nicolls" → take first token.
        opps_estimator_first = opps_writer.split()[0] if opps_writer else ''

        # Insurance — apply normalization via lookup if available.
        normalized_ins = ''
        if opps_carrier and insurance_lookup:
            normalized_ins = normalize_insurance_name(opps_carrier, insurance_lookup) or ''
        elif opps_carrier:
            normalized_ins = opps_carrier  # raw carrier name as fallback

        new_values = {}
        if not sp_wf and opps_wf:
            new_values['workfile_id'] = opps_wf
        if not sp_ins and normalized_ins:
            new_values['insurance'] = normalized_ins
        if not sp_est and opps_estimator_first:
            new_values['estimator'] = opps_estimator_first

        # Only include rows with actually-stampable fields (skip no-ops).
        if not new_values:
            continue

        matched_for_stamp.append({
            'list_item_id':    sp.get('id'),
            'workfile_id':     opps_wf,
            'customer_name':   sp.get('customer_name'),
            'vehicle':         sp.get('vehicle'),
            'match_type':      mtype,
            'new_values':      new_values,
            'opps_carrier':    opps_carrier,
            'opps_estimator':  opps_writer,
        })

    # Combine unmatched and ambiguous across both sides for the summary email.
    # (Same SP row may appear in both — that's fine, they're for different things.)
    combined_ambiguous = del_ambiguous + stamp_ambiguous

    return jsonify({
        'matched_for_delete':       matched_for_delete,
        'matched_for_stamp':        matched_for_stamp,
        'delete_candidates_total':  len(delete_candidates),
        'stamp_candidates_total':   len(stamp_candidates),
        'ambiguous':                combined_ambiguous,
        'summary': {
            'report_rows_total':      len(all_rows),
            'delete_candidates':      len(delete_candidates),
            'stamp_candidates':       len(stamp_candidates),
            'sharepoint_items':       len(sharepoint_items),
            'matched_for_delete':     len(matched_for_delete),
            'matched_for_stamp':      len(matched_for_stamp),
            'ambiguous':              len(combined_ambiguous),
        }
    })

# ─── /match-scan-report endpoint (Phase 7 — Scan Report Sync, June 2026) ──

def parse_scan_report_xml(xml_bytes):
    """Parse Diagnostic Scan Report XML.
    
    Returns list of dicts with: workfile_id, ro_number, vin, carrier_name,
    vehicle_year, vehicle_make_name, vehicle_model_name, scan_phase_description,
    created_datetime, scan_type.
    
    A single car may have multiple scan records (pre-repair + post-repair).
    Caller is responsible for grouping by workfile_id or vin as needed.
    """
    import xml.etree.ElementTree as ET
    
    def _strip_ns(tag):
        return tag.split('}')[-1]
    
    tree = ET.fromstring(xml_bytes)
    records = []
    for elem in tree.iter():
        children = {_strip_ns(c.tag): (c.text or '').strip() for c in elem}
        if 'vehicle_vin' in children and 'workfile_id' in children:
            records.append({
                'workfile_id':              children.get('workfile_id', ''),
                'ro_number':                children.get('repair_order_number', ''),
                'vin':                      children.get('vehicle_vin', ''),
                'carrier_name':             children.get('carrier_name', ''),
                'vehicle_year':             children.get('vehicle_year', ''),
                'vehicle_make_name':        children.get('vehicle_make_name', ''),
                'vehicle_model_name':       children.get('vehicle_model_name', ''),
                'scan_phase_description':   children.get('scan_phase_description', ''),
                'created_datetime':         children.get('created_datetime', ''),
                'scan_type':                children.get('scan_type', ''),
            })
    return records


def _parse_iso_date(s):
    """Parse YYYY-MM-DD or full ISO datetime, return date object or None."""
    if not s:
        return None
    try:
        from datetime import datetime
        return datetime.fromisoformat(s.split('T')[0]).date()
    except Exception:
        return None


def _days_between(d1_str, d2_str):
    """Days between two ISO date strings. Returns None if either is blank/invalid."""
    d1 = _parse_iso_date(d1_str)
    d2 = _parse_iso_date(d2_str)
    if d1 is None or d2 is None:
        return None
    return abs((d1 - d2).days)


def _disambiguate_scan_workfiles(sp_row, candidate_workfiles, insurance_lookup):
    """Given an SP row and multiple candidate scan workfiles for the same VIN,
    try to identify a single correct workfile using carrier and dates.
    
    Returns (chosen_workfile_dict, reason_text) or (None, reason_text).
    
    Disambiguation order:
      0. RO# match — if SP has an RO# and exactly one candidate's RO matches,
         choose it (added June 8, 2026).
      1. Carrier match — normalize sp_row.insurance and each candidate's
         carrier_name; if exactly one candidate matches, choose it.
      2. Pre-repair scan within 4 days of SP DropDate — if exactly one.
      3. Post-repair scan within 4 days of SP CCCPromisDate — if exactly one.
      Otherwise ambiguous.
    """
    # Step 0 — RO# match
    sp_ro = (sp_row.get('ro_number') or '').strip().lower()
    if sp_ro:
        ro_matches = []
        for wf in candidate_workfiles:
            wf_ro = (wf.get('ro_number') or '').strip().lower()
            if wf_ro and wf_ro == sp_ro:
                ro_matches.append(wf)
        if len(ro_matches) == 1:
            return ro_matches[0], f"matched by RO# ({sp_ro.upper()})"

    # Step 1 — carrier match
    sp_insurance = (sp_row.get('insurance') or '').strip()
    if sp_insurance and not sp_insurance.startswith('⚠️'):
        sp_normalized = normalize_insurance_name(sp_insurance, insurance_lookup)
        carrier_matches = []
        for wf in candidate_workfiles:
            wf_normalized = normalize_insurance_name(wf['carrier_name'], insurance_lookup)
            if sp_normalized and wf_normalized and sp_normalized == wf_normalized:
                carrier_matches.append(wf)
        if len(carrier_matches) == 1:
            return carrier_matches[0], f"matched by carrier ({sp_normalized})"
    
    # Step 2 — pre-repair scan near drop date
    drop_date = sp_row.get('drop_date', '')
    if drop_date:
        pre_near_drop = []
        for wf in candidate_workfiles:
            phase = (wf.get('scan_phase_description') or '').lower()
            if 'pre' not in phase:
                continue
            delta = _days_between(wf.get('created_datetime', ''), drop_date)
            if delta is not None and delta <= 4:
                pre_near_drop.append(wf)
        if len(pre_near_drop) == 1:
            return pre_near_drop[0], "matched by pre-repair scan near DropDate"
    
    # Step 3 — post-repair scan near CCCPromisDate
    promise_date = sp_row.get('cccpromisdate', '')
    if promise_date:
        post_near_promise = []
        for wf in candidate_workfiles:
            phase = (wf.get('scan_phase_description') or '').lower()
            if 'post' not in phase:
                continue
            delta = _days_between(wf.get('created_datetime', ''), promise_date)
            if delta is not None and delta <= 4:
                post_near_promise.append(wf)
        if len(post_near_promise) == 1:
            return post_near_promise[0], "matched by post-repair scan near CCCPromisDate"
    
    return None, f"VIN matched {len(candidate_workfiles)} workfiles, no disambiguator succeeded"


@app.route('/match-scan-report', methods=['POST'])
def match_scan_report():
    """Phase 7 — Scan Report Sync (June 2026).

    Uses CCC ONE's Diagnostic Scan Report (OPUS IVS) to enrich SP rows that
    have a VIN populated but are missing WorkfileID or RO#. Particularly
    useful for rows previously stuck in the ambiguous bucket due to
    multiple CCC workfiles matching customer+vehicle — the scan report
    provides VIN, which uniquely identifies a physical car, plus
    scan_phase_description + created_datetime which enable date-based
    disambiguation between multiple repair episodes for the same car.

    Match logic (per SP row):
      1. SP row must have VIN populated. Rows without VIN skip.
      2. Look up all scan records with matching VIN.
      3. If those scans all belong to a single workfile_id → confident match.
      4. If multiple workfile_ids → try disambiguators (carrier, then dates).
      5. If still ambiguous → report as ambiguous.

    Stamps on confident match:
      - RO# (if blank in SP)
      - WorkfileID (if blank in SP)
      - Insurance (normalized, if blank in SP)
      - Vehicle (year+make+model concat, if blank in SP)
      - VIN (already populated, but stamps if missing for some reason)
    """
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No JSON body received'}), 400
    if 'xml' not in data:
        return jsonify({'error': 'Missing xml field (base64 of report XML)'}), 400

    sharepoint_items, err = coerce_sharepoint_items(data.get('sharepoint_items'))
    if err:
        return jsonify({'error': err}), 400

    insurance_lookup, err = coerce_sharepoint_items(data.get('insurance_lookup'))
    if err:
        return jsonify({'error': f'insurance_lookup parse error: {err}'}), 400

    try:
        xml_bytes = base64.b64decode(data['xml'])
        scan_rows = parse_scan_report_xml(xml_bytes)
    except Exception as e:
        return jsonify({'error': f'Failed to parse XML report: {str(e)}'}), 400

    # Build VIN → list of scan records lookup
    from collections import defaultdict
    vin_to_scans = defaultdict(list)
    for s in scan_rows:
        if s['vin']:
            vin_to_scans[s['vin'].upper().strip()].append(s)

    matched = []
    ambiguous = []
    skipped_no_vin = 0
    skipped_no_scan = 0

    for sp in sharepoint_items:
        sp_vin = (sp.get('vin') or '').upper().strip()
        if not sp_vin:
            skipped_no_vin += 1
            continue

        candidate_scans = vin_to_scans.get(sp_vin, [])
        if not candidate_scans:
            skipped_no_scan += 1
            continue

        # Collapse scans to unique workfile_ids
        wfid_to_scans = defaultdict(list)
        for s in candidate_scans:
            wfid_to_scans[s['workfile_id']].append(s)

        # Pick one representative scan per workfile for disambiguation
        candidate_workfiles = []
        for wfid, scans in wfid_to_scans.items():
            # Prefer a scan that has a real RO# attached; otherwise first
            chosen = next((s for s in scans if s.get('ro_number')), scans[0])
            candidate_workfiles.append(chosen)

        if len(candidate_workfiles) == 1:
            chosen = candidate_workfiles[0]
            reason = "single workfile_id for VIN"
        else:
            chosen, reason = _disambiguate_scan_workfiles(sp, candidate_workfiles, insurance_lookup)
            if chosen is None:
                ambiguous.append({
                    'list_item_id':   sp.get('id'),
                    'customer_name':  sp.get('customer_name'),
                    'vehicle':        sp.get('vehicle'),
                    'vin':            sp_vin,
                    'reason':         reason,
                    'candidate_workfile_ids': [wf['workfile_id'] for wf in candidate_workfiles],
                    'candidate_ros':  [wf.get('ro_number', '') for wf in candidate_workfiles],
                })
                continue

        # Build new values to potentially stamp. Only stamps blank fields in SP.
        raw_carrier = chosen.get('carrier_name', '')
        vehicle_combined = ' '.join(filter(None, [
            chosen.get('vehicle_year', ''),
            chosen.get('vehicle_make_name', ''),
            chosen.get('vehicle_model_name', ''),
        ])).strip()

        new_values = {
            'ro_number':           chosen.get('ro_number', '') if not (sp.get('ro_number') or '').strip() else '',
            'workfile_id':         chosen.get('workfile_id', '') if not (sp.get('workfile_id') or '').strip() else '',
            'insurance':           normalize_insurance_name(raw_carrier, insurance_lookup) if not (sp.get('insurance') or '').strip() else '',
            'vehicle':             vehicle_combined if not (sp.get('vehicle') or '').strip() else '',
            'vin':                 sp_vin if not (sp.get('vin') or '').strip() else '',
        }
        # Filter out empty/no-op writes
        new_values = {k: v for k, v in new_values.items() if v}

        if not new_values:
            # Match confirmed but nothing to stamp — SP already has all the data
            continue

        matched.append({
            'list_item_id':       sp.get('id'),
            'customer_name':      sp.get('customer_name'),
            'vehicle':            sp.get('vehicle'),
            'vin':                sp_vin,
            'matched_workfile_id': chosen.get('workfile_id', ''),
            'matched_ro':         chosen.get('ro_number', ''),
            'match_reason':       reason,
            'new_values':         new_values,
        })

    return jsonify({
        'matched': matched,
        'ambiguous': ambiguous,
        'summary': {
            'scan_report_rows': len(scan_rows),
            'unique_vins_in_report': len(vin_to_scans),
            'sharepoint_items': len(sharepoint_items),
            'skipped_no_vin_in_sp': skipped_no_vin,
            'skipped_no_scan_for_vin': skipped_no_scan,
            'matched': len(matched),
            'ambiguous': len(ambiguous),
        }
    })
    
# ─── /board-data endpoint ─────────────────────────────────────────

@app.route('/board-data', methods=['POST'])
def board_data_post():
    global _board_data
    data = request.get_json()
    if not data or not isinstance(data, list):
        return jsonify({'error': 'Expected a JSON array'}), 400
    _board_data = data
    return jsonify({'status': 'ok', 'rows': len(_board_data)})

@app.route('/board-data', methods=['GET'])
def board_data_get():
    response = jsonify(_board_data)
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response

@app.route('/board', methods=['GET'])
def board():
    return send_from_directory('.', 'board.html')

@app.route('/board-manual', methods=['GET'])
def board_manual():
    return send_from_directory('.', 'board-manual.html')
    
@app.route('/3day', methods=['GET'])
def three_day():
    return send_from_directory('.', '3day.html')

@app.route('/3day-tv', methods=['GET'])
def three_day_tv():
    return send_from_directory('.', '3day-tv.html')

@app.route('/3day-mobile', methods=['GET'])
def three_day_mobile():
    return send_from_directory('.', '3day-mobile.html')

@app.route('/3day-auto', methods=['GET'])
def three_day_auto():
    return send_from_directory('.', '3day-auto.html')

@app.route('/3day-test-a', methods=['GET'])
def board_3day_test_a():
    return send_from_directory('.', '3day-test-a.html')

@app.route('/3day-test-b', methods=['GET'])
def board_3day_test_b():
    return send_from_directory('.', '3day-test-b.html')

# ─── /estimator-data endpoint ─────────────────────────────────────
# Separate feed for the per-estimator Kanban boards. Wider filter than
# /board-data (Closed eq false AND DropDate ne null) — produced by its
# own Power Automate flow on a 15-min timer. Kept fully separate from
# /board-data so the 3-day board is never affected.

@app.route('/estimator-data', methods=['POST'])
def estimator_data_post():
    global _estimator_data
    data = request.get_json()
    if not data or not isinstance(data, list):
        return jsonify({'error': 'Expected a JSON array'}), 400
    _estimator_data = data
    return jsonify({'status': 'ok', 'rows': len(_estimator_data)})

@app.route('/estimator-data', methods=['GET'])
def estimator_data_get():
    response = jsonify(_estimator_data)
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response

# ─── estimator board file routes ──────────────────────────────────

@app.route('/main', methods=['GET'])
def board_main():
    return send_from_directory('.', 'main.html')

@app.route('/logan', methods=['GET'])
def board_logan():
    return send_from_directory('.', 'logan.html')

@app.route('/cord', methods=['GET'])
def board_cord():
    return send_from_directory('.', 'cord.html')

@app.route('/dana', methods=['GET'])
def board_dana():
    return send_from_directory('.', 'dana.html')

@app.route('/jennie', methods=['GET'])
def board_jennie():
    return send_from_directory('.', 'jennie.html')

@app.route('/other', methods=['GET'])
def board_other():
    return send_from_directory('.', 'other.html')

# ─── /last-sync endpoint ──────────────────────────────────────────

_last_sync = None

@app.route('/last-sync', methods=['POST'])
def last_sync_post():
    ts = datetime.utcnow().isoformat() + 'Z'
    try:
        with open(_SYNC_FILE, 'w') as f:
            f.write(ts)
    except Exception:
        pass
    return jsonify({'status': 'ok', 'last_sync': ts})

@app.route('/last-sync', methods=['GET'])
def last_sync_get():
    ts = None
    try:
        with open(_SYNC_FILE, 'r') as f:
            ts = f.read().strip()
    except Exception:
        pass
    response = jsonify({'last_sync': ts})
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response

# ─── /mtd endpoint ────────────────────────────────────────────────

_mtd_data = {
    'doneDollars': 0,
    'doneLabor': 0,
    'closedDollars': 0,
    'closedLabor': 0,
    'projectedDollars': 0,
    'projectedLabor': 0,
    'updated': None,
}

@app.route('/mtd', methods=['POST'])
def mtd_post():
    """Receive month-to-date stats from PA flow.

    Expected body:
      {
        "doneDollars": <num>,
        "doneLabor": <num>,
        "closedDollars": <num>,
        "closedLabor": <num>,
        "projectedDollars": <num>,
        "projectedLabor": <num>
      }

    Done MTD = production performance (DoneStatusTime in current month).
    Closed MTD = estimator/billing performance (ClosedStatusTime in current month).
    Projected MTD = total promised this month (CCCPromisDate in current month, all rows).
    """
    global _mtd_data
    data = request.get_json()
    if not data:
        return jsonify({'error': 'Expected JSON'}), 400
    _mtd_data = {
        'doneDollars':      data.get('doneDollars', 0),
        'doneLabor':        data.get('doneLabor', 0),
        'closedDollars':    data.get('closedDollars', 0),
        'closedLabor':      data.get('closedLabor', 0),
        'projectedDollars': data.get('projectedDollars', 0),
        'projectedLabor':   data.get('projectedLabor', 0),
        'updated':          datetime.utcnow().isoformat() + 'Z',
    }
    try:
        with open(os.path.join(os.path.dirname(__file__), 'mtd.txt'), 'w') as f:
            import json as _json
            f.write(_json.dumps(_mtd_data))
    except Exception:
        pass
    return jsonify({'status': 'ok', 'data': _mtd_data})

@app.route('/mtd', methods=['GET'])
def mtd_get():
    global _mtd_data
    if _mtd_data['updated'] is None:
        try:
            with open(os.path.join(os.path.dirname(__file__), 'mtd.txt'), 'r') as f:
                import json as _json
                _mtd_data = _json.loads(f.read())
        except Exception:
            pass
    response = jsonify(_mtd_data)
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response

# ─── /mtd-by-estimator endpoint ───────────────────────────────────

_mtd_by_estimator_data = {
    'All':     {'completed': 0, 'closed': 0},
    'Logan':   {'completed': 0, 'closed': 0},
    'Cordale': {'completed': 0, 'closed': 0},
    'Dana':    {'completed': 0, 'closed': 0},
    'Jennie':  {'completed': 0, 'closed': 0},
    'Other':   {'completed': 0, 'closed': 0},
    'updated': None
}

@app.route('/mtd-by-estimator', methods=['POST'])
def mtd_by_estimator_post():
    global _mtd_by_estimator_data
    data = request.get_json()
    if not data:
        return jsonify({'error': 'Expected JSON'}), 400
    _mtd_by_estimator_data = {
        'All':     data.get('All',     {'completed': 0, 'closed': 0}),
        'Logan':   data.get('Logan',   {'completed': 0, 'closed': 0}),
        'Cordale': data.get('Cordale', {'completed': 0, 'closed': 0}),
        'Dana':    data.get('Dana',    {'completed': 0, 'closed': 0}),
        'Jennie':  data.get('Jennie',  {'completed': 0, 'closed': 0}),
        'Other':   data.get('Other',   {'completed': 0, 'closed': 0}),
        'updated': datetime.utcnow().isoformat() + 'Z'
    }
    try:
        with open(os.path.join(os.path.dirname(__file__), 'mtd_by_estimator.txt'), 'w') as f:
            import json as _json
            f.write(_json.dumps(_mtd_by_estimator_data))
    except Exception:
        pass
    return jsonify({'status': 'ok', 'data': _mtd_by_estimator_data})

@app.route('/mtd-by-estimator', methods=['GET'])
def mtd_by_estimator_get():
    global _mtd_by_estimator_data
    if _mtd_by_estimator_data['updated'] is None:
        try:
            with open(os.path.join(os.path.dirname(__file__), 'mtd_by_estimator.txt'), 'r') as f:
                import json as _json
                _mtd_by_estimator_data = _json.loads(f.read())
        except Exception:
            pass
    response = jsonify(_mtd_by_estimator_data)
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response

# ─── /health endpoint ─────────────────────────────────────────────

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'good'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
