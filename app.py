from flask import Flask, request, jsonify
import tempfile
import os
import base64
import json
import re
import xml.etree.ElementTree as ET
from dbfread import DBF

app = Flask(__name__)

# ─── Phase 3 mapping tables ───────────────────────────────────────

# Body technician → DTBS Tech choice value
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

# Paint technician → DTBS Painter choice value
PAINTER_MAPPING = {
    'Doug Curtis':     'Doug',
    'Wayne Decker':    'Wayne',
    'Rick Hopkins':    'Rick',
    'Admir Huskic':    'Admir',
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

    # Phase 4: Reassembly
    '4:Reassembly':                        'Reassembly',
    '4:Reassembly ANNEX':                  'Reassembly',

    # Phase 5: QC / Detail
    '5:Detail':                            'QC',
    '5:QC':                                'QC',
    '5:QC FAIL':                           'QC',
    '5:Post-Scan':                         'QC',

    # Phase 6: Done / Customer
    '6:Done, To Estimator':                'QC',
    '6:Repairs Complete, Customer Notified': 'Ready for Delivery',
    '6:Waiting on Insurance for Delivery': 'Ready for Delivery',
    '6:CustomerRequestRecall/Oilchange ef': None,  # service work — skip

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

    value_type: 'bool' | 'date' | 'text'
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
            'days_in_shop':      _xml_text(o, 'days_in_shop'),
            'parts_received_pct': _xml_text(o, 'parts_received_percent'),
            'labor_assigned_pct': _xml_text(o, 'labor_assigned_percent'),
            'total_loss':        _xml_text(o, 'is_total_loss').lower() == 'true',
        })
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
    """Apply rank-based progression rule.
    Returns True if new_status should overwrite current_status."""
    if new_status is None:
        return False
    # Sublet and Total Loss always overwrite
    if new_status in PHASES_ALWAYS_OVERWRITE:
        return True
    new_rank = DTBS_STATUS_RANK.get(new_status, 0)
    cur_rank = DTBS_STATUS_RANK.get(current_status or '', 0)
    return new_rank > cur_rank

def map_tech(ccc_full_name):
    """Map CCC body tech full name to DTBS Tech choice value.
    Returns empty string if no mapping (don't write)."""
    if not ccc_full_name:
        return ''
    return TECH_MAPPING.get(ccc_full_name.strip(), '')

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

def run_match_engine(report_rows, sharepoint_items):
    """Shared matching engine. Returns (matched_pairs, unmatched, ambiguous).

    matched_pairs is list of (report_row, sp_item, match_type) tuples.
    Caller is responsible for building the response shape from these pairs.

    Match precedence:
      Path A    — workfile_id           (CCC-internal unique ID, exact)
      Path A.5  — ro_number             (exact, case-insensitive, trimmed)
                  ro_number_compatible  (Tekion suffix case: SP has extra '-N')
      Path B    — customer + vehicle    (fuzzy fallback for un-synced rows)

    Path B includes a disqualifier: any SP row whose workfile_id or
    ro_number contradicts the report row's IDs is skipped from candidates.
    This prevents already-claimed SP rows from being magnet-matched on
    shared customer + vehicle prefix (e.g., dealer accounts with many ROs).
    """
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

        # Path B: customer + vehicle prefix (with disqualifier)
        if not norm_owner or not norm_vehicle:
            unmatched.append({
                'ro_number': ro_number,
                'owner': row['owner'],
                'vehicle': row['vehicle'],
                'reason': 'report row missing customer or vehicle'
            })
            continue

        candidates = []
        for item in sharepoint_items:
            item_customer = (item.get('customer_name') or '').lower()
            item_vehicle = (item.get('vehicle') or '').lower()
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

        if len(candidates) == 1:
            provisional.append((row, candidates[0], 'customer_vehicle'))
        elif len(candidates) == 0:
            unmatched.append({
                'ro_number': ro_number,
                'owner': row['owner'],
                'vehicle': row['vehicle'],
                'reason': 'no SharePoint item with matching customer + vehicle'
            })
        else:
            est_matches = [
                c for c in candidates
                if estimator_first_name_match(report_estimator, c.get('estimator', ''))
            ]
            if len(est_matches) == 1:
                provisional.append((row, est_matches[0], 'customer_vehicle_estimator'))
            else:
                ambiguous.append({
                    'ro_number': ro_number,
                    'owner': row['owner'],
                    'vehicle': row['vehicle'],
                    'candidate_ids': [c.get('id') for c in candidates],
                    'reason': f'{len(candidates)} SharePoint items matched same customer + vehicle prefix'
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
        })
    return results

def is_cancelled_opportunity(row):
    """Filter: row represents a workfile that was EMS-exported but never
    became an active RO, and is now safe to delete from DTBS.

    Criteria:
    - converted_datetime is blank (never became an RO), AND
    - cancel_date is populated OR workfile_status is Closed

    Cancel reason is explicitly NOT used — estimators choose it based on
    notification suppression, not accurate categorization.
    """
    if row.get('converted_datetime'):
        return False  # became an RO at some point — leave alone
    if row.get('cancel_date'):
        return True   # explicitly cancelled
    if row.get('workfile_status', '').strip() == 'Closed':
        return True   # closed without converting
    return False      # still open, may convert later

def cancelled_opp_safety_guards(sp_item):
    """Check if a matched SP row has any indicators of human/sync activity
    that should prevent auto-deletion. Returns (is_safe, reason).

    Failed guards route the match to ambiguous instead of delete:
    - ro_number (Title) populated — sync or human claimed the row
    - Tech, Painter, ProductionNotes, PartsNotes, PartsStatus populated
    - RepairStatus is anything other than blank or 'Prelim'

    SP item dict keys expected: ro_number, tech, painter, production_notes,
    parts_notes, parts_status, repair_status. These come from the PA Project
    SP Items Select action — see flow build instructions.
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
        cust_first = get_val(ad1, 'INSD_FN')
        cust_last = get_val(ad1, 'INSD_LN')
        cust_co = get_val(ad1, 'INSD_CO_NM')
        # OWNR_ fallback fields: for third-party claims (CUST_PR='C', claimant)
        # where the car owner is the Ken Garff customer but the policy holder
        # (INSD_) is a different person (the at-fault driver). CCC stores the
        # claimant's info under OWNR_. Without this fallback, third-party claim
        # workfiles come into DTBS with blank CustomerName.
        ownr_first = get_val(ad1, 'OWNR_FN')
        ownr_last = get_val(ad1, 'OWNR_LN')
        ownr_co = get_val(ad1, 'OWNR_CO_NM')

        if cust_first or cust_last:
            # "Last, First" format — matches how shop staff reference cars
            # (by last name) and how CCC ONE reports come in for human customers.
            customer_name = f"{cust_last}, {cust_first}".strip(', ').strip()
        elif cust_co:
            customer_name = cust_co
        elif ownr_first or ownr_last:
            # Third-party claim fallback — owner is the Ken Garff customer.
            customer_name = f"{ownr_last}, {ownr_first}".strip(', ').strip()
        else:
            customer_name = ownr_co

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
            'other_hrs': str(other_hrs)
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

    matched_pairs, unmatched, ambiguous = run_match_engine(report_rows, sharepoint_items)

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
            'actual_delivery': vehicle_out if is_completed and vehicle_out else None,
            'done':            is_completed,
            'total_loss':      m.get('is_total_loss', False),
            'insurance':       m.get('normalized_insurance', ''),
            'estimator':       m.get('estimator_first_name', ''),
        }
        m['changes'] = compute_changes(sp, new_values, RO_SYNC_DIFF_FIELDS)

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

    matched_pairs, unmatched, ambiguous = run_match_engine(report_rows, sharepoint_items)

    matched = []
    for row, sp, mtype in matched_pairs:
        sp_insurance_now = sp.get('insurance', '') or ''
        sp_status_now = sp.get('repair_status', '') or ''

        # Repair Status decision (rank-based)
        new_status = map_phase_to_status(row.get('repair_phase', ''))
        write_status = should_write_status(new_status, sp_status_now)

        # Tech & Painter mapping (skip if no mapping)
        new_tech = map_tech(row.get('body_tech', ''))
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
            # Production metrics (informational, not currently written to SP)
            'days_in_shop':           row.get('days_in_shop', ''),
            'parts_received_pct':     row.get('parts_received_pct', ''),
            'labor_assigned_pct':     row.get('labor_assigned_pct', ''),
        })

    # Compute changes per matched row (May 18 — email visibility feature).
    for m in matched:
        sp = next((s for s in sharepoint_items if s.get('id') == m['list_item_id']), {})
        vehicle_out = m.get('vehicle_out_datetime', '')
        vehicle_in = m.get('vehicle_in_datetime', '')
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
        }
        m['changes'] = compute_changes(sp, new_values, PRODUCTION_SYNC_DIFF_FIELDS)

    # Stale tracker detection: SP items that aren't in any matched pair
    matched_sp_ids = {p[1].get('id') for p in matched_pairs}
    stale_sp_rows = []
    for item in sharepoint_items:
        if item.get('id') not in matched_sp_ids:
            stale_sp_rows.append({
                'list_item_id': item.get('id'),
                'customer_name': item.get('customer_name', ''),
                'vehicle': item.get('vehicle', ''),
                'workfile_id': item.get('workfile_id', ''),
                'ro_number': item.get('ro_number', ''),
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

    matched_pairs, unmatched, ambiguous = run_match_engine(report_rows, sharepoint_items)

    matched = []
    for row, sp, mtype in matched_pairs:
        sp_insurance_now = sp.get('insurance', '') or ''
        is_delivered = row.get('is_delivered', False)
        # Phase 5: file_status_name from CCC drives the new Closed flag.
        # Done flips when EITHER the vehicle is delivered OR the file is closed in CCC
        # (file_status=Closed without delivery happens for total losses, etc.)
        is_closed = row.get('file_status', '').strip().lower() == 'closed'
        should_set_done = is_delivered or is_closed

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
            'should_set_done':        should_set_done,
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
            'done':            m.get('should_set_done', False),
            'closed':          m.get('is_closed', False),
            'actual_delivery': vehicle_out if is_delivered_now and vehicle_out else None,
            'total_loss':      m.get('is_total_loss', False),
            'insurance':       m.get('normalized_insurance', ''),
            'estimator':       m.get('estimator_first_name', ''),
        }
        m['changes'] = compute_changes(sp, new_values, CLEANUP_SYNC_DIFF_FIELDS)

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

    matched_pairs, unmatched, ambiguous = run_match_engine(candidate_rows, sharepoint_items)

    # Apply safety guards. Failed guards move matches to ambiguous.
    safe_matches = []
    for row, sp, mtype in matched_pairs:
        is_safe, reason = cancelled_opp_safety_guards(sp)
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

# ─── /health endpoint (unchanged) ─────────────────────────────────

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
