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
    for i, item in enumerate(raw):
        if not isinstance(item, dict):
            return None, f'sharepoint_items[{i}] must be a dict, got {type(item).__name__}. Sample: {repr(item)[:200]}'
    return raw, None

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
        })
    return results

def normalize_owner(owner):
    """Convert 'Last, First' to 'First Last' to match EMS parser output."""
    if not owner:
        return ''
    if ',' in owner:
        parts = owner.split(',', 1)
        return f"{parts[1].strip()} {parts[0].strip()}"
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

def run_match_engine(report_rows, sharepoint_items):
    """Shared matching engine. Returns (matched_pairs, unmatched, ambiguous).

    matched_pairs is list of (report_row, sp_item, match_type) tuples.
    Caller is responsible for building the response shape from these pairs.
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
        norm_owner = normalize_owner(row['owner']).lower()
        norm_vehicle = normalize_year_4to2(row['vehicle']).lower()
        report_estimator = row.get('estimator', '')

        # Path A: workfile_id
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

        # Path B: customer + vehicle prefix
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

# ─── /parse endpoint (unchanged) ──────────────────────────────────

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

        if cust_first or cust_last:
            customer_name = f"{cust_first} {cust_last}".strip()
        else:
            customer_name = cust_co

        est_first = get_val(ad2, 'EST_CT_FN')
        est_last = get_val(ad2, 'EST_CT_LN')
        estimator = est_first.strip() if est_first else est_last.strip()
        drop_date = get_val(ad2, 'RO_IN_DATE')
        promise_date = get_val(ad2, 'TAR_DATE')

        year = get_val(veh, 'V_MODEL_YR')
        make_code = get_val(veh, 'V_MAKECODE')
        model_full = get_val(veh, 'V_MODEL')
        model_short = ' '.join(model_full.split()[:2]) if model_full else ''
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

# ─── /match-ro-report endpoint (Phase 2) ──────────────────────────

@app.route('/match-ro-report', methods=['POST'])
def match_ro_report():
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
        report_rows = parse_ro_report_xml(xml_bytes)
    except Exception as e:
        return jsonify({'error': f'Failed to parse XML report: {str(e)}'}), 400

    # Build a workfile_id index for fast lookup. Items without workfile_id
    # fall through to the customer+vehicle prefix matcher below.
    wf_index = {}
    for item in sharepoint_items:
        wf = (item.get('workfile_id') or '').strip()
        if wf:
            wf_index.setdefault(wf, []).append(item)

    # Provisional results — we'll post-process for duplicate SP matches.
    provisional_matches = []  # list of (report_row, sp_item, match_type)
    unmatched = []
    ambiguous = []

    for row in report_rows:
        ro_number = row['ro_number']
        report_wf = (row.get('workfile_id') or '').strip()
        norm_owner = normalize_owner(row['owner']).lower()
        norm_vehicle = normalize_year_4to2(row['vehicle']).lower()
        report_estimator = row['estimator']

        # Path A: workfile_id match (preferred — unique key)
        if report_wf and report_wf in wf_index:
            wf_candidates = wf_index[report_wf]
            if len(wf_candidates) == 1:
                provisional_matches.append((row, wf_candidates[0], 'workfile_id'))
                continue
            ambiguous.append({
                'ro_number': ro_number,
                'owner': row['owner'],
                'vehicle': row['vehicle'],
                'candidate_ids': [c.get('id') for c in wf_candidates],
                'reason': 'multiple SharePoint items share this workfile_id'
            })
            continue

        # Path B: customer + vehicle prefix fallback
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
            if item_customer == norm_owner and norm_vehicle.startswith(item_vehicle):
                candidates.append(item)

        if len(candidates) == 1:
            provisional_matches.append((row, candidates[0], 'customer_vehicle'))
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
                provisional_matches.append((row, est_matches[0], 'customer_vehicle_estimator'))
            else:
                ambiguous.append({
                    'ro_number': ro_number,
                    'owner': row['owner'],
                    'vehicle': row['vehicle'],
                    'candidate_ids': [c.get('id') for c in candidates],
                    'reason': f'{len(candidates)} SharePoint items matched same customer + vehicle prefix'
                })

    # Post-process: detect SharePoint items that would be matched by multiple
    # report rows. Move ALL such collisions to ambiguous to prevent silent overwrites.
    sp_id_to_rows = {}
    for row, sp, mtype in provisional_matches:
        sp_id_to_rows.setdefault(sp.get('id'), []).append((row, sp, mtype))

    matched = []
    for sp_id, hits in sp_id_to_rows.items():
        if len(hits) == 1:
            row, sp, mtype = hits[0]
            sp_insurance_now = sp.get('insurance', '') or ''
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
                'carrier_name':           row.get('insurance_company', ''),
                'estimator_first_name':   estimator_first_name(row.get('estimator', '')),
                'insurance_needs_fix':    insurance_needs_correction(sp_insurance_now)
            })
        else:
            # Multiple report rows want the same SP item — all go to ambiguous
            for row, sp, mtype in hits:
                ambiguous.append({
                    'ro_number': row['ro_number'],
                    'owner': row['owner'],
                    'vehicle': row['vehicle'],
                    'candidate_ids': [sp.get('id')],
                    'reason': f"multiple report rows ({len(hits)}) matched the same SharePoint item — manual review needed"
                })

    return jsonify({
        'matched': matched,
        'unmatched_report_rows': unmatched,
        'ambiguous': ambiguous,
        'summary': {
            'report_rows_total': len(report_rows),
            'sharepoint_items': len(sharepoint_items),
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
    Writes phase, tech, painter, dates. Never writes Done/ActualDelivery."""
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
            'carrier_name':           row.get('insurance_company', ''),
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
    Writes Done/ActualDelivery for delivered vehicles. Final field sync."""
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
        report_rows = parse_vehicles_scheduled_out_xml(xml_bytes)
    except Exception as e:
        return jsonify({'error': f'Failed to parse XML report: {str(e)}'}), 400

    matched_pairs, unmatched, ambiguous = run_match_engine(report_rows, sharepoint_items)

    matched = []
    for row, sp, mtype in matched_pairs:
        sp_insurance_now = sp.get('insurance', '') or ''
        is_delivered = row.get('is_delivered', False)

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
            'is_total_loss':          row.get('total_loss', False),
            'carrier_name':           row.get('insurance_company', ''),
            'estimator_first_name':   estimator_first_name(row.get('estimator', '')),
            # Conditional fields
            'insurance_needs_fix':    insurance_needs_fix_or_blank(sp_insurance_now),
        })

    return jsonify({
        'matched': matched,
        'unmatched_report_rows': unmatched,
        'ambiguous': ambiguous,
        'summary': {
            'report_rows_total': len(report_rows),
            'sharepoint_items': len(sharepoint_items),
            'matched': len(matched),
            'unmatched': len(unmatched),
            'ambiguous': len(ambiguous),
        }
    })

# ─── /health endpoint (unchanged) ─────────────────────────────────

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
