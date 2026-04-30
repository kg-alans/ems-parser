from flask import Flask, request, jsonify
import tempfile
import os
import base64
import re
import xml.etree.ElementTree as ET
from dbfread import DBF

app = Flask(__name__)

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

# ─── /match-ro-report endpoint ────────────────────────────────────

@app.route('/match-ro-report', methods=['POST'])
def match_ro_report():
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No JSON body received'}), 400
    if 'xml' not in data:
        return jsonify({'error': 'Missing xml field (base64 of report XML)'}), 400

    sharepoint_items = data.get('sharepoint_items', [])

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

# ─── /health endpoint (unchanged) ─────────────────────────────────

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
