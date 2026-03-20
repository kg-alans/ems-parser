from flask import Flask, request, jsonify
import tempfile
import os
import base64
from dbfread import DBF

app = Flask(__name__)

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

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
