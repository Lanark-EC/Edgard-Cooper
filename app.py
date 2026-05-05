from flask import Flask, render_template, request, send_file, jsonify, redirect, url_for, flash
import io, traceback, json, os, shutil
from datetime import datetime, date
from pathlib import Path

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 150 * 1024 * 1024  # 150MB max upload
app.secret_key = "ec_scripthub_secret"

# ─── Script Hub imports ────────────────────────────────────────
from scripts.launch_check import run_launch_check
from scripts.garvis_export import run_garvis_export

# ─── Promo Uplift data paths ───────────────────────────────────
DATA_DIR           = Path("data")
BASELINE_PATH      = DATA_DIR / "baseline.json"
BASELINE_META_PATH = DATA_DIR / "baseline_meta.json"
DB_PATH            = DATA_DIR / "promo_db.json"
UPLOADS_DIR        = Path("uploads")
DATA_DIR.mkdir(exist_ok=True)
UPLOADS_DIR.mkdir(exist_ok=True)

EXECUTION_THRESHOLD = 20

# ─── Promo helpers ─────────────────────────────────────────────
def load_json(path, default):
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return default

def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)

def parse_baseline(filepath):
    import pandas as pd
    import gc
    xl = pd.ExcelFile(filepath, engine="openpyxl")
    result = {"forecast": {}, "actuals": {}}
    sheet_map = {
        "forecast": [s for s in xl.sheet_names if "forecast" in s.lower()],
        "actuals":  [s for s in xl.sheet_names if "actual"   in s.lower()],
    }
    for kind, sheets in sheet_map.items():
        if not sheets:
            continue

        # Read only first row to detect columns
        df_head = pd.read_excel(filepath, sheet_name=sheets[0],
                                nrows=1, engine="openpyxl")
        col_map = {str(c).strip().lower(): c for c in df_head.columns}

        def find_col(candidates):
            for c in candidates:
                if c.lower() in col_map:
                    return col_map[c.lower()]
            return None

        chain_col   = find_col(["chain", "customer", "client"])
        prod_col    = find_col(["productid", "product_id", "product id", "sap", "material", "sku"])
        country_col = find_col(["country", "market"])

        # Fallback to positional
        cols = list(df_head.columns)
        if not chain_col:   chain_col   = cols[0] if len(cols) > 0 else None
        if not prod_col:    prod_col    = cols[1] if len(cols) > 1 else None
        if not country_col: country_col = cols[2] if len(cols) > 2 else None

        id_cols   = [c for c in [chain_col, prod_col, country_col] if c]
        date_cols = [c for c in df_head.columns if c not in id_cols]

        # Pre-parse date column names once
        date_map = {}
        for col in date_cols:
            col_str = str(col).strip()
            d = None
            for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y"):
                try:
                    d = datetime.strptime(col_str, fmt).date()
                    break
                except ValueError:
                    continue
            if d is None:
                try:
                    parsed = pd.to_datetime(col, errors="coerce")
                    if not pd.isna(parsed):
                        d = parsed.date()
                except Exception:
                    pass
            if d is not None:
                date_map[col] = d.isoformat()

        valid_date_cols = list(date_map.keys())

        # Read in chunks to avoid memory overflow
        CHUNK = 2000
        sheet_data = pd.read_excel(filepath, sheet_name=sheets[0],
                                   engine="openpyxl")

        for start in range(0, len(sheet_data), CHUNK):
            chunk = sheet_data.iloc[start:start + CHUNK]
            for _, row in chunk.iterrows():
                try:
                    chain   = str(row[chain_col]).strip()   if chain_col   else ""
                    country = str(row[country_col]).strip() if country_col else ""
                    raw_p   = row[prod_col] if prod_col else ""
                    prod    = str(int(raw_p)) if pd.notna(raw_p) and str(raw_p).strip() not in ("", "nan") else str(raw_p).strip()
                except Exception:
                    continue

                for col in valid_date_cols:
                    val = row[col]
                    if pd.isna(val):
                        continue
                    try:
                        key = f"{chain}__{prod}__{country}__{date_map[col]}"
                        result[kind][key] = float(val)
                    except Exception:
                        continue

            gc.collect()

        del sheet_data
        gc.collect()

    return result

def parse_promo(filepath, promo_name):
    import pandas as pd, datetime as dt

    df_raw = pd.read_excel(filepath, sheet_name="Forecast by exact #",
                           header=0, engine="openpyxl")

    # Detect if row 0 is a real header or a meta-row
    # Check if actual column names look like real headers already
    col0 = str(df_raw.columns[0]).strip().lower()
    real_header_in_row0 = col0 in ("country", "select", "chain") or col0.startswith("select")

    if real_header_in_row0:
        # Real headers are in row 0 of the data (pandas already read them as columns)
        # BUT the column names may be garbled ("Select \x80" etc) — use row 0 values instead
        first_row = list(df_raw.iloc[0])
        if str(first_row[0]).strip().lower() in ("country", "chain", "ean"):
            # Row 0 contains real headers, data starts at row 1
            real_headers = [str(v).strip() if pd.notna(v) else "" for v in first_row]
            df = df_raw.iloc[1:].reset_index(drop=True)
        else:
            # Column names ARE the headers already
            real_headers = [str(c).strip() for c in df_raw.columns]
            df = df_raw.reset_index(drop=True)
    else:
        real_headers = [str(c).strip() for c in df_raw.columns]
        df = df_raw.reset_index(drop=True)

    # Map column indices by searching real_headers
    def find_idx(candidates):
        for cand in candidates:
            for i, h in enumerate(real_headers):
                if cand.lower() == h.lower() or cand.lower() in h.lower():
                    return i
        return None

    idx_country = find_idx(["Country"])
    idx_chain   = find_idx(["Chain"])
    idx_sap     = find_idx(["SAP code", "SAP", "ProductID", "product_id", "Material"])
    idx_desc    = find_idx(["Description", "Desc", "Product"])
    idx_subtype = find_idx(["Demand Type", "Subtype", "Type"])
    idx_demand  = find_idx(["Detail", "Demand"])

    # Week columns: anything after the last known id column that looks like a date/week
    last_id_idx = max(i for i in [idx_country, idx_chain, idx_sap, idx_desc, idx_subtype, idx_demand] if i is not None)
    import re
    raw_cols = list(df_raw.columns)
    week_col_indices = []
    for i in range(last_id_idx + 1, len(real_headers)):
        h = real_headers[i]
        # Include if it looks like a week (2026-W13) or date
        if re.match(r"\d{4}[-_]?W\d{1,2}", h, re.IGNORECASE): week_col_indices.append(i)
        elif re.match(r"\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}", h): week_col_indices.append(i)
        elif re.match(r"\d{4}[/\-]\d{1,2}[/\-]\d{1,2}", h): week_col_indices.append(i)
        # Also include unnamed/blank headers that follow week columns (Excel date columns often unnamed)
        elif not h or h.startswith("Unnamed"): week_col_indices.append(i)

    entries = []
    for _, row in df.iterrows():
        def get(idx):
            if idx is None: return ""
            v = row.iloc[idx]
            return str(v).strip() if pd.notna(v) else ""

        country = get(idx_country)
        chain   = get(idx_chain)
        sap     = get(idx_sap)
        desc    = get(idx_desc)
        subtype = get(idx_subtype)
        demand  = get(idx_demand)

        # Clean SAP: remove decimals like "1000338.0"
        if sap and sap not in ("", "nan"):
            try:
                sap = str(int(float(sap)))
            except Exception:
                pass
        else:
            continue

        weeks = []
        for wi in week_col_indices:
            if wi >= len(row):
                continue
            val = row.iloc[wi]
            if pd.isna(val):
                continue
            try:
                fval = float(val)
            except Exception:
                continue
            if fval == 0:
                continue

            week_label = real_headers[wi] if wi < len(real_headers) else ""

            # Convert week label to a date
            # Handles: "2026-W13", "2026W13", "W13 2026", datetime objects, DD/MM/YYYY
            week_date = None
            raw_col = raw_cols[wi] if wi < len(raw_cols) else None

            if isinstance(raw_col, dt.datetime):
                week_date = raw_col.date().isoformat()
            else:
                label = str(week_label).strip()
                m = re.match(r"(\d{4})[-_]?W(\d{1,2})", label, re.IGNORECASE)
                if m:
                    year, week = int(m.group(1)), int(m.group(2))
                    try:
                        d = dt.datetime.strptime(f"{year}-W{week:02d}-1", "%G-W%V-%u").date()
                        week_date = d.isoformat()
                    except Exception:
                        week_date = f"{year}-W{week:02d}"
                else:
                    # Try parsing as date string
                    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%m/%d/%Y"):
                        try:
                            week_date = dt.datetime.strptime(label, fmt).date().isoformat()
                            break
                        except ValueError:
                            continue
                    if not week_date:
                        try:
                            week_date = pd.to_datetime(raw_col, errors="coerce").date().isoformat()
                        except Exception:
                            week_date = label  # fallback: use label as-is

            if week_date:
                weeks.append({"date": week_date, "label": week_label, "units": fval})

        if weeks:
            entries.append({"country": country, "chain": chain, "sap": sap,
                            "desc": desc, "subtype": subtype, "demand": demand, "weeks": weeks})
    return entries

def calculate_uplift(promo_entries, baseline):
    forecast, actuals, results = baseline.get("forecast", {}), baseline.get("actuals", {}), []
    for entry in promo_entries:
        chain, sap, country = entry["chain"], entry["sap"], entry["country"]
        total_forecast = total_actual = total_promo_units = missing_fc = missing_act = 0
        week_details = []
        for w in entry["weeks"]:
            d, key = w["date"], f"{chain}__{sap}__{country}__{w['date']}"
            fc, act = forecast.get(key), actuals.get(key)
            total_promo_units += w["units"]
            if fc  is not None: total_forecast += fc
            else: missing_fc += 1
            if act is not None: total_actual   += act
            else: missing_act += 1
            week_details.append({"date": d, "label": w["label"],
                                  "promo_units": w["units"], "forecast": fc, "actual": act})
        uplift_units = total_actual - total_forecast if missing_act == 0 and missing_fc == 0 else None
        uplift_pct   = round(uplift_units / total_forecast * 100, 1) if (uplift_units is not None and total_forecast > 0) else None
        auto_status  = "no_data" if uplift_pct is None else ("suspect" if uplift_pct < EXECUTION_THRESHOLD else "confirmed")
        results.append({**entry, "weeks": week_details,
                        "total_promo_units": total_promo_units,
                        "total_forecast": round(total_forecast, 1),
                        "total_actual":   round(total_actual,   1),
                        "uplift_units":   round(uplift_units,   1) if uplift_units is not None else None,
                        "uplift_pct":     uplift_pct, "auto_status": auto_status,
                        "override": None, "missing_fc": missing_fc, "missing_act": missing_act})
    return results

# ═══════════════════════════════════════════════════════════════
# ROUTES — Script Hub
# ═══════════════════════════════════════════════════════════════

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/run/launch-check", methods=["POST"])
def api_launch_check():
    try:
        orders_file = request.files.get("orders_file")
        launch_file = request.files.get("launch_file")
        if not orders_file or not launch_file:
            return jsonify({"error": "Upload both files."}), 400
        output_buf, stats = run_launch_check(orders_file, launch_file)
        return send_file(output_buf,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name=f"launch_check_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx")
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route("/run/garvis-export", methods=["POST"])
def api_garvis_export():
    try:
        garvis_file = request.files.get("garvis_file")
        if not garvis_file:
            return jsonify({"error": "Upload a Garvis export file."}), 400
        output_buf, stats = run_garvis_export(garvis_file)
        return send_file(output_buf,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name=f"GARVIS_OVERVIEW_{datetime.now().strftime('%Y%m%dT%H%M')}.xlsx")
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# ═══════════════════════════════════════════════════════════════
# ROUTES — Promo Uplift
# ═══════════════════════════════════════════════════════════════

@app.route("/promo-uplift")
def promo_uplift():
    db            = load_json(DB_PATH, [])
    baseline_meta = load_json(BASELINE_META_PATH, None)
    confirmed     = [p for p in db if (p.get("override") or p.get("auto_status")) == "confirmed"]
    uplift_vals   = [p["uplift_pct"] for p in confirmed if p.get("uplift_pct") is not None]
    avg_uplift    = round(sum(uplift_vals) / len(uplift_vals), 1) if uplift_vals else None
    return render_template("promo_uplift.html",
        promos=db, baseline_meta=baseline_meta,
        total_promos=len(db), confirmed=len(confirmed), avg_uplift=avg_uplift)

import threading

# Track background job status
_job_status = {}  # job_id -> {"status": "running"|"done"|"error", "msg": "..."}

def _process_baseline_bg(path, filename, job_id):
    """Run baseline parsing in a background thread."""
    try:
        _job_status[job_id] = {"status": "running", "msg": "Parsing file..."}
        baseline = parse_baseline(str(path))
        save_json(BASELINE_PATH, baseline)
        save_json(BASELINE_META_PATH, {
            "filename": filename,
            "uploaded_at": datetime.now().isoformat(),
            "forecast_rows": len(baseline["forecast"]),
            "actuals_rows":  len(baseline["actuals"]),
        })
        # Recalculate existing promos
        db = load_json(DB_PATH, [])
        for promo in db:
            updated = calculate_uplift(promo["entries"], baseline)
            override_map = {(e["sap"], e["chain"], e["country"]): e.get("override") for e in promo["entries"]}
            for entry in updated:
                entry["override"] = override_map.get((entry["sap"], entry["chain"], entry["country"]))
            promo["entries"] = updated
            promo["recalculated_at"] = datetime.now().isoformat()
        save_json(DB_PATH, db)
        _job_status[job_id] = {
            "status": "done",
            "msg": f"Baseline updated — {len(baseline['forecast'])} forecast rows, {len(baseline['actuals'])} actuals rows."
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        _job_status[job_id] = {"status": "error", "msg": f"{type(e).__name__}: {e}"}

@app.route("/upload_baseline", methods=["POST"])
def upload_baseline():
    f = request.files.get("baseline_file")
    if not f or not f.filename:
        flash("No file selected.", "error")
        return redirect(url_for("promo_uplift"))
    path = UPLOADS_DIR / "baseline_latest.xlsx"
    f.save(path)

    # Save pending status immediately
    job_id = datetime.now().strftime('%Y%m%d_%H%M%S%f')
    _job_status[job_id] = {"status": "running", "msg": "Processing..."}
    save_json(BASELINE_META_PATH, {
        "filename": f.filename,
        "uploaded_at": datetime.now().isoformat(),
        "forecast_rows": "processing...",
        "actuals_rows": "processing...",
        "job_id": job_id,
        "processing": True,
    })

    # Start background thread
    t = threading.Thread(target=_process_baseline_bg, args=(path, f.filename, job_id), daemon=True)
    t.start()

    flash(f"Baseline file '{f.filename}' uploaded. Processing in background — this may take 1–2 minutes for large files. Refresh this page to see when it's ready.", "success")
    return redirect(url_for("promo_uplift"))

@app.route("/baseline_status")
def baseline_status():
    """Poll endpoint to check background job status."""
    meta = load_json(BASELINE_META_PATH, None)
    if not meta:
        return jsonify({"status": "none"})
    job_id = meta.get("job_id")
    if not job_id or not meta.get("processing"):
        return jsonify({"status": "done", "msg": "Baseline active."})
    job = _job_status.get(job_id, {"status": "running", "msg": "Processing..."})
    if job["status"] == "done":
        # Update meta to remove processing flag
        meta["processing"] = False
        save_json(BASELINE_META_PATH, meta)
    return jsonify(job)



def _process_promo_bg(path, filename, promo_name, promo_id, job_id):
    """Run promo parsing in a background thread."""
    try:
        _job_status[job_id] = {"status": "running", "msg": "Parsing promo file..."}
        entries = parse_promo(str(path), promo_name)
        if not entries:
            _job_status[job_id] = {"status": "error", "msg": "No promo data found in the file."}
            # Remove the placeholder from db
            db = load_json(DB_PATH, [])
            db = [p for p in db if p["id"] != promo_id]
            save_json(DB_PATH, db)
            return
        baseline = load_json(BASELINE_PATH, {})
        enriched = calculate_uplift(entries, baseline)
        db = load_json(DB_PATH, [])
        for p in db:
            if p["id"] == promo_id:
                p["sku_count"] = len(enriched)
                p["entries"]   = enriched
                p["processing"] = False
                break
        save_json(DB_PATH, db)
        _job_status[job_id] = {"status": "done", "msg": f"'{promo_name}' ready — {len(enriched)} SKUs.", "promo_id": promo_id}
    except Exception as e:
        import traceback
        traceback.print_exc()
        _job_status[job_id] = {"status": "error", "msg": f"{type(e).__name__}: {e}"}
        db = load_json(DB_PATH, [])
        db = [p for p in db if p["id"] != promo_id]
        save_json(DB_PATH, db)

@app.route("/upload_promo", methods=["POST"])
def upload_promo():
    f          = request.files.get("promo_file")
    promo_name = request.form.get("promo_name", "").strip()
    if not f or not f.filename:
        flash("No file selected.", "error")
        return redirect(url_for("promo_uplift"))
    if not promo_name:
        promo_name = f.filename.replace(".xlsx", "").replace("_", " ")
    path     = UPLOADS_DIR / f"promo_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    f.save(path)

    # Add placeholder to DB immediately
    promo_id = f"promo_{datetime.now().strftime('%Y%m%d_%H%M%S%f')}"
    job_id   = f"job_{promo_id}"
    db = load_json(DB_PATH, [])
    db.append({"id": promo_id, "name": promo_name, "filename": f.filename,
               "uploaded_at": datetime.now().isoformat(),
               "sku_count": 0, "entries": [], "processing": True, "job_id": job_id})
    save_json(DB_PATH, db)

    # Start background thread
    t = threading.Thread(target=_process_promo_bg,
                         args=(path, f.filename, promo_name, promo_id, job_id), daemon=True)
    t.start()

    flash(f"Promo '{promo_name}' uploaded. Processing in background — refresh in a moment.", "success")
    return redirect(url_for("promo_uplift"))

@app.route("/promo_status/<job_id>")
def promo_status(job_id):
    job = _job_status.get(job_id, {"status": "running", "msg": "Processing..."})
    return jsonify(job)



@app.route("/promo/<promo_id>")
def promo_detail(promo_id):
    db    = load_json(DB_PATH, [])
    promo = next((p for p in db if p["id"] == promo_id), None)
    if not promo:
        flash("Promo not found.", "error")
        return redirect(url_for("promo_uplift"))
    return render_template("promo_detail.html", promo=promo, threshold=EXECUTION_THRESHOLD)

@app.route("/promo/<promo_id>/override", methods=["POST"])
def set_override(promo_id):
    data    = request.get_json()
    sap, chain, country, status = data.get("sap"), data.get("chain"), data.get("country"), data.get("status")
    db      = load_json(DB_PATH, [])
    promo   = next((p for p in db if p["id"] == promo_id), None)
    if not promo:
        return jsonify({"error": "not found"}), 404
    for entry in promo["entries"]:
        if entry["sap"] == sap and entry["chain"] == chain and entry["country"] == country:
            entry["override"] = status or None
            break
    save_json(DB_PATH, db)
    return jsonify({"ok": True})

@app.route("/promo/<promo_id>/delete", methods=["POST"])
def delete_promo(promo_id):
    db = load_json(DB_PATH, [])
    db = [p for p in db if p["id"] != promo_id]
    save_json(DB_PATH, db)
    flash("Promo deleted.", "success")
    return redirect(url_for("promo_uplift"))

@app.route("/api/prefill")
def prefill():
    chain, sap, country = request.args.get("chain",""), request.args.get("sap",""), request.args.get("country","")
    db, matches = load_json(DB_PATH, []), []
    for promo in db:
        for entry in promo["entries"]:
            if (entry.get("override") or entry.get("auto_status")) != "confirmed": continue
            if entry.get("uplift_pct") is None: continue
            if chain   and entry["chain"]   != chain:   continue
            if sap     and entry["sap"]     != sap:     continue
            if country and entry["country"] != country: continue
            matches.append({"promo": promo["name"], "uplift_pct": entry["uplift_pct"]})
    if not matches:
        return jsonify({"found": False})
    vals = [m["uplift_pct"] for m in matches]
    return jsonify({"found": True, "count": len(matches),
                    "avg": round(sum(vals)/len(vals),1), "min": min(vals), "max": max(vals), "history": matches})

if __name__ == "__main__":
    app.run(debug=True, port=5000)
