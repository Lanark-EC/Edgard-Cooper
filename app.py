from flask import Flask, render_template, request, send_file, jsonify, redirect, url_for, flash
import io, traceback, json, os, shutil
from datetime import datetime, date
from pathlib import Path

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024
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
    xl = pd.ExcelFile(filepath)
    result = {"forecast": {}, "actuals": {}}
    sheet_map = {
        "forecast": [s for s in xl.sheet_names if "forecast" in s.lower()],
        "actuals":  [s for s in xl.sheet_names if "actual"   in s.lower()],
    }
    for kind, sheets in sheet_map.items():
        if not sheets:
            continue
        df = pd.read_excel(filepath, sheet_name=sheets[0])
        id_cols  = ["Chain", "ProductID", "Country"]
        date_cols = [c for c in df.columns if c not in id_cols]
        for _, row in df.iterrows():
            chain   = str(row["Chain"]).strip()
            prod    = str(int(row["ProductID"])) if pd.notna(row["ProductID"]) else ""
            country = str(row["Country"]).strip()
            for col in date_cols:
                val = row[col]
                if pd.isna(val):
                    continue
                try:
                    d = datetime.strptime(str(col).strip(), "%d/%m/%Y").date()
                except ValueError:
                    continue
                key = f"{chain}__{prod}__{country}__{d.isoformat()}"
                result[kind][key] = float(val)
    return result

def parse_promo(filepath, promo_name):
    import pandas as pd, datetime as dt
    df_raw = pd.read_excel(filepath, sheet_name="Forecast by exact #", header=0)
    real_headers = list(df_raw.iloc[0])
    df = df_raw.iloc[1:].reset_index(drop=True)
    df.columns = df_raw.columns
    raw_cols  = list(df_raw.columns)
    week_labels = {raw_cols[i]: str(real_headers[i]) for i in range(10, len(raw_cols))}
    entries = []
    for _, row in df.iterrows():
        country = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ""
        chain   = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else ""
        sap     = str(row.iloc[4]).strip() if pd.notna(row.iloc[4]) else ""
        desc    = str(row.iloc[5]).strip() if pd.notna(row.iloc[5]) else ""
        subtype = str(row.iloc[6]).strip() if pd.notna(row.iloc[6]) else ""
        demand  = str(row.iloc[7]).strip() if pd.notna(row.iloc[7]) else ""
        if not sap or sap == "nan":
            continue
        weeks = []
        for col in raw_cols[10:]:
            val = row[col]
            if pd.notna(val) and float(val) != 0:
                if isinstance(col, dt.datetime):
                    week_date = col.date().isoformat()
                else:
                    try:
                        week_date = pd.to_datetime(col).date().isoformat()
                    except:
                        continue
                weeks.append({"date": week_date, "label": week_labels.get(col, ""), "units": float(val)})
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

@app.route("/upload_baseline", methods=["POST"])
def upload_baseline():
    f = request.files.get("baseline_file")
    if not f or not f.filename:
        flash("No file selected.", "error")
        return redirect(url_for("promo_uplift"))
    path = UPLOADS_DIR / "baseline_latest.xlsx"
    f.save(path)
    try:
        baseline = parse_baseline(str(path))
        save_json(BASELINE_PATH, baseline)
        save_json(BASELINE_META_PATH, {"filename": f.filename,
            "uploaded_at": datetime.now().isoformat(),
            "forecast_rows": len(baseline["forecast"]),
            "actuals_rows":  len(baseline["actuals"])})
        db = load_json(DB_PATH, [])
        for promo in db:
            updated = calculate_uplift(promo["entries"], baseline)
            override_map = {(e["sap"], e["chain"], e["country"]): e.get("override") for e in promo["entries"]}
            for entry in updated:
                entry["override"] = override_map.get((entry["sap"], entry["chain"], entry["country"]))
            promo["entries"] = updated
            promo["recalculated_at"] = datetime.now().isoformat()
        save_json(DB_PATH, db)
        flash(f"Baseline updated — {len(baseline['forecast'])} forecast rows, {len(baseline['actuals'])} actuals rows.", "success")
    except Exception as e:
        flash(f"Error parsing baseline: {e}", "error")
    return redirect(url_for("promo_uplift"))

@app.route("/upload_promo", methods=["POST"])
def upload_promo():
    f          = request.files.get("promo_file")
    promo_name = request.form.get("promo_name", "").strip()
    if not f or not f.filename:
        flash("No file selected.", "error")
        return redirect(url_for("promo_uplift"))
    if not promo_name:
        promo_name = f.filename.replace(".xlsx", "").replace("_", " ")
    path = UPLOADS_DIR / f"promo_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    f.save(path)
    try:
        entries  = parse_promo(str(path), promo_name)
        if not entries:
            flash("No promo data found in the file.", "error")
            return redirect(url_for("promo_uplift"))
        baseline = load_json(BASELINE_PATH, {})
        enriched = calculate_uplift(entries, baseline)
        db       = load_json(DB_PATH, [])
        promo_id = f"promo_{datetime.now().strftime('%Y%m%d_%H%M%S%f')}"
        db.append({"id": promo_id, "name": promo_name, "filename": f.filename,
                   "uploaded_at": datetime.now().isoformat(),
                   "sku_count": len(enriched), "entries": enriched})
        save_json(DB_PATH, db)
        flash(f"Promo '{promo_name}' uploaded — {len(enriched)} SKUs found.", "success")
        return redirect(url_for("promo_detail", promo_id=promo_id))
    except Exception as e:
        flash(f"Error parsing promo file: {e}", "error")
        return redirect(url_for("promo_uplift"))

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
