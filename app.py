from flask import Flask, render_template, request, send_file, jsonify, redirect, url_for, flash
import io, traceback, json, os
from datetime import datetime, date
from pathlib import Path

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 150 * 1024 * 1024
app.secret_key = "ec_scripthub_secret"

# ─── Script Hub imports ────────────────────────────────────────
from scripts.launch_check import run_launch_check
from scripts.garvis_export import run_garvis_export

# ─── Uploads dir (temp only) ──────────────────────────────────
UPLOADS_DIR = Path("uploads")
UPLOADS_DIR.mkdir(exist_ok=True)

EXECUTION_THRESHOLD = 20

# ═══════════════════════════════════════════════════════════════
# DATABASE LAYER — PostgreSQL
# ═══════════════════════════════════════════════════════════════
import psycopg2
from psycopg2.extras import Json

_db_conn = None

def get_db():
    global _db_conn
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL environment variable not set.")
    try:
        if _db_conn is None or _db_conn.closed:
            raise Exception("reconnect")
        _db_conn.cursor().execute("SELECT 1")
    except Exception:
        _db_conn = psycopg2.connect(db_url, sslmode="require")
        _db_conn.autocommit = True
    return _db_conn

def init_db():
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS kv_store (
            key        TEXT PRIMARY KEY,
            value      JSONB NOT NULL,
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.close()

def db_get(key, default=None):
    try:
        cur = get_db().cursor()
        cur.execute("SELECT value FROM kv_store WHERE key = %s", (key,))
        row = cur.fetchone()
        cur.close()
        return row[0] if row else default
    except Exception as e:
        print(f"db_get error: {e}")
        return default

def db_set(key, value):
    try:
        cur = get_db().cursor()
        cur.execute("""
            INSERT INTO kv_store (key, value, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (key) DO UPDATE
              SET value = EXCLUDED.value, updated_at = NOW()
        """, (key, Json(value)))
        cur.close()
    except Exception as e:
        print(f"db_set error: {e}")

try:
    init_db()
    print("Database initialized.")
except Exception as e:
    print(f"Database init failed: {e}")

# ─── Key constants ────────────────────────────────────────────
KEY_BASELINE      = "baseline"
KEY_BASELINE_META = "baseline_meta"
KEY_PROMO_DB      = "promo_db"

def load_json(key, default):
    return db_get(key, default)

def save_json(key, data):
    db_set(key, data)

def parse_and_save_baseline(filepath, filename, job_id):
    """
    Parse baseline Excel and save directly to DB in small chunks.
    Never holds the full dataset in memory at once.
    """
    import pandas as pd, gc

    _set_job(job_id, "running", "Opening Excel file...")

    xl = pd.ExcelFile(filepath, engine="openpyxl")
    sheet_map = {
        "forecast": next((s for s in xl.sheet_names if "forecast" in s.lower()), None),
        "actuals":  next((s for s in xl.sheet_names if "actual"   in s.lower()), None),
    }

    CHUNK_SIZE = 5000  # rows per read chunk
    DB_CHUNK   = 8000  # key-value pairs per DB write

    totals = {"forecast": 0, "actuals": 0}

    for kind, sheet in sheet_map.items():
        if not sheet:
            db_set(f"baseline_{kind}_chunks", {"count": 0, "total": 0})
            continue

        _set_job(job_id, "running", f"Reading {kind} sheet...")

        # Read header row only to detect columns
        df_head = pd.read_excel(filepath, sheet_name=sheet,
                                nrows=1, engine="openpyxl")
        col_map = {str(c).strip().lower(): c for c in df_head.columns}

        def find_col(candidates):
            for c in candidates:
                if c.lower() in col_map:
                    return col_map[c.lower()]
            return None

        chain_col   = find_col(["chain","customer","client"]) or df_head.columns[0]
        prod_col    = find_col(["productid","product_id","product id","sap","material","sku"]) or df_head.columns[1]
        country_col = find_col(["country","market"]) or df_head.columns[2]
        id_cols     = [chain_col, prod_col, country_col]
        date_cols   = [c for c in df_head.columns if c not in id_cols]

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

        # Get total rows for progress
        total_rows = 0
        db_chunk_idx = 0
        current_chunk = {}

        # Read full sheet but process in pandas chunks
        _set_job(job_id, "running", f"Processing {kind} data (this takes a few minutes)...")

        df_full = pd.read_excel(filepath, sheet_name=sheet, engine="openpyxl")
        total_rows = len(df_full)

        for row_start in range(0, total_rows, CHUNK_SIZE):
            chunk_df = df_full.iloc[row_start:row_start + CHUNK_SIZE]

            for _, row in chunk_df.iterrows():
                try:
                    chain   = str(row[chain_col]).strip()
                    country = str(row[country_col]).strip()
                    raw_p   = row[prod_col]
                    prod    = str(int(raw_p)) if pd.notna(raw_p) and str(raw_p).strip() not in ("","nan") else str(raw_p).strip()
                except Exception:
                    continue

                for col in valid_date_cols:
                    val = row[col]
                    if pd.isna(val):
                        continue
                    try:
                        key = f"{chain}__{prod}__{country}__{date_map[col]}"
                        current_chunk[key] = float(val)
                    except Exception:
                        continue

                    # Flush to DB when chunk is full
                    if len(current_chunk) >= DB_CHUNK:
                        db_set(f"baseline_{kind}_chunk_{db_chunk_idx}", current_chunk)
                        totals[kind] += len(current_chunk)
                        db_chunk_idx += 1
                        current_chunk = {}

            pct = min(100, int((row_start + CHUNK_SIZE) / total_rows * 100))
            _set_job(job_id, "running", f"Processing {kind}: {pct}% ({totals[kind]:,} entries saved)...")
            gc.collect()

        # Flush remaining
        if current_chunk:
            db_set(f"baseline_{kind}_chunk_{db_chunk_idx}", current_chunk)
            totals[kind] += len(current_chunk)
            db_chunk_idx += 1

        db_set(f"baseline_{kind}_chunks", {"count": db_chunk_idx, "total": totals[kind]})
        del df_full
        gc.collect()

    db_set(KEY_BASELINE_META, {
        "filename": filename,
        "uploaded_at": datetime.now().isoformat(),
        "forecast_rows": totals["forecast"],
        "actuals_rows":  totals["actuals"],
        "processing": False,
    })
    return totals

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
            entries.append({"country": normalize_country(country), "chain": chain, "sap": sap,
                            "desc": desc, "subtype": subtype, "demand": demand, "weeks": weeks})
    return entries

def load_baseline():
    """Load baseline from chunked DB storage."""
    result = {"forecast": {}, "actuals": {}}
    for kind, prefix in [("forecast", "baseline_forecast"), ("actuals", "baseline_actuals")]:
        meta = db_get(f"{prefix}_chunks")
        if not meta:
            continue
        for i in range(meta["count"]):
            chunk = db_get(f"{prefix}_chunk_{i}", {})
            result[kind].update(chunk)
    return result

# Country name → ISO code mapping
COUNTRY_MAP = {
    "france": "FR", "frankrijk": "FR",
    "belgium": "BE", "belgië": "BE", "belgie": "BE", "belgique": "BE",
    "netherlands": "NL", "nederland": "NL", "pays-bas": "NL",
    "germany": "DE", "deutschland": "DE", "allemagne": "DE",
    "italy": "IT", "italia": "IT", "italie": "IT",
    "spain": "ES", "españa": "ES", "espagne": "ES",
    "united kingdom": "GB", "uk": "GB", "great britain": "GB",
    "sweden": "SE", "sverige": "SE",
    "denmark": "DK", "danmark": "DK",
    "norway": "NO", "norge": "NO",
    "finland": "FI", "suomi": "FI",
    "austria": "AT", "österreich": "AT",
    "switzerland": "CH", "schweiz": "CH", "suisse": "CH",
    "portugal": "PT",
    "poland": "PL", "polska": "PL",
    "czech republic": "CZ", "czechia": "CZ",
}

def normalize_country(c):
    """Convert full country name to ISO code if possible."""
    if not c:
        return c
    lower = c.strip().lower()
    return COUNTRY_MAP.get(lower, c.strip())

def get_surrounding_weeks(date_str, n=1):
    """Return ISO date strings for n weeks before and after a given date."""
    from datetime import timedelta
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
        return [
            (d - timedelta(weeks=i)).isoformat() for i in range(n, 0, -1)
        ] + [date_str] + [
            (d + timedelta(weeks=i)).isoformat() for i in range(1, n + 1)
        ]
    except Exception:
        return [date_str]

def calculate_uplift(promo_entries, baseline):
    forecast, actuals, results = baseline.get("forecast", {}), baseline.get("actuals", {}), []
    for entry in promo_entries:
        chain   = entry["chain"]
        sap     = entry["sap"]
        country = normalize_country(entry["country"])
        # Use active_week_date override if set (from F feature)
        active_week_override = entry.get("active_week_override", {})

        total_forecast = total_actual = total_promo_units = missing_fc = missing_act = 0
        week_details = []

        for w in entry["weeks"]:
            d   = w["date"]
            key = f"{chain}__{sap}__{country}__{d}"
            fc  = forecast.get(key)
            act = actuals.get(key)

            # Handle both old format (promo_units) and new format (units)
            promo_units = w.get("units") or w.get("promo_units") or 0

            # Build surrounding weeks context (+/- 1 week)
            surrounding = []
            for sd in get_surrounding_weeks(d, n=1):
                skey = f"{chain}__{sap}__{country}__{sd}"
                surrounding.append({
                    "date":   sd,
                    "actual": actuals.get(skey),
                    "is_promo_week": sd == d,
                })

            # Check if user selected a different active week for this SKU+week
            override_date = active_week_override.get(d)
            if override_date:
                act_key = f"{chain}__{sap}__{country}__{override_date}"
                act = actuals.get(act_key)

            total_promo_units += promo_units
            if fc  is not None: total_forecast += fc
            else: missing_fc += 1
            if act is not None: total_actual += act
            else: missing_act += 1

            week_details.append({
                "date": d, "label": w.get("label", d),
                "promo_units": promo_units,
                "forecast": fc,
                "actual": act,
                "active_date": override_date or d,
                "surrounding": surrounding,
            })

        expected     = round(total_forecast + total_promo_units, 1)
        uplift_units = round(total_actual - expected, 1) if missing_act == 0 and missing_fc == 0 else None
        uplift_pct   = round(uplift_units / expected * 100, 1) if (uplift_units is not None and expected > 0) else None
        auto_status  = "no_data" if uplift_pct is None else ("suspect" if uplift_pct < EXECUTION_THRESHOLD else "confirmed")
        results.append({**entry, "weeks": week_details,
                        "total_promo_units": total_promo_units,
                        "total_forecast": round(total_forecast, 1),
                        "total_actual":   round(total_actual,   1),
                        "expected":       expected,
                        "uplift_units":   uplift_units,
                        "uplift_pct":     uplift_pct, "auto_status": auto_status,
                        "override": None, "missing_fc": missing_fc, "missing_act": missing_act,
                        "active_week_override": active_week_override})
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
    db            = load_json(KEY_PROMO_DB, [])
    baseline_meta = load_json(KEY_BASELINE_META, None)
    confirmed     = [p for p in db if (p.get("override") or p.get("auto_status")) == "confirmed"]
    uplift_vals   = [p["uplift_pct"] for p in confirmed if p.get("uplift_pct") is not None]
    avg_uplift    = round(sum(uplift_vals) / len(uplift_vals), 1) if uplift_vals else None
    return render_template("promo_uplift.html",
        promos=db, baseline_meta=baseline_meta,
        total_promos=len(db), confirmed=len(confirmed), avg_uplift=avg_uplift)

import threading

def _set_job(job_id, status, msg):
    db_set(f"job_{job_id}", {"status": status, "msg": msg})

def _get_job(job_id):
    return db_get(f"job_{job_id}", {"status": "running", "msg": "Processing..."})

def _process_baseline_bg(path, filename, job_id):
    try:
        totals = parse_and_save_baseline(str(path), filename, job_id)

        # Recalculate existing promos if any — but only load baseline once
        db = load_json(KEY_PROMO_DB, [])
        if db:
            _set_job(job_id, "running", "Recalculating existing promos...")
            try:
                for promo in db:
                    baseline = lookup_baseline_for_entries(promo["entries"])
                    updated = calculate_uplift(promo["entries"], baseline)
                    override_map = {(e["sap"], e["chain"], e["country"]): e.get("override") for e in promo["entries"]}
                    for entry in updated:
                        entry["override"] = override_map.get((entry["sap"], entry["chain"], entry["country"]))
                    promo["entries"] = updated
                    promo["recalculated_at"] = datetime.now().isoformat()
                save_json(KEY_PROMO_DB, db)
            except Exception as e:
                # Don't fail the whole job if recalc fails — baseline is saved successfully
                print(f"Promo recalc failed (non-critical): {e}")
                _set_job(job_id, "done",
                         f"Baseline ready — {totals['forecast']:,} forecast + {totals['actuals']:,} actuals rows. "
                         f"Note: existing promos could not be auto-recalculated (use ↻ Recalculate on each promo).")
                return

        _set_job(job_id, "done",
                 f"Baseline ready — {totals['forecast']:,} forecast + {totals['actuals']:,} actuals rows.")
    except Exception as e:
        import traceback
        traceback.print_exc()
        _set_job(job_id, "error", f"{type(e).__name__}: {e}")

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
    _set_job(job_id, "running", "Processing...")
    save_json(KEY_BASELINE_META, {
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
    meta = load_json(KEY_BASELINE_META, None)
    if not meta:
        return jsonify({"status": "none"})
    job_id = meta.get("job_id")
    if not job_id or not meta.get("processing"):
        return jsonify({"status": "done", "msg": "Baseline active."})
    job = _get_job(job_id)
    if job["status"] == "done":
        meta["processing"] = False
        save_json(KEY_BASELINE_META, meta)
    return jsonify(job)



def _process_promo_bg(path, filename, promo_name, promo_id, job_id):
    """Run promo parsing in a background thread."""
    try:
        _set_job(job_id, "running", "Parsing promo file...")
        entries = parse_promo(str(path), promo_name)
        if not entries:
            _set_job(job_id, "error", "No promo data found in the file.")
            db = load_json(KEY_PROMO_DB, [])
            save_json(KEY_PROMO_DB, [p for p in db if p["id"] != promo_id])
            return
        baseline = lookup_baseline_for_entries(entries)
        _set_job(job_id, "running", f"Calculating uplift for {len(entries)} SKUs...")
        enriched = calculate_uplift(entries, baseline)
        db = load_json(KEY_PROMO_DB, [])
        for p in db:
            if p["id"] == promo_id:
                p["sku_count"] = len(enriched)
                p["entries"]   = enriched
                p["processing"] = False
                break
        save_json(KEY_PROMO_DB, db)
        _set_job(job_id, "done", f"'{promo_name}' ready — {len(enriched)} SKUs.")
    except Exception as e:
        import traceback
        traceback.print_exc()
        _set_job(job_id, "error", f"{type(e).__name__}: {e}")
        db = load_json(KEY_PROMO_DB, [])
        save_json(KEY_PROMO_DB, [p for p in db if p["id"] != promo_id])

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
    db = load_json(KEY_PROMO_DB, [])
    db.append({"id": promo_id, "name": promo_name, "filename": f.filename,
               "uploaded_at": datetime.now().isoformat(),
               "sku_count": 0, "entries": [], "processing": True, "job_id": job_id})
    save_json(KEY_PROMO_DB, db)

    # Start background thread
    t = threading.Thread(target=_process_promo_bg,
                         args=(path, f.filename, promo_name, promo_id, job_id), daemon=True)
    t.start()

    flash(f"Promo '{promo_name}' uploaded. Processing in background — refresh in a moment.", "success")
    return redirect(url_for("promo_uplift"))

@app.route("/promo_status/<job_id>")
def promo_status(job_id):
    return jsonify(_get_job(job_id))



def lookup_baseline_for_entries(entries):
    """Load only the baseline chunks that contain keys for the given entries."""
    needed_keys = set()
    for entry in entries:
        chain   = entry["chain"]
        sap     = entry["sap"]
        country = normalize_country(entry["country"])
        for w in entry.get("weeks", []):
            needed_keys.add(f"{chain}__{sap}__{country}__{w['date']}")
            for sd in get_surrounding_weeks(w["date"], n=1):
                needed_keys.add(f"{chain}__{sap}__{country}__{sd}")

    result = {"forecast": {}, "actuals": {}}
    for kind, prefix in [("forecast", "baseline_forecast"), ("actuals", "baseline_actuals")]:
        meta = db_get(f"{prefix}_chunks")
        if not meta:
            continue
        # Scan ALL chunks — don't stop early (keys are spread across chunks)
        for i in range(meta["count"]):
            chunk = db_get(f"{prefix}_chunk_{i}", {})
            for k in needed_keys:
                if k in chunk:
                    result[kind][k] = chunk[k]
    return result

@app.route("/promo/<promo_id>/recalculate", methods=["POST"])
def recalculate_promo(promo_id):
    db    = load_json(KEY_PROMO_DB, [])
    promo = next((p for p in db if p["id"] == promo_id), None)
    if not promo:
        flash("Promo not found.", "error")
        return redirect(url_for("promo_uplift"))
    try:
        for entry in promo["entries"]:
            entry["country"] = normalize_country(entry["country"])
        # Load only the baseline data needed for this promo's SKUs
        baseline = lookup_baseline_for_entries(promo["entries"])
        updated  = calculate_uplift(promo["entries"], baseline)
        override_map = {(e["sap"], e["chain"], e["country"]): e.get("override") for e in promo["entries"]}
        for entry in updated:
            entry["override"] = override_map.get((entry["sap"], entry["chain"], entry["country"]))
        promo["entries"] = updated
        promo["recalculated_at"] = datetime.now().isoformat()
        save_json(KEY_PROMO_DB, db)
        flash("Promo recalculated successfully.", "success")
    except Exception as e:
        flash(f"Error: {e}", "error")
    return redirect(url_for("promo_detail", promo_id=promo_id))

@app.route("/promo/<promo_id>/set_active_week", methods=["POST"])
def set_active_week(promo_id):
    """Set which week's actuals to use for a specific SKU+promo_week combination."""
    data         = request.get_json()
    sap          = data.get("sap")
    chain        = data.get("chain")
    country      = data.get("country")
    promo_date   = data.get("promo_date")   # original promo week date
    active_date  = data.get("active_date")  # selected actual week date

    db    = load_json(KEY_PROMO_DB, [])
    promo = next((p for p in db if p["id"] == promo_id), None)
    if not promo:
        return jsonify({"error": "not found"}), 404

    for entry in promo["entries"]:
        if entry["sap"] == sap and entry["chain"] == chain and entry["country"] == country:
            if "active_week_override" not in entry:
                entry["active_week_override"] = {}
            if active_date == promo_date:
                # Reset to default
                entry["active_week_override"].pop(promo_date, None)
            else:
                entry["active_week_override"][promo_date] = active_date

            # Recalculate this entry
            baseline = lookup_baseline_for_entries([entry])
            updated  = calculate_uplift([entry], baseline)
            if updated:
                u = updated[0]
                entry.update({k: u[k] for k in [
                    "weeks", "total_promo_units", "total_forecast", "total_actual",
                    "uplift_units", "uplift_pct", "auto_status", "active_week_override"
                ]})
            break

    save_json(KEY_PROMO_DB, db)
    return jsonify({"ok": True})

@app.route("/promo/<promo_id>")
def promo_detail(promo_id):
    db    = load_json(KEY_PROMO_DB, [])
    promo = next((p for p in db if p["id"] == promo_id), None)
    if not promo:
        flash("Promo not found.", "error")
        return redirect(url_for("promo_uplift"))
    return render_template("promo_detail.html", promo=promo, threshold=EXECUTION_THRESHOLD)

@app.route("/promo/<promo_id>/override", methods=["POST"])
def set_override(promo_id):
    data    = request.get_json()
    sap, chain, country, status = data.get("sap"), data.get("chain"), data.get("country"), data.get("status")
    db      = load_json(KEY_PROMO_DB, [])
    promo   = next((p for p in db if p["id"] == promo_id), None)
    if not promo:
        return jsonify({"error": "not found"}), 404
    for entry in promo["entries"]:
        if entry["sap"] == sap and entry["chain"] == chain and entry["country"] == country:
            entry["override"] = status or None
            break
    save_json(KEY_PROMO_DB, db)
    return jsonify({"ok": True})

@app.route("/promo/<promo_id>/delete", methods=["POST"])
def delete_promo(promo_id):
    db = load_json(KEY_PROMO_DB, [])
    db = [p for p in db if p["id"] != promo_id]
    save_json(KEY_PROMO_DB, db)
    flash("Promo deleted.", "success")
    return redirect(url_for("promo_uplift"))

@app.route("/api/prefill")
def prefill():
    chain, sap, country = request.args.get("chain",""), request.args.get("sap",""), request.args.get("country","")
    db, matches = load_json(KEY_PROMO_DB, []), []
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

@app.route("/debug/lookup")
def debug_lookup():
    chain   = request.args.get("chain", "Carrefour FR")
    prod    = request.args.get("prod", "1000338")
    country = request.args.get("country", "FR")
    date    = request.args.get("date", "2026-03-23")

    key_exact  = f"{chain}__{prod}__{country}__{date}"
    key_france = f"{chain}__{prod}__France__{date}"

    baseline = load_baseline()
    forecast = baseline.get("forecast", {})
    actuals  = baseline.get("actuals", {})

    # Search all keys for this product + chain combo
    search = f"{chain}__{prod}__"
    fc_matches  = [k for k in forecast if k.startswith(search)][:15]
    act_matches = [k for k in actuals  if k.startswith(search)][:15]

    return jsonify({
        "key_tried_FR":     key_exact,
        "key_tried_France": key_france,
        "in_forecast_FR":     key_exact  in forecast,
        "in_actuals_FR":      key_exact  in actuals,
        "in_forecast_France": key_france in forecast,
        "in_actuals_France":  key_france in actuals,
        "forecast_matches":  fc_matches,
        "actuals_matches":   act_matches,
        "total_forecast_keys": sum(db_get(f"baseline_forecast_chunks", {}).get("total", 0) for _ in [1]),
        "total_actuals_keys":  sum(db_get(f"baseline_actuals_chunks",  {}).get("total", 0) for _ in [1]),
    })

@app.route("/debug/full_reset")
def debug_full_reset():
    """Clear all baseline chunks, meta and promos completely."""
    # Clear baseline meta
    db_set(KEY_BASELINE_META, None)
    db_set(KEY_PROMO_DB, [])

    # Clear all forecast chunks
    fc_meta = db_get("baseline_forecast_chunks", {"count": 0})
    for i in range(fc_meta.get("count", 0) + 5):
        db_set(f"baseline_forecast_chunk_{i}", {})
    db_set("baseline_forecast_chunks", None)

    # Clear all actuals chunks
    act_meta = db_get("baseline_actuals_chunks", {"count": 0})
    for i in range(act_meta.get("count", 0) + 5):
        db_set(f"baseline_actuals_chunk_{i}", {})
    db_set("baseline_actuals_chunks", None)

    return jsonify({"ok": True, "msg": "Full reset complete. All baseline and promo data cleared."})

@app.route("/debug/clear_baseline")
def debug_clear_baseline():
    """Reset stuck baseline processing state."""
    db_set(KEY_BASELINE_META, None)
    return jsonify({"ok": True, "msg": "Baseline meta cleared. You can upload again."})

@app.route("/debug/job/<job_id>")
def debug_job(job_id):
    return jsonify(_get_job(job_id))

@app.route("/debug/meta")
def debug_meta():
    fc_meta  = db_get("baseline_forecast_chunks")
    act_meta = db_get("baseline_actuals_chunks")

    # Spot-check: look for Carrefour FR__1000338__FR__2026-03-23 across all chunks
    test_key = "Carrefour FR__1000338__FR__2026-03-23"
    found_in_fc  = False
    found_in_act = False
    if fc_meta:
        for i in range(fc_meta["count"]):
            chunk = db_get(f"baseline_forecast_chunk_{i}", {})
            if test_key in chunk:
                found_in_fc = chunk[test_key]
                break
    if act_meta:
        for i in range(act_meta["count"]):
            chunk = db_get(f"baseline_actuals_chunk_{i}", {})
            if test_key in chunk:
                found_in_act = chunk[test_key]
                break

    return jsonify({
        "baseline_meta":    db_get(KEY_BASELINE_META),
        "promo_db_count":   len(db_get(KEY_PROMO_DB, [])),
        "forecast_chunks":  fc_meta,
        "actuals_chunks":   act_meta,
        "spot_check_key":   test_key,
        "found_in_forecast": found_in_fc,
        "found_in_actuals":  found_in_act,
    })

if __name__ == "__main__":
    app.run(debug=True, port=5000)
