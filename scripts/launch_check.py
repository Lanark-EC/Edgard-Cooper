import pandas as pd
import numpy as np
import io
from pathlib import Path

# =========================
# USER SETTINGS
# =========================
HEADER_ROW_ORDERS = 0
HEADER_ROW_LAUNCH = 0
USE_COLUMN_LETTERS = False
MEANINGFUL_LAUNCH_THRESHOLD_PCT = 0.05

# =========================
# HELPERS
# =========================
def excel_col_names(n):
    names = []
    for i in range(1, n + 1):
        s = ""
        x = i
        while x > 0:
            x, rem = divmod(x - 1, 26)
            s = chr(65 + rem) + s
        names.append(s)
    return names

def load_excel(file_obj, header_row=0, use_column_letters=False):
    df = pd.read_excel(file_obj, header=header_row)
    if use_column_letters:
        df.columns = excel_col_names(len(df.columns))
    else:
        df.columns = [str(c).strip() for c in df.columns]
    return df

def normalize_text(s):
    if pd.isna(s):
        return ""
    return str(s).strip().upper()

def safe_date(s):
    return pd.to_datetime(s, errors="coerce")

def safe_num(s):
    return pd.to_numeric(s, errors="coerce")

def find_week_columns(df):
    return list(df.columns[7:])

def pick_col(df, candidates, fallback_idx=None):
    """Find a column by name. Case-insensitive, supports partial matching."""
    cols_upper = {str(c).strip().upper(): c for c in df.columns}
    # 1. Exact match (case-insensitive)
    for cand in candidates:
        if cand.upper() in cols_upper:
            return cols_upper[cand.upper()]
    # 2. Partial match: candidate is contained in column name
    for cand in candidates:
        for col_up, col_orig in cols_upper.items():
            if cand.upper() in col_up:
                return col_orig
    # 3. Partial match: column name is contained in candidate
    for cand in candidates:
        for col_up, col_orig in cols_upper.items():
            if col_up in cand.upper():
                return col_orig
    # 4. Fallback to positional index (last resort)
    if fallback_idx is not None and fallback_idx < len(df.columns):
        return df.columns[fallback_idx]
    raise KeyError(f"Could not find any of these columns: {candidates}. Available: {list(df.columns)}")

def run_launch_check(orders_file, launch_file):
    orders = load_excel(orders_file, header_row=HEADER_ROW_ORDERS)
    launch = load_excel(launch_file, header_row=HEADER_ROW_LAUNCH)

    orders_order_date_col    = pick_col(orders, ["ORDER DATE", "ORDER_DATE", "DOCUMENT DATE"])
    orders_delivery_date_col = pick_col(orders, ["DELIVERY DATE", "DELIVERY_DATE"])
    orders_material_col      = pick_col(orders, ["MATERIAL", "SKU", "PRODUCT"])
    orders_qty_col           = pick_col(orders, ["ORDER QTY", "ORDER QUANTITY", "QTY", "QUANTITY", "ORDER QUANTITY (ITEM)", "ORDER QTY (ITEM)", "QUANTITY (ITEM)"])
    orders_sales_unit_col    = pick_col(orders, ["SALES UNIT", "UNIT", "UOM"])
    orders_chain_col         = pick_col(orders, ["CHAIN", "CLIENT", "CUSTOMER"])
    orders_country_col       = pick_col(orders, ["COUNTRY", "MARKET"])

    launch_material_col   = pick_col(launch, ["MATERIAL", "SKU", "PRODUCT", "ITEM"])
    launch_chain_col      = pick_col(launch, ["CHAIN", "CLIENT", "CUSTOMER", "CUSTOMER VALUE"])
    launch_country_col    = pick_col(launch, ["COUNTRY", "MARKET", "LOCATION VALUE"])
    launch_start_date_col = pick_col(launch, ["START DATE", "LAUNCH DATE", "START"])

    launch_week_cols = find_week_columns(launch)

    # Clean orders
    orders = orders.copy()
    orders["material_key"]  = orders[orders_material_col].map(normalize_text)
    orders["chain_key"]     = orders[orders_chain_col].map(normalize_text)
    orders["country_key"]   = orders[orders_country_col].map(normalize_text)
    orders["order_date"]    = orders[orders_order_date_col].map(safe_date)
    orders["delivery_date"] = orders[orders_delivery_date_col].map(safe_date)
    orders["order_qty"]     = orders[orders_qty_col].map(safe_num)
    orders["sales_unit"]    = orders[orders_sales_unit_col].astype(str).str.strip()

    orders = orders.dropna(subset=["order_date", "order_qty"])
    orders = orders[
        (orders["material_key"] != "") &
        (orders["chain_key"] != "") &
        (orders["country_key"] != "")
    ]

    orders_by_day = (
        orders.groupby(["material_key", "chain_key", "country_key", "order_date"], as_index=False)
        .agg(
            total_qty=("order_qty", "sum"),
            first_delivery_date=("delivery_date", "min"),
            sales_unit=("sales_unit", lambda x: ", ".join(sorted(set([v for v in x if v and v != "nan"]))))
        )
    )

    first_order = (
        orders_by_day.sort_values("order_date")
        .groupby(["material_key", "chain_key", "country_key"], as_index=False)
        .first()
        .rename(columns={
            "order_date": "actual_first_order_date",
            "first_delivery_date": "actual_first_delivery_date",
            "total_qty": "first_order_qty",
            "sales_unit": "sales_unit_first_order"
        })
    )

    orders_by_day = orders_by_day.merge(
        first_order[["material_key", "chain_key", "country_key", "actual_first_order_date"]],
        on=["material_key", "chain_key", "country_key"],
        how="left"
    )

    orders_by_day["days_from_first_order"] = (
        orders_by_day["order_date"] - orders_by_day["actual_first_order_date"]
    ).dt.days

    orders_by_day["qty_7d"]  = np.where(
        (orders_by_day["days_from_first_order"] >= 0) & (orders_by_day["days_from_first_order"] < 7),
        orders_by_day["total_qty"], 0)
    orders_by_day["qty_28d"] = np.where(
        (orders_by_day["days_from_first_order"] >= 0) & (orders_by_day["days_from_first_order"] < 28),
        orders_by_day["total_qty"], 0)
    orders_by_day["qty_52w"] = np.where(
        (orders_by_day["days_from_first_order"] >= 0) & (orders_by_day["days_from_first_order"] < 364),
        orders_by_day["total_qty"], 0)

    rollups = (
        orders_by_day.groupby(["material_key", "chain_key", "country_key"], as_index=False)
        .agg(
            actual_qty_first_7d=("qty_7d", "sum"),
            actual_qty_first_28d=("qty_28d", "sum"),
            actual_qty_first_52w=("qty_52w", "sum"),
        )
    )
    first_order = first_order.merge(rollups, on=["material_key", "chain_key", "country_key"], how="left")

    # Clean launch
    launch = launch.copy()
    launch["material_key"]        = launch[launch_material_col].map(normalize_text)
    launch["chain_key"]           = launch[launch_chain_col].map(normalize_text)
    launch["country_key"]         = launch[launch_country_col].map(normalize_text)
    launch["planned_launch_date"] = launch[launch_start_date_col].map(safe_date)

    for c in launch_week_cols:
        launch[c] = launch[c].map(safe_num)

    launch["expected_w1"]        = launch[launch_week_cols[0]] if len(launch_week_cols) >= 1 else np.nan
    launch["expected_w2"]        = launch[launch_week_cols[1]] if len(launch_week_cols) >= 2 else np.nan
    launch["expected_52w_total"] = launch[launch_week_cols].sum(axis=1, skipna=True)

    launch_summary = launch[[
        launch_material_col, launch_chain_col, launch_country_col,
        "material_key", "chain_key", "country_key",
        "planned_launch_date", "expected_w1", "expected_w2", "expected_52w_total"
    ]].copy()
    launch_summary = launch_summary.rename(columns={
        launch_material_col: "material_original",
        launch_chain_col:    "chain_original",
        launch_country_col:  "country_original"
    })

    result = launch_summary.merge(first_order, on=["material_key", "chain_key", "country_key"], how="left")

    result["launch_date_diff_days"] = (
        result["actual_first_order_date"] - result["planned_launch_date"]
    ).dt.days

    result["first_order_vs_expected_w1_pct"] = np.where(
        result["expected_w1"].fillna(0) > 0,
        result["first_order_qty"] / result["expected_w1"], np.nan)
    result["actual_7d_vs_expected_w1_pct"] = np.where(
        result["expected_w1"].fillna(0) > 0,
        result["actual_qty_first_7d"] / result["expected_w1"], np.nan)
    result["actual_52w_vs_expected_52w_pct"] = np.where(
        result["expected_52w_total"].fillna(0) > 0,
        result["actual_qty_first_52w"] / result["expected_52w_total"], np.nan)

    orders_with_launch_check = orders_by_day.merge(
        launch_summary[["material_key", "chain_key", "country_key", "expected_w1"]],
        on=["material_key", "chain_key", "country_key"], how="left"
    )
    orders_with_launch_check["pct_of_expected_w1"] = np.where(
        orders_with_launch_check["expected_w1"].fillna(0) > 0,
        orders_with_launch_check["total_qty"] / orders_with_launch_check["expected_w1"], np.nan)

    meaningful_launch = (
        orders_with_launch_check.loc[
            orders_with_launch_check["pct_of_expected_w1"] >= MEANINGFUL_LAUNCH_THRESHOLD_PCT
        ]
        .sort_values("order_date")
        .groupby(["material_key", "chain_key", "country_key"], as_index=False)
        .first()
        .rename(columns={
            "order_date": "meaningful_launch_date",
            "total_qty":  "meaningful_launch_day_qty",
            "pct_of_expected_w1": "meaningful_launch_day_pct_of_expected_w1"
        })
    )

    result = result.merge(
        meaningful_launch[[
            "material_key", "chain_key", "country_key",
            "meaningful_launch_date", "meaningful_launch_day_qty",
            "meaningful_launch_day_pct_of_expected_w1"
        ]],
        on=["material_key", "chain_key", "country_key"], how="left"
    )

    def status_row(row):
        if pd.isna(row["actual_first_order_date"]):
            return "No orders found"
        if pd.notna(row["launch_date_diff_days"]) and abs(row["launch_date_diff_days"]) <= 7:
            return "Planned and actual close"
        if pd.notna(row["launch_date_diff_days"]) and row["launch_date_diff_days"] > 7:
            return "Actual later than planned"
        if pd.notna(row["launch_date_diff_days"]) and row["launch_date_diff_days"] < -7:
            return "Actual earlier than planned"
        return "Check"

    result["launch_status"] = result.apply(status_row, axis=1)

    final_cols = [
        "material_original", "chain_original", "country_original",
        "planned_launch_date", "actual_first_order_date", "actual_first_delivery_date",
        "meaningful_launch_date", "launch_date_diff_days",
        "expected_w1", "first_order_qty", "first_order_vs_expected_w1_pct",
        "actual_qty_first_7d", "actual_7d_vs_expected_w1_pct",
        "expected_52w_total", "actual_qty_first_52w", "actual_52w_vs_expected_52w_pct",
        "meaningful_launch_day_qty", "meaningful_launch_day_pct_of_expected_w1",
        "sales_unit_first_order", "launch_status",
    ]

    result = result[final_cols].copy()

    exceptions = result[
        result["actual_first_order_date"].isna() |
        (result["first_order_vs_expected_w1_pct"].fillna(0) < 0.05) |
        (result["launch_date_diff_days"].abs().fillna(9999) > 14)
    ].copy()

    buf = io.BytesIO()
    # Free large intermediate dataframes before writing
    del orders, orders_by_day, orders_with_launch_check, launch, launch_summary
    import gc; gc.collect()

    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        result.to_excel(writer, sheet_name="Launch_Check", index=False)
        exceptions.to_excel(writer, sheet_name="Exceptions", index=False)
    buf.seek(0)

    stats = {
        "total":      len(result),
        "on_time":    int((result["launch_status"] == "Planned and actual close").sum()),
        "late":       int((result["launch_status"] == "Actual later than planned").sum()),
        "early":      int((result["launch_status"] == "Actual earlier than planned").sum()),
        "no_orders":  int((result["launch_status"] == "No orders found").sum()),
    }
    return buf, stats
