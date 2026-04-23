import pandas as pd
import numpy as np
import io

MEANINGFUL_LAUNCH_THRESHOLD_PCT = 0.05

def excel_col_names(n):
    names = []
    for i in range(1, n + 1):
        s, x = "", i
        while x > 0:
            x, rem = divmod(x - 1, 26)
            s = chr(65 + rem) + s
        names.append(s)
    return names

def load_excel_df(file_obj, header_row=0):
    df = pd.read_excel(file_obj, header=header_row)
    df.columns = [str(c).strip() for c in df.columns]
    return df

def normalize_text(s):
    return "" if pd.isna(s) else str(s).strip().upper()

def safe_date(s):
    return pd.to_datetime(s, errors="coerce")

def safe_num(s):
    return pd.to_numeric(s, errors="coerce")

def find_week_columns(df):
    return list(df.columns[7:])

def pick_col(df, candidates, fallback_idx=None):
    cols_upper = {str(c).strip().upper(): c for c in df.columns}
    for cand in candidates:
        if cand.upper() in cols_upper:
            return cols_upper[cand.upper()]
    if fallback_idx is not None and fallback_idx < len(df.columns):
        return df.columns[fallback_idx]
    raise KeyError(f"Kolom niet gevonden: {candidates}")

def run_launch_check(orders_file, launch_file):
    orders = load_excel_df(orders_file)
    launch = load_excel_df(launch_file)

    o_date     = pick_col(orders, ["ORDER DATE","ORDER_DATE"], 3)
    o_del      = pick_col(orders, ["DELIVERY DATE","DELIVERY_DATE"], 4)
    o_mat      = pick_col(orders, ["MATERIAL","SKU","PRODUCT"], 5)
    o_qty      = pick_col(orders, ["ORDER QTY","ORDER QUANTITY","QTY","QUANTITY"], 6)
    o_unit     = pick_col(orders, ["SALES UNIT","UNIT","UOM"], 11)
    o_chain    = pick_col(orders, ["CHAIN","CLIENT","CUSTOMER"], 1)
    o_country  = pick_col(orders, ["COUNTRY","MARKET"], 2)
    l_mat      = pick_col(launch, ["MATERIAL","SKU","PRODUCT"], 0)
    l_chain    = pick_col(launch, ["CHAIN","CLIENT","CUSTOMER"], 2)
    l_country  = pick_col(launch, ["COUNTRY","MARKET"], 4)
    l_start    = pick_col(launch, ["START DATE","LAUNCH DATE","START"], 6)
    l_week_cols = find_week_columns(launch)

    orders = orders.copy()
    orders["material_key"]  = orders[o_mat].map(normalize_text)
    orders["chain_key"]     = orders[o_chain].map(normalize_text)
    orders["country_key"]   = orders[o_country].map(normalize_text)
    orders["order_date"]    = orders[o_date].map(safe_date)
    orders["delivery_date"] = orders[o_del].map(safe_date)
    orders["order_qty"]     = orders[o_qty].map(safe_num)
    orders["sales_unit"]    = orders[o_unit].astype(str).str.strip()
    orders = orders.dropna(subset=["order_date","order_qty"])
    orders = orders[(orders["material_key"]!="")&(orders["chain_key"]!="")&(orders["country_key"]!="")]

    obd = (orders.groupby(["material_key","chain_key","country_key","order_date"], as_index=False)
           .agg(total_qty=("order_qty","sum"), first_delivery_date=("delivery_date","min"),
                sales_unit=("sales_unit", lambda x: ", ".join(sorted(set([v for v in x if v and v!="nan"]))))))

    first_order = (obd.sort_values("order_date")
                   .groupby(["material_key","chain_key","country_key"], as_index=False).first()
                   .rename(columns={"order_date":"actual_first_order_date","first_delivery_date":"actual_first_delivery_date",
                                    "total_qty":"first_order_qty","sales_unit":"sales_unit_first_order"}))

    obd = obd.merge(first_order[["material_key","chain_key","country_key","actual_first_order_date"]],
                    on=["material_key","chain_key","country_key"], how="left")
    obd["days_from_first_order"] = (obd["order_date"]-obd["actual_first_order_date"]).dt.days
    obd["qty_7d"]  = np.where((obd["days_from_first_order"]>=0)&(obd["days_from_first_order"]<7),  obd["total_qty"],0)
    obd["qty_28d"] = np.where((obd["days_from_first_order"]>=0)&(obd["days_from_first_order"]<28), obd["total_qty"],0)
    obd["qty_52w"] = np.where((obd["days_from_first_order"]>=0)&(obd["days_from_first_order"]<364),obd["total_qty"],0)

    rollups = (obd.groupby(["material_key","chain_key","country_key"], as_index=False)
               .agg(actual_qty_first_7d=("qty_7d","sum"), actual_qty_first_28d=("qty_28d","sum"), actual_qty_first_52w=("qty_52w","sum")))
    first_order = first_order.merge(rollups, on=["material_key","chain_key","country_key"], how="left")

    launch = launch.copy()
    launch["material_key"]       = launch[l_mat].map(normalize_text)
    launch["chain_key"]          = launch[l_chain].map(normalize_text)
    launch["country_key"]        = launch[l_country].map(normalize_text)
    launch["planned_launch_date"]= launch[l_start].map(safe_date)
    for c in l_week_cols:
        launch[c] = launch[c].map(safe_num)
    launch["expected_w1"] = launch[l_week_cols[0]] if len(l_week_cols)>=1 else np.nan
    launch["expected_w2"] = launch[l_week_cols[1]] if len(l_week_cols)>=2 else np.nan
    launch["expected_52w_total"] = launch[l_week_cols].sum(axis=1, skipna=True)

    ls = launch[[l_mat,l_chain,l_country,"material_key","chain_key","country_key",
                 "planned_launch_date","expected_w1","expected_w2","expected_52w_total"]].copy()
    ls = ls.rename(columns={l_mat:"material_original",l_chain:"chain_original",l_country:"country_original"})

    result = ls.merge(first_order, on=["material_key","chain_key","country_key"], how="left")
    result["launch_date_diff_days"] = (result["actual_first_order_date"]-result["planned_launch_date"]).dt.days
    result["first_order_vs_expected_w1_pct"] = np.where(result["expected_w1"].fillna(0)>0, result["first_order_qty"]/result["expected_w1"], np.nan)
    result["actual_7d_vs_expected_w1_pct"]   = np.where(result["expected_w1"].fillna(0)>0, result["actual_qty_first_7d"]/result["expected_w1"], np.nan)
    result["actual_52w_vs_expected_52w_pct"] = np.where(result["expected_52w_total"].fillna(0)>0, result["actual_qty_first_52w"]/result["expected_52w_total"], np.nan)

    owlc = obd.merge(ls[["material_key","chain_key","country_key","expected_w1"]],
                     on=["material_key","chain_key","country_key"], how="left")
    owlc["pct_of_expected_w1"] = np.where(owlc["expected_w1"].fillna(0)>0, owlc["total_qty"]/owlc["expected_w1"], np.nan)
    ml = (owlc.loc[owlc["pct_of_expected_w1"]>=MEANINGFUL_LAUNCH_THRESHOLD_PCT]
          .sort_values("order_date")
          .groupby(["material_key","chain_key","country_key"], as_index=False).first()
          .rename(columns={"order_date":"meaningful_launch_date","total_qty":"meaningful_launch_day_qty",
                           "pct_of_expected_w1":"meaningful_launch_day_pct_of_expected_w1"}))
    result = result.merge(ml[["material_key","chain_key","country_key","meaningful_launch_date",
                               "meaningful_launch_day_qty","meaningful_launch_day_pct_of_expected_w1"]],
                          on=["material_key","chain_key","country_key"], how="left")

    def status_row(row):
        if pd.isna(row["actual_first_order_date"]): return "No orders found"
        if pd.notna(row["launch_date_diff_days"]) and abs(row["launch_date_diff_days"])<=7: return "Planned and actual close"
        if pd.notna(row["launch_date_diff_days"]) and row["launch_date_diff_days"]>7: return "Actual later than planned"
        if pd.notna(row["launch_date_diff_days"]) and row["launch_date_diff_days"]<-7: return "Actual earlier than planned"
        return "Check"

    result["launch_status"] = result.apply(status_row, axis=1)
    final_cols = ["material_original","chain_original","country_original","planned_launch_date",
                  "actual_first_order_date","actual_first_delivery_date","meaningful_launch_date",
                  "launch_date_diff_days","expected_w1","first_order_qty","first_order_vs_expected_w1_pct",
                  "actual_qty_first_7d","actual_7d_vs_expected_w1_pct","expected_52w_total",
                  "actual_qty_first_52w","actual_52w_vs_expected_52w_pct","meaningful_launch_day_qty",
                  "meaningful_launch_day_pct_of_expected_w1","sales_unit_first_order","launch_status"]
    result = result[[c for c in final_cols if c in result.columns]].copy()
    exceptions = result[result["actual_first_order_date"].isna()|
                        (result["first_order_vs_expected_w1_pct"].fillna(0)<0.05)|
                        (result["launch_date_diff_days"].abs().fillna(9999)>14)].copy()

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        result.to_excel(writer, sheet_name="Launch_Check", index=False)
        exceptions.to_excel(writer, sheet_name="Exceptions", index=False)
        obd.to_excel(writer, sheet_name="Orders_By_Day", index=False)
    buf.seek(0)

    stats = {
        "total": len(result),
        "on_time": int((result["launch_status"]=="Planned and actual close").sum()),
        "late":    int((result["launch_status"]=="Actual later than planned").sum()),
        "early":   int((result["launch_status"]=="Actual earlier than planned").sum()),
        "no_orders": int((result["launch_status"]=="No orders found").sum()),
    }
    return buf, stats
