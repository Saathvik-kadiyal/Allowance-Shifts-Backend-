"""
Service for exporting client summary data as an Excel file.
"""
 
import os
from datetime import date
from fastapi import HTTPException
from sqlalchemy.orm import Session
import pandas as pd
from diskcache import Cache
 
from services.client_summary_service import (
    client_summary_service,
    is_default_latest_month_request,
    LATEST_MONTH_KEY,
    CACHE_TTL,
)
 
 
cache = Cache("./diskcache/latest_month")
 
EXPORT_DIR = "exports"
DEFAULT_EXPORT_FILE = "client_summary_latest.xlsx"
 
 
def client_summary_download_service(db: Session, payload: dict) -> str:
    """
    Generate and export client summary Excel.
 
    - Column "Client Partner" comes from account_manager in DB.
    - Employee-level shifts included if present.
    - Fallback to department totals if employee shift data missing.
    - Zero departments are preserved.
    - Uses cache ONLY for default latest-month request.
    """
 
    payload = payload or {}
 
    if is_default_latest_month_request(payload):
        cached = cache.get(f"{LATEST_MONTH_KEY}:excel")
        if cached and os.path.exists(cached["file_path"]):
            return cached["file_path"]
 
    emp_filter = payload.get("emp_id")
    manager_filter = payload.get("account_manager")
 
    summary_data = client_summary_service(db, payload)
    if not summary_data:
        raise HTTPException(404, "No data available")
 
    rows = []
 
    sorted_periods = sorted(summary_data.keys())
 
    for period_key in sorted_periods:
        period_data = summary_data[period_key]
        if "clients" not in period_data:
            continue
 
        for client_name, client_block in period_data["clients"].items():
            client_partner_value = client_block.get("account_manager", "")
            departments = client_block.get("departments", {})
 
            for dept_name, dept_block in departments.items():
                employees = dept_block.get("employees", [])
 
                if not employees:
                    if manager_filter and manager_filter != client_partner_value:
                        continue
 
                    rows.append({
                        "Period": period_key,
                        "Client": client_name,
                        "Client Partner": client_partner_value,
                        "Employee ID": "",
                        "Department": dept_name,
                        "Head Count": dept_block.get("dept_head_count", 0),
                        "Shift A": f"₹{dept_block.get('dept_A', 0):,}",
                        "Shift B": f"₹{dept_block.get('dept_B', 0):,}",
                        "Shift C": f"₹{dept_block.get('dept_C', 0):,}",
                        "Shift PRIME": f"₹{dept_block.get('dept_PRIME', 0):,}",
                        "Total Allowance": f"₹{dept_block.get('dept_total', 0):,}",
                    })
                else:
                    for emp in employees:
                        if emp_filter and emp_filter != emp.get("emp_id"):
                            continue
                        if manager_filter and manager_filter != emp.get("account_manager", client_partner_value):
                            continue
 
                        rows.append({
                            "Period": period_key,
                            "Client": client_name,
                            "Client Partner": emp.get("account_manager", client_partner_value),
                            "Employee ID": emp.get("emp_id", ""),
                            "Department": dept_name,
                            "Head Count": 1,
                            "Shift A": f"₹{emp.get('shift_A', dept_block.get('dept_A', 0)):,}",
                            "Shift B": f"₹{emp.get('shift_B', dept_block.get('dept_B', 0)):,}",
                            "Shift C": f"₹{emp.get('shift_C', dept_block.get('dept_C', 0)):,}",
                            "Shift PRIME": f"₹{emp.get('shift_PRIME', dept_block.get('dept_PRIME', 0)):,}",
                            "Total Allowance": f"₹{emp.get('total_allowance', dept_block.get('dept_total', 0)):,}",
                        })
 
    if not rows:
        raise HTTPException(404, "No data available for export")
 
    df = pd.DataFrame(rows)
    df["Period"] = pd.to_datetime(df["Period"], format="%Y-%m")
    df = df.sort_values(by=["Period", "Client", "Department", "Employee ID"])
    df["Period"] = df["Period"].dt.strftime("%Y-%m")
 
    os.makedirs(EXPORT_DIR, exist_ok=True)
 
    if is_default_latest_month_request(payload):
        file_path = os.path.join(EXPORT_DIR, DEFAULT_EXPORT_FILE)
    else:
        file_path = os.path.join(
            EXPORT_DIR,
            f"client_summary_{date.today():%Y%m%d_%H%M%S}.xlsx",
        )
 
    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Client Summary")
 
    if is_default_latest_month_request(payload):
        cache.set(
            f"{LATEST_MONTH_KEY}:excel",
            {
                "_cached_month": df["Period"].iloc[0],
                "file_path": file_path,
            },
            expire=CACHE_TTL,
        )
 
    return file_path
 
 