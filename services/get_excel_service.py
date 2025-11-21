import pandas as pd
import os
from datetime import datetime
from sqlalchemy.orm import Session
from models.models import ShiftAllowances
 
 
def convert_to_db_date_format(payroll_month: str) -> str:
    """
    Converts MM-YYYY → YYYY-MM-01 (DB format)
    Example: 03-2025 → 2025-03-01
    """
    try:
        dt = datetime.strptime(payroll_month, "%m-%Y")
        return dt.strftime("%Y-%m-01")
    except ValueError:
        raise ValueError("Invalid format. Use MM-YYYY (e.g., 03-2025)")
 
 
def export_excel_by_payroll_month(db: Session, payroll_month: str):
    """
    Fetch all data for the given payroll month and export to Excel.
    Returns file path if found, else None.
    """
 
    # Convert MM-YYYY to YYYY-MM-01
    db_date = convert_to_db_date_format(payroll_month)
 
    # Fetch records
    data = db.query(ShiftAllowances).filter(
        ShiftAllowances.payroll_month == db_date
    ).all()
 
    if not data:
        return None
 
    # Convert to DataFrame with all fields
    df = pd.DataFrame([
        {
            "emp_id": d.emp_id,
            "emp_name": d.emp_name,
            "grade": d.grade,
            "department": d.department,
            "client": d.client,
            "project": d.project,
            "project_code": d.project_code,
            "account_manager": d.account_manager,
            "practice_lead": d.practice_lead,
            "delivery_manager": d.delivery_manager,
            "duration_month": d.duration_month,
            "payroll_month": payroll_month,  # keep original format
            "shift_a_days": d.shift_a_days,
            "shift_b_days": d.shift_b_days,
            "shift_c_days": d.shift_c_days,
            "prime_days": d.prime_days,
            "total_days": d.total_days,
            "billability_status": d.billability_status,
            "practice_remarks": d.practice_remarks,
            "rmg_comments": d.rmg_comments,
            "amar_approval": d.amar_approval,
        }
        for d in data
    ])
 
    df = df.fillna("")
 
    # Export folder
    os.makedirs("exports", exist_ok=True)
 
    filename = f"Shift_Allowances_{payroll_month}.xlsx"
    filepath = os.path.join("exports", filename)
 
    # Write Excel
    df.to_excel(filepath, index=False)
 
    return filepath
   