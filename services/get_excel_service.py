import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import func
from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount
from fastapi import HTTPException
from datetime import datetime, date

def export_filtered_excel(
    db: Session,
    emp_id: str | None = None,
    account_manager: str | None = None,
    start_month: str | None = None,
    end_month: str | None = None
):

    SHIFT_LABELS = {
        "A": "A",
        "B": "B",
        "C": "C",
        "PRIME": "PRIME"
    }

    # Base query without shift aggregation
    query = (
        db.query(
            ShiftAllowances.id,
            ShiftAllowances.emp_id,
            ShiftAllowances.emp_name,
            ShiftAllowances.grade,
            ShiftAllowances.department,
            ShiftAllowances.client,
            ShiftAllowances.project,
            ShiftAllowances.project_code,
            ShiftAllowances.account_manager,
            ShiftAllowances.delivery_manager,
            ShiftAllowances.practice_lead,
            ShiftAllowances.billability_status,
            ShiftAllowances.practice_remarks,
            ShiftAllowances.rmg_comments,
            ShiftAllowances.duration_month,
            ShiftAllowances.payroll_month
        )
    )

    # Filters
    if emp_id:
        query = query.filter(func.trim(ShiftAllowances.emp_id) == emp_id.strip())

    if account_manager:
        query = query.filter(func.lower(func.trim(ShiftAllowances.account_manager)) ==
                             account_manager.strip().lower())

    today = date.today()
    current_month_start = today.replace(day=1)

    if start_month or end_month:
        if not start_month:
            raise HTTPException(status_code=400, detail="start_month is required when end_month is provided")

        try:
            start_date = datetime.strptime(start_month, "%Y-%m").date().replace(day=1)
        except ValueError:
            raise HTTPException(status_code=400, detail="start_month must be YYYY-MM")

        if end_month:
            try:
                end_date = datetime.strptime(end_month, "%Y-%m").date().replace(day=1)
            except ValueError:
                raise HTTPException(status_code=400, detail="end_month must be YYYY-MM")

            if end_date > current_month_start:
                raise HTTPException(status_code=400, detail="end_month cannot be greater than current month")

            if start_date > end_date:
                raise HTTPException(status_code=400, detail="start_month cannot be after end_month")

            query = query.filter(
                func.date_trunc("month", ShiftAllowances.duration_month) >= start_date,
                func.date_trunc("month", ShiftAllowances.duration_month) <= end_date,
            )
        else:
            query = query.filter(func.date_trunc("month", ShiftAllowances.duration_month) == start_date)

    else:
        if not emp_id and not account_manager:
            query = query.filter(func.date_trunc("month", ShiftAllowances.duration_month) == current_month_start)

    # Execute query
    rows = query.all()
    if not rows:
        raise HTTPException(status_code=404, detail="No records found for given filters")

    # Shift allowance amounts
    shift_amounts = db.query(ShiftsAmount).all()
    ALLOWANCE_MAP = {item.shift_type.upper(): float(item.amount) for item in shift_amounts} if shift_amounts else {}

    final_data = []
    for row in rows:
        # fetch shift type & days for each employee
        mappings = db.query(ShiftMapping.shift_type, ShiftMapping.days)\
                     .filter(ShiftMapping.shiftallowance_id == row.id).all()

        shift_entries = []
        total_allowances = 0

        for m in mappings:
            days = float(m.days)
            if days > 0:
                label = SHIFT_LABELS.get(m.shift_type.upper(), m.shift_type.upper())
                shift_entries.append(f"{label}-{int(days)}")
                total_allowances += ALLOWANCE_MAP.get(m.shift_type.upper(), 0) * days

        final_data.append({
            "emp_id": row.emp_id,
            "emp_name": row.emp_name,
            "grade": row.grade,
            "department": row.department,
            "client": row.client,
            "project": row.project,
            "project_code": row.project_code,
            "account_manager": row.account_manager,
            "shift_details": ", ".join(shift_entries) if shift_entries else None,
            "delivery_manager": row.delivery_manager,
            "practice_lead": row.practice_lead,
            "billability_status": row.billability_status,
            "practice_remarks": row.practice_remarks,
            "rmg_comments": row.rmg_comments,
            "duration_month": row.duration_month.strftime("%Y-%m") if row.duration_month else None,
            "payroll_month": row.payroll_month.strftime("%Y-%m") if row.payroll_month else None,
            "total_allowances": total_allowances
        })

    return pd.DataFrame(final_data)
