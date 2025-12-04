from datetime import datetime
from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func
from fastapi.encoders import jsonable_encoder
from models.models import ShiftAllowances, ShiftMapping

SHIFT_LABELS = {
    "A": "A(9PM to 6AM)",
    "B": "B(4PM to 1AM)",
    "C": "C(6AM to 3PM)",
    "PRIME": "PRIME(12AM to 9AM)"
}

def export_filtered_excel(
    db: Session,
    emp_id: str | None = None,
    account_manager: str | None = None,
    start_month: str | None = None,
    end_month: str | None = None,
    start: int = 0,
    limit: int = 10,
):
    if end_month and not start_month:
        raise HTTPException(status_code=400, detail="start_month is required when end_month is provided")

    for m in [start_month, end_month]:
        if m:
            try:
                datetime.strptime(m, "%Y-%m")
            except ValueError:
                raise HTTPException(status_code=400, detail="Month format must be YYYY-MM")

    base_query = (
        db.query(
            ShiftAllowances.id,
            ShiftAllowances.emp_id,
            ShiftAllowances.emp_name,
            ShiftAllowances.grade,
            ShiftAllowances.department,
            ShiftAllowances.client,
            ShiftAllowances.project,
            ShiftAllowances.account_manager,
            func.to_char(ShiftAllowances.duration_month, "YYYY-MM").label("duration_month"),
            func.to_char(ShiftAllowances.payroll_month, "YYYY-MM").label("payroll_month")
        )
    )

    # MONTH FILTERS
    if start_month and end_month:
        base_query = base_query.filter(
            func.to_char(ShiftAllowances.duration_month, "YYYY-MM") >= start_month,
            func.to_char(ShiftAllowances.duration_month, "YYYY-MM") <= end_month
        )
    elif start_month:
        base_query = base_query.filter(
            func.to_char(ShiftAllowances.duration_month, "YYYY-MM") == start_month
        )
    # else → No month filter → return full DB based on other search params

    # EMP / ACCOUNT MANAGER FILTERS
    if emp_id:
        base_query = base_query.filter(func.upper(ShiftAllowances.emp_id).like(f"%{emp_id.upper()}%"))

    if account_manager:
        base_query = base_query.filter(func.upper(ShiftAllowances.account_manager).like(f"%{account_manager.upper()}%"))

    # PAGINATION
    total_records = base_query.count()
    rows = base_query.order_by(
        ShiftAllowances.duration_month.desc(),
        ShiftAllowances.emp_id.asc()
    ).offset(start).limit(limit).all()

    if not rows:
        raise HTTPException(status_code=404, detail="No data found based on filters")

    result = []
    SHIFT_LABELS = {
        "A": "A(9PM to 6AM)",
        "B": "B(4PM to 1AM)",
        "C": "C(6AM to 3PM)",
        "PRIME": "PRIME(12AM to 9AM)"
    }

    for row in rows:
        row_dict = row._asdict()
        shiftallowance_id = row_dict.pop("id")

        mappings = db.query(ShiftMapping.shift_type, ShiftMapping.days).filter(
            ShiftMapping.shiftallowance_id == shiftallowance_id
        ).all()

        shift_output = {}
        for m in mappings:
            days = float(m.days)
            if days > 0:
                shift_output[SHIFT_LABELS.get(m.shift_type, m.shift_type)] = days

        row_dict["shift_details"] = shift_output
        result.append(row_dict)

    return total_records, result