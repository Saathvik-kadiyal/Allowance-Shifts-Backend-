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
    end_month: str | None = None
):

    # ===== VALIDATIONS =====
    if end_month and not start_month:
        raise HTTPException(status_code=400, detail="start_month is required when end_month is provided")

    for m in [start_month, end_month]:
        if m:
            try:
                datetime.strptime(m, "%Y-%m")
            except ValueError:
                raise HTTPException(status_code=400, detail="Month format must be YYYY-MM")
    if start_month and end_month and start_month > end_month:
        raise HTTPException(
            status_code=400,
            detail="start_month must be less than or equal to end_month"
        )
    current_month = datetime.now().strftime("%Y-%m")
    if start_month and start_month > current_month:
        raise HTTPException(
            status_code=400,
            detail=f"start_month '{start_month}' cannot be greater than current month {current_month}"
        )

    if end_month and end_month > current_month:
        raise HTTPException(
            status_code=400,
            detail=f"end_month '{end_month}' cannot be greater than current month {current_month}"
        )
    # base query
    query = (
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

    # month filters
    if start_month and end_month:
        query = query.filter(
            func.to_char(ShiftAllowances.duration_month, "YYYY-MM") >= start_month,
            func.to_char(ShiftAllowances.duration_month, "YYYY-MM") <= end_month
        )
    elif start_month:
        query = query.filter(
            func.to_char(ShiftAllowances.duration_month, "YYYY-MM") == start_month
        )

    # emp filters
    if emp_id:
        query = query.filter(func.upper(ShiftAllowances.emp_id).like(f"%{emp_id.upper()}%"))

    if account_manager:
        query = query.filter(func.upper(ShiftAllowances.account_manager).like(f"%{account_manager.upper()}%"))


    rows = query.order_by(ShiftAllowances.duration_month, ShiftAllowances.emp_id).all()

    if not rows:
        raise HTTPException(status_code=404, detail="No data found based on filters")

    final_data = []
    for row in rows:
        base = row._asdict()
        shiftallowance_id = base.pop("id")

        mappings = (
            db.query(ShiftMapping.shift_type, ShiftMapping.days)
            .filter(ShiftMapping.shiftallowance_id == shiftallowance_id)
            .all()
        )

        shift_output = {}
        for m in mappings:
            if m.days and float(m.days) > 0:
                label = SHIFT_LABELS.get(m.shift_type, m.shift_type)
                shift_output[label] = float(m.days)

        final_data.append({k: v for k, v in {**base, **shift_output}.items() if v is not None})

    return jsonable_encoder(final_data)