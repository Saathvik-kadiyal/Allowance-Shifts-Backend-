"""
Shift search service.

This module provides functionality to search employee shift records
within a given month or month range. It validates input months, prevents
future-date queries, and returns structured shift data grouped per
employee per month with human-readable shift labels.

The service is intended for reporting and audit use cases where detailed
shift breakdowns are required across one or more months.
"""

from datetime import datetime, date
from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func
from models.models import ShiftAllowances, ShiftMapping

SHIFT_LABELS = {
    "A": "A(9PM to 6AM)",
    "B": "B(4PM to 1AM)",
    "C": "C(6AM to 3PM)",
    "PRIME": "PRIME(12AM to 9AM)"
}

def search_shift_by_month_range(
    db: Session,
    start_month: str | None = None,
    end_month: str | None = None
):
    """
    Search shift records within a given month or month range.

    At least one of start_month or end_month must be provided.
    The service prevents querying future months and returns shift
    data enriched with descriptive shift labels.

    Args:
        db (Session): Active SQLAlchemy database session.
        start_month (str | None): Start month in YYYY-MM format.
        end_month (str | None): End month in YYYY-MM format.

    Returns:
        list[dict]: List of shift records with employee details and
        labeled shift-day mappings.

    Raises:
        HTTPException: If input validation fails or no records are found.
    """

    if not start_month and not end_month:
        raise HTTPException(status_code=400, detail="Provide at least one month.")

    try:
        start_date = datetime.strptime(start_month, "%Y-%m").date() if start_month else None
        end_date = datetime.strptime(end_month, "%Y-%m").date() if end_month else None
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid month format. Use YYYY-MM")

    today = date.today()
    current_month_start = today.replace(day=1)

    if end_date and end_date > current_month_start:
        raise HTTPException(status_code=400,
                            detail=f"end_month cannot be greater than {today.strftime('%Y-%m')}")

    if start_date:
        start_date = start_date.replace(day=1)
    if end_date:
        end_date = end_date.replace(day=1)

    query = db.query(
        ShiftAllowances.id,
        ShiftAllowances.emp_id,
        ShiftAllowances.emp_name,
        ShiftAllowances.grade,
        ShiftAllowances.department,
        ShiftAllowances.client,
        ShiftAllowances.project,
        ShiftAllowances.account_manager,
        ShiftAllowances.duration_month,
        ShiftAllowances.payroll_month
    )

    if start_date and end_date:
        query = query.filter(
            func.date_trunc("month", ShiftAllowances.duration_month) >= start_date,
            func.date_trunc("month", ShiftAllowances.duration_month) <= end_date
        )
    elif start_date:
        query = query.filter(
            func.date_trunc("month", ShiftAllowances.duration_month) == start_date)
    elif end_date:
        query = query.filter(func.date_trunc("month", ShiftAllowances.duration_month) == end_date)

    rows = query.order_by(ShiftAllowances.duration_month, ShiftAllowances.emp_id).all()

    if not rows:
        raise HTTPException(status_code=404, detail="No records found for given month range")

    final_data = []
    for row in rows:
        base = row._asdict()
        shiftallowance_id = base.pop("id")


        base.pop("project_code", None)


        base["duration_month"] = row.duration_month.strftime("%Y-%m")
        base["payroll_month"] = row.payroll_month.strftime("%Y-%m")

        # Fetch shift types and days
        shift_output = {}
        mappings = db.query(ShiftMapping.shift_type, ShiftMapping.days)\
                     .filter(ShiftMapping.shiftallowance_id == shiftallowance_id).all()

        for m in mappings:
            if m.days is not None:
                val = float(m.days)
                if val > 0:
                    label = SHIFT_LABELS.get(m.shift_type, m.shift_type)
                    shift_output[label] = val

        record = {**base, **shift_output}
        final_data.append(record)

    return final_data
