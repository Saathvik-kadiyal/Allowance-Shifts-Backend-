from datetime import datetime, date
from calendar import monthrange
from typing import Optional, Dict, Any
from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, and_

from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount


def parse_yyyy_mm(value: str) -> date:
    try:
        dt = datetime.strptime(value, "%Y-%m")
        return date(dt.year, dt.month, 1)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid month format '{value}'. Expected YYYY-MM")


def last_day_of_month(d: date) -> date:
    from calendar import monthrange
    _, last = monthrange(d.year, d.month)
    return date(d.year, d.month, last)


def client_summary_service(
    db: Session,
    start_month: Optional[str],
    end_month: Optional[str],
):
    if end_month and not start_month:
        raise HTTPException(status_code=400, detail="end_month cannot be provided without start_month.")

    if not start_month and not end_month:
        latest_date = db.query(func.max(ShiftAllowances.duration_month)).scalar()
        if not latest_date:
            return {"clients": {}, "total": {}}
        start_date = date(latest_date.year, latest_date.month, 1)
        end_date = last_day_of_month(latest_date)
    else:
        start_date = parse_yyyy_mm(start_month)
        if end_month:
            end_date_raw = parse_yyyy_mm(end_month)
            if (end_date_raw.year, end_date_raw.month) < (start_date.year, start_date.month):
                raise HTTPException(status_code=400, detail="end_month must be >= start_month.")
            end_date = last_day_of_month(end_date_raw)
        else:
            end_date = last_day_of_month(start_date)

    current_month = date.today().replace(day=1)
    if (start_date.year, start_date.month) > (current_month.year, current_month.month):
        raise HTTPException(status_code=400, detail="start_month cannot be greater than current month.")
    if (end_date.year, end_date.month) > (current_month.year, current_month.month):
        raise HTTPException(status_code=400, detail="end_month cannot be greater than current month.")

    q = (
        db.query(
            ShiftAllowances.client,
            ShiftAllowances.emp_id,
            ShiftAllowances.emp_name,
            ShiftAllowances.department,
            ShiftMapping.shift_type,
            ShiftMapping.days,
            ShiftsAmount.amount,
        )
        .join(ShiftMapping, ShiftMapping.shiftallowance_id == ShiftAllowances.id)
        .join(
            ShiftsAmount,
            and_(
                ShiftsAmount.shift_type == ShiftMapping.shift_type,
                ShiftsAmount.payroll_year == func.to_char(ShiftAllowances.payroll_month, "YYYY"),
            )
        )
        .filter(
            ShiftAllowances.duration_month >= start_date,
            ShiftAllowances.duration_month <= end_date
        )
    )

    rows = q.all()
    if not rows:
        return {"clients": {}, "total": {}}

    clients: Dict[str, Any] = {}
    global_emp_set = set()
    global_totals = {"A": 0.0, "B": 0.0, "C": 0.0, "PRIME": 0.0}

    for client, emp_id, emp_name, dept, shift_type, days, amount in rows:
        dept = dept or "UNKNOWN"
        shift_allowance = float(days) * float(amount)

        global_emp_set.add(emp_id)
        global_totals[shift_type] += shift_allowance

        client_bucket = clients.setdefault(
            client,
            {
                "client_head_set": set(),
                "client_A": 0.0, "client_B": 0.0, "client_C": 0.0, "client_PRIME": 0.0,
                "departments": {}
            },
        )

        client_bucket["client_head_set"].add(emp_id)
        client_bucket[f"client_{shift_type}"] += shift_allowance

        dept_bucket = client_bucket["departments"].setdefault(
            dept,
            {
                "dept_head_set": set(),
                "dept_A": 0.0, "dept_B": 0.0, "dept_C": 0.0, "dept_PRIME": 0.0,
                "dept_total": 0.0,
                "employees": {}
            }
        )

        dept_bucket["dept_head_set"].add(emp_id)
        dept_bucket[f"dept_{shift_type}"] += shift_allowance
        dept_bucket["dept_total"] += shift_allowance

        emp_bucket = dept_bucket["employees"].setdefault(
            emp_id,
            {"emp_id": emp_id, "emp_name": emp_name, "A": 0.0, "B": 0.0, "C": 0.0, "PRIME": 0.0, "total": 0.0}
        )
        emp_bucket[shift_type] += shift_allowance
        emp_bucket["total"] += shift_allowance

    for client_key, client_bucket in clients.items():
        client_bucket["client_head_count"] = len(client_bucket["client_head_set"])
        del client_bucket["client_head_set"]
        client_bucket["client_total"] = (
            client_bucket["client_A"] + client_bucket["client_B"] +
            client_bucket["client_C"] + client_bucket["client_PRIME"]
        )

        for dept_key, dept_bucket in client_bucket["departments"].items():
            dept_bucket["dept_head_count"] = len(dept_bucket["dept_head_set"])
            del dept_bucket["dept_head_set"]
            dept_bucket["employees"] = list(dept_bucket["employees"].values())

    total_row = {
        "total_head_count": len(global_emp_set),
        "A": global_totals["A"],
        "B": global_totals["B"],
        "C": global_totals["C"],
        "PRIME": global_totals["PRIME"],
        "total_allowance": (
            global_totals["A"] + global_totals["B"] + global_totals["C"] + global_totals["PRIME"]
        )
    }

    return {
        "clients": clients,
        "total": total_row
    }
