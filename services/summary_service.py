from sqlalchemy.orm import Session
from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount
from sqlalchemy import extract
from decimal import Decimal
from fastapi import HTTPException
import re
 
 
def get_client_shift_summary(db: Session, payroll_month: str):
    """Fetch shift summary filtered by payroll_month (YYYY-MM) including total allowances."""
 
    if " " in payroll_month:
        raise HTTPException(
            status_code=400,
            detail="Spaces are not allowed in payroll_month. Use format YYYY-MM"
        )
 
    if not re.match(r"^\d{4}-\d{2}$", payroll_month):
        raise HTTPException(
            status_code=400,
            detail="Invalid payroll_month format. Expected YYYY-MM"
        )
    year, month = payroll_month.split("-")
 
    records = (
        db.query(ShiftAllowances)
        .filter(
            extract("year", ShiftAllowances.payroll_month) == int(year),
            extract("month", ShiftAllowances.payroll_month) == int(month)
        )
        .all()
    )
 
    if not records:
        return []
 
    summary = {}
 
    for row in records:
        client = row.client or "Unknown"
 
        if client not in summary:
            summary[client] = {
                "employees": set(),
                "shift_a": Decimal(0),
                "shift_b": Decimal(0),
                "shift_c": Decimal(0),
                "prime": Decimal(0),
                "total_allowances": Decimal(0)
            }
 
        summary[client]["employees"].add(row.emp_id)
 
        for mapping in row.shift_mappings:
            shift_type = mapping.shift_type.strip().upper()
            days = Decimal(mapping.days or 0)
 
            if shift_type == "A":
                summary[client]["shift_a"] += days
            elif shift_type == "B":
                summary[client]["shift_b"] += days
            elif shift_type == "C":
                summary[client]["shift_c"] += days
            elif shift_type == "PRIME":
                summary[client]["prime"] += days
 
            rate = (
                db.query(ShiftsAmount.amount)
                .filter(ShiftsAmount.shift_type == shift_type)
                .filter(ShiftsAmount.payroll_year == year)
                .scalar()
            ) or 0
 
            rate = Decimal(str(rate))
            summary[client]["total_allowances"] += days * rate
 
    result = [
        {
            "client": client,
            "total_employees": len(info["employees"]),
            "shift_a_days": float(info["shift_a"]),
            "shift_b_days": float(info["shift_b"]),
            "shift_c_days": float(info["shift_c"]),
            "prime_days": float(info["prime"]),
            "total_allowances": float(info["total_allowances"])
        }
        for client, info in summary.items()
    ]
    grand = {
        "client": "TOTAL",
        "total_employees": sum(len(info["employees"]) for info in summary.values()),
        "shift_a_days": float(sum(info["shift_a"] for info in summary.values())),
        "shift_b_days": float(sum(info["shift_b"] for info in summary.values())),
        "shift_c_days": float(sum(info["shift_c"] for info in summary.values())),
        "prime_days": float(sum(info["prime"] for info in summary.values())),
        "total_allowances": float(sum(info["total_allowances"] for info in summary.values()))
    }

    result.append(grand)
    return result