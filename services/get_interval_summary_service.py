from datetime import datetime
from dateutil.relativedelta import relativedelta
from fastapi import HTTPException
from sqlalchemy.orm import Session
from services.summary_service import get_client_shift_summary

def get_interval_summary_service(start_month: str, end_month: str, db: Session):
    # Validate input format
    try:
        start = datetime.strptime(start_month + "-01", "%Y-%m-%d").date()
        end = datetime.strptime(end_month + "-01", "%Y-%m-%d").date()
    except:
        raise HTTPException(status_code=400, detail="Invalid input month format. Expected YYYY-MM")

    if start > end:
        raise HTTPException(status_code=400, detail="start_month must be <= end_month")

    current = start
    interval_summary = {}

    # Iterate month by month
    while current <= end:
        month_str = current.strftime("%Y-%m")
        month_summary = get_client_shift_summary(db, payroll_month=month_str)

        interval_summary[month_str] = month_summary  # even if empty return empty list
        current += relativedelta(months=1)

    return interval_summary
