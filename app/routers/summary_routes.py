from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from db import get_db
from utils.dependencies import get_current_user
from services.summary_service import get_client_shift_summary
from schemas.displayschema import ClientSummary
from models.models import ShiftAllowances

router = APIRouter(prefix="/summary", tags=["Summary"])


@router.get("/client-shift-summary", response_model=list[ClientSummary])
def client_shift_summary(
    payroll_month: str | None = None,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    if not payroll_month:
        latest_date = db.query(ShiftAllowances.payroll_month).order_by(ShiftAllowances.payroll_month.desc()).first()
        
        if not latest_date or not latest_date[0]:
            raise HTTPException(status_code=404, detail="No payroll data available in the system")

        payroll_month = latest_date[0].strftime("%Y-%m")

    summary = get_client_shift_summary(db, payroll_month)

    if not summary:
        raise HTTPException(
            status_code=404,
            detail=f"No records found for payroll month {payroll_month}"
        )

    return summary
