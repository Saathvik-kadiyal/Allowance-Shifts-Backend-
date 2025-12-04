from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from db import get_db
from services.search_service import export_filtered_excel as get_employee_details
from utils.dependencies import get_current_user

router = APIRouter(prefix="/employee-details", tags=["Search Details"])

@router.get("/Search")
def fetch_employee_details(
    emp_id: str | None = Query(None),
    account_manager: str | None = Query(None),
    start_month: str | None = Query(None),
    end_month: str | None = Query(None),
    start: int = Query(0, ge=0),
    limit: int = Query(10, gt=0),
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
):
    total_records, data = get_employee_details(
        db, emp_id, account_manager, start_month, end_month, start, limit
    )

    return {
        "total_records": total_records,
        "data": data
    }