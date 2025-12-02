from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from db import get_db
from services.client_summary_service import client_summary_service

router = APIRouter()

@router.get("/client-summary")
def client_summary(
    start_month: str | None = Query(None),
    end_month: str | None = Query(None),
    db: Session = Depends(get_db),
):
    return client_summary_service(
        db=db,
        start_month=start_month,
        end_month=end_month
    )