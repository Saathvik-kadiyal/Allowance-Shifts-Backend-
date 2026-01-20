"""
Routes for downloading client summary reports.
"""
 
from fastapi import APIRouter, Depends, Body
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from db import get_db
from services.client_summary_download_service import client_summary_download_service
from utils.dependencies import get_current_user
 
router = APIRouter(prefix="/client-summary")
 
 
@router.post("/download")
def download_client_summary_excel(
    payload: dict = Body(
        ...,
        example={
            "clients": "ALL",                
            "emp_id": "IN01804611",            
            "account_manager": "John Doe",    
            "start_month": "YYYY-MM",          
            "end_month": "YYYY-MM",            
            "selected_year": "YYYY",          
            "selected_months": ["01", "02"],  
            "selected_quarters": ["Q1"]  
        }
    ),
    db: Session = Depends(get_db),
    _current_user=Depends(get_current_user),
):
    """
    Generate and download the client summary Excel report.
 
    Behavior:
    - If no month, range, or quarter is provided, defaults to the latest month available in the DB.
    - Filters by `emp_id` and `account_manager` if provided.
    - Clients can be "ALL" or a mapping {client: [departments]}.
    - Returns an Excel file (cached if default latest-month request).
    """
    file_path = client_summary_download_service(db=db, payload=payload)
 
    return FileResponse(
        path=file_path,
        filename="client_summary.xlsx",
        media_type=(
            "application/vnd.openxmlformats-officedocument."
            "spreadsheetml.sheet"
        ),
    )
 
 