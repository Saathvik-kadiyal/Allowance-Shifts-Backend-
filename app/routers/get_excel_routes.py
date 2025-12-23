"""
Routes for exporting filtered shift data as Excel files.
"""


import io

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from db import get_db
from services.get_excel_service import export_filtered_excel
from utils.dependencies import get_current_user

router = APIRouter(prefix="/excel", tags=["Excel Data"])

@router.get("/download")
def download_excel(
    emp_id: str | None = Query(None),
    account_manager: str | None = Query(None),
    department: str | None = Query(None),
    client: str | None = Query(None),
    start_month: str | None = Query(None),
    end_month: str | None = Query(None),
    db: Session = Depends(get_db),
    _current_user = Depends(get_current_user),
):
    """Download filtered shift data as an Excel file."""

    df = export_filtered_excel(
        db=db,
        emp_id=emp_id,
        account_manager=account_manager,
        start_month=start_month,
        end_month=end_month,
        department=department,
        client=client
    )

    file_stream = io.BytesIO()
    df.to_excel(file_stream, index=False)
    file_stream.seek(0)

    return StreamingResponse(
        file_stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=shift_data.xlsx"}
    )
