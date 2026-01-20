"""
Excel upload and correction API routes.

This module provides API endpoints to:
- Upload Excel files containing employee shift data.
- Download generated error Excel files for invalid records.
- Submit corrected rows to update previously failed records.

All endpoints are secured and require authentication.
Temporary files are managed through a shared upload service.
"""

import os
from fastapi import APIRouter, UploadFile, Depends, HTTPException, Request
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from db import get_db
from utils.dependencies import get_current_user
from services.upload_service import process_excel_upload, TEMP_FOLDER,update_corrected_rows
from schemas.displayschema import CorrectedRowsRequest

router = APIRouter(prefix="/upload")


# Upload Endpoint
@router.post("/")
async def upload_excel(
    file: UploadFile,
    request: Request,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    """
    Upload an Excel file containing shift data for processing.

    The uploaded file is validated and processed asynchronously.
    Any invalid rows are written to an error Excel file, which can
    later be downloaded for correction.

    Args:
        file (UploadFile): Excel file uploaded by the user.
        request (Request): FastAPI request object used to determine base URL.
        db (Session): Active database session.
        current_user: Authenticated user context.

    Returns:
        dict: Upload result including success status, processed records,
        and error file details if applicable.

    Raises:
        HTTPException: If the file is invalid or processing fails.
    """
    base_url = str(request.base_url).rstrip("/")
    result = await process_excel_upload(file=file, db=db, user=current_user, base_url=base_url)
    return result



# Error File Download Endpoint
@router.get("/error-files/{filename}")
async def download_error_file(filename: str, _current_user=Depends(get_current_user)):
    """
    Download a generated error Excel file.

    This endpoint allows users to download an Excel file that contains
    rows which failed validation during the upload process.

    Args:
        filename (str): Name of the error Excel file.
        _current_user: Authenticated user context.

    Returns:
        FileResponse: Excel file containing invalid records.

    Raises:
        HTTPException: If the requested file does not exist.
    """
    file_path = os.path.join(TEMP_FOLDER, filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(
        file_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=filename
    )

@router.post("/correct_error_rows")
def correct_error_rows(
    payload: CorrectedRowsRequest,
    db: Session = Depends(get_db),
    _current_user=Depends(get_current_user),
):
    """
    Submit corrected rows for previously failed Excel uploads.

    This endpoint accepts corrected data for rows that failed validation
    during the upload process and attempts to reprocess them.

    Args:
        payload (CorrectedRowsRequest): Corrected row data payload.
        db (Session): Active database session.
        _current_user: Authenticated user context.

    Returns:
        dict: Result of the correction operation including success and failure counts.

    Raises:
        HTTPException: If validation or update fails.
    """
    print(payload.corrected_rows)
    return update_corrected_rows(
        db=db,
        corrected_rows=payload.corrected_rows
    )
