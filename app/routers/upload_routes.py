import os
from fastapi import APIRouter, UploadFile, Depends, HTTPException, Request
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from db import get_db
from utils.dependencies import get_current_user
from services.upload_service import process_excel_upload, TEMP_FOLDER

router = APIRouter(prefix="/upload")


# Upload Endpoint
@router.post("/")
async def upload_excel(
    file: UploadFile,
    request: Request,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    base_url = str(request.base_url).rstrip("/")
    result = await process_excel_upload(file=file, db=db, user=current_user, base_url=base_url)
    return result


# Error File Download Endpoint
@router.get("/error-files/{filename}")
async def download_error_file(filename: str, current_user=Depends(get_current_user)):
    file_path = os.path.join(TEMP_FOLDER, filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(
        file_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=filename
    )
