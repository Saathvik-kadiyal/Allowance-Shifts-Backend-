import os
import uuid
import io
import pandas as pd
from fastapi import HTTPException
from sqlalchemy.orm import Session
from models.models import UploadedFiles, ShiftAllowances
from models.enums import ExcelColumnMap

# Folder for saving generated error Excels
TEMP_FOLDER = "media/error_excels"
os.makedirs(TEMP_FOLDER, exist_ok=True)


# Helper: Validate Excel Data
def validate_excel_data(df: pd.DataFrame, required_columns: list, numeric_columns: list):
    errors = []
    error_rows = []

    for idx, row in df.iterrows():
        row_errors = []
        for col in numeric_columns:
            value = row[col]
            # Type check
            if not pd.api.types.is_numeric_dtype(type(value)):
                try:
                    df.at[idx, col] = pd.to_numeric(value)
                except (ValueError, TypeError):
                    row_errors.append(f"Invalid value in '{col}' → '{value}' (expected numeric)")
        if row_errors:
            row_data = row.to_dict()
            row_data["error"] = "; ".join(row_errors)
            error_rows.append(row_data)
            errors.append(idx)

    clean_df = df.drop(index=errors).reset_index(drop=True)
    error_df = pd.DataFrame(error_rows) if error_rows else None
    return clean_df, error_df


#Main Upload Processor
async def process_excel_upload(file, db: Session, user, base_url: str):
    uploaded_by = user.id

    if not file.filename.endswith((".xls", ".xlsx")):
        raise HTTPException(status_code=400, detail="Only Excel files are allowed")

    uploaded_file = UploadedFiles(
        filename=file.filename,
        uploaded_by=uploaded_by,
        status="processing"
    )
    db.add(uploaded_file)
    db.commit()
    db.refresh(uploaded_file)

    try:
        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents))

        column_mapping = {e.value: e.name for e in ExcelColumnMap}
        df.rename(columns=column_mapping, inplace=True)

        required_columns = [e.name for e in ExcelColumnMap]
        missing = [c for c in required_columns if c not in df.columns]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing columns in Excel: {missing}")

        df = df.where(pd.notnull(df), 0)

        numeric_columns = [
            "shift_a_days", "shift_b_days", "shift_c_days",
            "prime_days", "total_days", "billable_days",
            "non_billable_days", "diff", "final_total_days"
        ]

        clean_df, error_df = validate_excel_data(df, required_columns, numeric_columns)

        # Always insert valid rows (if any)
        inserted_count = 0
        if not clean_df.empty:
            clean_df = clean_df.astype({col: "int64" for col in numeric_columns})
            shift_records = [
                ShiftAllowances(file_id=uploaded_file.id, **row)
                for row in clean_df[required_columns].to_dict(orient="records")
            ]
            db.bulk_save_objects(shift_records)
            db.commit()
            inserted_count = len(shift_records)

        # If invalid rows exist, generate downloadable Excel
        if error_df is not None and not error_df.empty:
            error_filename = f"error_{uuid.uuid4().hex}.xlsx"
            error_path = os.path.join(TEMP_FOLDER, error_filename)
            error_df.to_excel(error_path, index=False)

            uploaded_file.status = "partially_processed"
            uploaded_file.record_count = inserted_count
            db.commit()

            error_download_link = f"{base_url}/upload/error-files/{error_filename}"

            return {
                "message": "File partially processed. Some rows contained invalid data.",
                "records_inserted": inserted_count,
                "records_skipped": len(error_df),
                "download_link": error_download_link,
                "file_name": error_filename
            }

        # All rows valid
        uploaded_file.status = "processed"
        uploaded_file.record_count = inserted_count
        db.commit()

        return {
            "message": "File processed successfully",
            "file_id": uploaded_file.id,
            "records": inserted_count
        }

    except HTTPException:
        db.rollback()
        uploaded_file.status = "failed"
        db.commit()
        raise

    except Exception as e:
        db.rollback()
        uploaded_file.status = "failed"
        db.commit()
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")
