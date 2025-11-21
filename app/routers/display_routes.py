from fastapi import APIRouter, Depends, HTTPException, Query,Body
from sqlalchemy.orm import Session
from db import get_db
from models.models import ShiftAllowances,ShiftMapping
from utils.dependencies import get_current_user
from schemas.displayschema import PaginatedShiftResponse,EmployeeResponse,PartialUpdateShiftRequest,PartialUpdateShiftResponse
from services.display_service import partial_update_shift
from sqlalchemy import func

router = APIRouter(prefix="/display")

@router.get("/", response_model=PaginatedShiftResponse)
def get_all_data(
    start: int = Query(0, ge=0),
    limit: int = Query(10, gt=0),
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
):
    query = (
        db.query(
            ShiftAllowances.id.label("id"),
            ShiftAllowances.emp_id.label("emp_id"),
            ShiftAllowances.emp_name.label("emp_name"),
            ShiftAllowances.department.label("department"),
            ShiftAllowances.payroll_month.label("month"),
            ShiftAllowances.client.label("client"),
            ShiftAllowances.project_code.label("project_code"),
            func.array_agg(ShiftMapping.shift_type).label("shift_category")
        )
        .outerjoin(ShiftMapping, ShiftAllowances.id == ShiftMapping.shiftallowance_id)
        .group_by(ShiftAllowances.id)
    )
 
    total_records = query.count()
 
    data = (
        query.order_by(ShiftAllowances.id.asc())
        .offset(start)
        .limit(limit)
        .all()
    )
 
    if not data:
        raise HTTPException(status_code=404, detail="No data found for given range")
 
    return {
        "total_records": total_records,
        "data": data
    }

@router.get("/{id}",response_model=EmployeeResponse)
def get_detail_page(id:int, 
                    db:Session = Depends(get_db),
                    current_user=Depends(get_current_user),):
    data = db.query(ShiftAllowances).filter(ShiftAllowances.id==id).first()
    if not data:
        raise HTTPException(status_code=404,detail="Given id doesn't exist")
    return data

@router.patch(
    "/shift/partial-update/{id}",
    response_model=PartialUpdateShiftResponse
)
def partial_update_shift_route(
    id: int,
    updates: PartialUpdateShiftRequest = Body(...),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    update_data = updates.model_dump(exclude_unset=True)
    if not update_data:
        raise HTTPException(status_code=400, detail="No updates provided")
 
    updated_record = partial_update_shift(db=db, record_id=id, updates=update_data)
 
    return {
        "message": f"Record ID {id} updated successfully",
        "updated_fields": list(update_data.keys()),
        "shift_a_days": updated_record.shift_a_days,
        "shift_b_days": updated_record.shift_b_days,
        "shift_c_days": updated_record.shift_c_days,
        "prime_days": updated_record.prime_days,
        "total_days": updated_record.total_days,
        "total_days_allowance": getattr(updated_record, "total_days_allowance", 0),
    }