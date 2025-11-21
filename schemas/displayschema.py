from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime,date


class ShiftAllowancesResponse(BaseModel):
    id: int
    emp_id: str
    emp_name: str
    department: str
    month: date
    client: str
    project_code: Optional[str]
    shift_category: List[str]
 
    class Config:
        from_attributes = True


class EmployeeResponse(BaseModel):
    id: int
    emp_id: Optional[str]
    emp_name: Optional[str]
    grade: Optional[str]
    department: Optional[str]
    client: Optional[str]
    project: Optional[str]
    project_code: Optional[str]
    account_manager: Optional[str]
    practice_lead: Optional[str]
    delivery_manager: Optional[str]
    month_year: Optional[date]
    duration_month: Optional[date]
    payroll_month: Optional[date]
    shift_a_days: Optional[int] = 0
    shift_b_days: Optional[int] = 0
    shift_c_days: Optional[int] = 0
    prime_days: Optional[int] = 0
    total_days: Optional[int] = 0
    billability_status: Optional[str]
    practice_remarks: Optional[str]
    rmg_comments: Optional[str]
    amar_approval: Optional[str]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class PaginatedShiftResponse(BaseModel):
    total_records: int
    data: List[ShiftAllowancesResponse]

    class Config:
        from_attributes = True

class PartialUpdateShiftRequest(BaseModel):
    shift_a_days: Optional[int] = None
    shift_b_days: Optional[int] = None
    shift_c_days: Optional[int] = None
    prime_days: Optional[int] = None
 
    class Config:
        from_attributes = True
       
 
 
class PartialUpdateShiftResponse(BaseModel):
    message: str
    updated_fields: List[str]
    shift_a_days: Optional[int]
    shift_b_days: Optional[int]
    shift_c_days: Optional[int]
    prime_days: Optional[int]
    total_days: Optional[int]
    total_days_allowance: Optional[float]
 
    class Config:
        from_attributes = True