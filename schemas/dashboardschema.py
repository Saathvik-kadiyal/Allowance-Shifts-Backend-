from pydantic import BaseModel
from typing import List
 
class PieChartClientShift(BaseModel):
    client_name: str
    total_employees: int
    shift_a: int
    shift_b: int
    shift_c: int
    prime: int
    total_days: int
    total_allowances: float
 
class Config:
    from_attributes= True
 
class VerticalGraphResponse(BaseModel):
    client_name: str
    total_days: float
    total_allowances: float

class VerticalBarResponse(BaseModel):
    months_range: str
    clients: List[VerticalGraphResponse]