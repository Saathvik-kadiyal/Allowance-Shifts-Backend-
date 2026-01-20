"""
Client summary service for month, quarter, and range based analytics.
"""
 
from datetime import date, datetime
from typing import List, Dict
from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_, cast, Integer
 
from models.models import ShiftAllowances, ShiftMapping, ShiftsAmount
 
from diskcache import Cache
 
cache = Cache("./diskcache/latest_month")
LATEST_MONTH_KEY = "client_summary:latest_month"
CACHE_TTL = 24 * 60 * 60
 
 
def is_default_latest_month_request(payload: dict) -> bool:
    return (
        not payload
        or (
            payload.get("clients") in (None, "ALL")
            and not payload.get("selected_year")
            and not payload.get("selected_months")
            and not payload.get("selected_quarters")
            and not payload.get("start_month")
            and not payload.get("end_month")
            and not payload.get("emp_id")
            and not payload.get("account_manager")
        )
    )
 
 
def validate_year(year: int):
    current_year = date.today().year
    if year <= 0:
        raise HTTPException(400, "selected_year must be greater than 0")
    if year > current_year:
        raise HTTPException(400, "selected_year cannot be in the future")
 
 
def parse_yyyy_mm(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m").date().replace(day=1)
    except Exception:
        raise HTTPException(400, "Invalid month format. Expected YYYY-MM")
 
 
def quarter_to_months(q: str) -> List[int]:
    mapping = {
        "Q1": [1, 2, 3],
        "Q2": [4, 5, 6],
        "Q3": [7, 8, 9],
        "Q4": [10, 11, 12],
    }
    q = q.upper().strip()
    if q not in mapping:
        raise HTTPException(400, "Invalid quarter (expected Q1–Q4)")
    return mapping[q]
 
 
def month_range(start: date, end: date) -> List[date]:
    if start > end:
        raise HTTPException(400, "start_month cannot be after end_month")
 
    months = []
    cur = start
    while cur <= end:
        months.append(cur)
        year = cur.year + (cur.month // 12)
        month = (cur.month % 12) + 1
        cur = cur.replace(year=year, month=month)
    return months
 
 
def empty_shift_totals():
    return {"A": 0, "B": 0, "C": 0, "PRIME": 0}
 
 
def client_summary_service(db: Session, payload: dict):
    payload = payload or {}
 
    emp_id = payload.get("emp_id")
    account_manager = payload.get("account_manager")
 
   
    if is_default_latest_month_request(payload):
        cached = cache.get(LATEST_MONTH_KEY)
        if cached:
            return cached["data"]
 
    selected_year = payload.get("selected_year")
    selected_months = payload.get("selected_months", [])
    selected_quarters = payload.get("selected_quarters", [])
    start_month = payload.get("start_month")
    end_month = payload.get("end_month")
    clients_payload = payload.get("clients")
 
    months: List[date] = []
    quarter_map: Dict[str, List[date]] = {}
 
    client_name_map = {}
    dept_name_map = {}
 
    if not clients_payload or clients_payload == "ALL":
        normalized_clients = {}
 
        if not selected_year and not selected_months and not selected_quarters and not start_month and not end_month:
            latest_month = db.query(func.max(ShiftAllowances.duration_month)).scalar()
            if not latest_month:
                raise HTTPException(404, "No data available in database")
 
            months = [date(latest_month.year, latest_month.month, 1)]
            selected_year = str(latest_month.year)
 
    elif isinstance(clients_payload, dict):
        normalized_clients = {}
 
        for client, depts in clients_payload.items():
            client_lc = client.lower()
            client_name_map[client_lc] = client
            normalized_clients[client_lc] = []
 
            for dept in (depts or []):
                dept_lc = dept.lower()
                dept_name_map[(client_lc, dept_lc)] = dept
                normalized_clients[client_lc].append(dept_lc)
 
        if not selected_year and not selected_months and not selected_quarters and not start_month and not end_month:
            latest_month = db.query(func.max(ShiftAllowances.duration_month)).scalar()
            if not latest_month:
                raise HTTPException(404, "No data available in database")
 
            months = [date(latest_month.year, latest_month.month, 1)]
            selected_year = str(latest_month.year)
 
    else:
        raise HTTPException(400, "clients must be 'ALL' or {client: [departments]}")
 
   
    if (selected_months or selected_quarters) and not selected_year:
        raise HTTPException(400, "selected_year is mandatory")
 
    if start_month and end_month:
        months = month_range(parse_yyyy_mm(start_month), parse_yyyy_mm(end_month))
 
    elif selected_months:
        validate_year(int(selected_year))
        months = [date(int(selected_year), int(m), 1) for m in selected_months]
 
    elif selected_quarters:
        validate_year(int(selected_year))
        for q in selected_quarters:
            mlist = [date(int(selected_year), m, 1) for m in quarter_to_months(q)]
            quarter_map[f"{mlist[0]:%Y-%m} - {mlist[-1]:%Y-%m}"] = mlist
 
    elif not months:
        raise HTTPException(400, "No valid date filter provided")
 
    response: Dict = {}
 
    if selected_quarters:
        for q in quarter_map:
            response[q] = {"message": f"No data found for {q}"}
    else:
        for m in months:
            response[m.strftime("%Y-%m")] = {"message": f"No data found for {m:%Y-%m}"}
 
 
    query = (
        db.query(
            ShiftAllowances.duration_month,
            ShiftAllowances.client,
            ShiftAllowances.department,
            ShiftAllowances.emp_id,
            ShiftAllowances.emp_name,
            ShiftAllowances.account_manager,
            ShiftMapping.shift_type,
            ShiftMapping.days,
            ShiftsAmount.amount,
        )
        .join(ShiftMapping, ShiftMapping.shiftallowance_id == ShiftAllowances.id)
        .outerjoin(
            ShiftsAmount,
            and_(
                ShiftMapping.shift_type == ShiftsAmount.shift_type,
                cast(ShiftsAmount.payroll_year, Integer)
                == func.extract("year", ShiftAllowances.duration_month),
            ),
        )
    )
 
 
    if normalized_clients:
        filters = []
        for client_lc, depts_lc in normalized_clients.items():
            if depts_lc:
                filters.append(
                    and_(
                        func.lower(ShiftAllowances.client) == client_lc,
                        func.lower(ShiftAllowances.department).in_(depts_lc),
                    )
                )
            else:
                filters.append(func.lower(ShiftAllowances.client) == client_lc)
 
        query = query.filter(or_(*filters))
 
    if emp_id:
        query = query.filter(
            func.lower(ShiftAllowances.emp_id) == emp_id.lower()
        )
 
    if account_manager:
        if isinstance(account_manager, list):
            account_manager = account_manager[0]
        
        query = query.filter(
        func.lower(ShiftAllowances.account_manager)
        == account_manager.lower()
    )

 
   
    date_list = (
        [m for ml in quarter_map.values() for m in ml]
        if selected_quarters
        else months
    )
 
    query = query.filter(
        or_(
            *[
                and_(
                    func.extract("year", ShiftAllowances.duration_month) == m.year,
                    func.extract("month", ShiftAllowances.duration_month) == m.month,
                )
                for m in date_list
            ]
        )
    )
 
    rows = query.all()
 
 
    for dm, client, dept, eid, ename, acc_mgr, stype, days, amt in rows:
 
        period_key = (
            next(
                (q for q, ml in quarter_map.items() if dm.replace(day=1) in ml),
                None,
            )
            if selected_quarters
            else dm.strftime("%Y-%m")
        )
 
        if not period_key:
            continue
 
        if "message" in response.get(period_key, {}):
            response[period_key] = {
                "clients": {},
                "month_total": {
                    "total_head_count": 0,
                    **empty_shift_totals(),
                    "total_allowance": 0,
                },
            }
 
        client_name = client_name_map.get(client.lower(), client)
        dept_name = dept_name_map.get((client.lower(), dept.lower()), dept)
 
        total = float(days or 0) * float(amt or 0)
        month_block = response[period_key]
 
        client_block = month_block["clients"].setdefault(
            client_name,
            {
                **{f"client_{k}": 0 for k in ["A", "B", "C", "PRIME"]},
                "departments": {},
                "client_head_count": 0,
                "client_total": 0,
            },
        )
 
        dept_block = client_block["departments"].setdefault(
            dept_name,
            {
                **{f"dept_{k}": 0 for k in ["A", "B", "C", "PRIME"]},
                "dept_total": 0,
                "employees": [],
                "dept_head_count": 0,
            },
        )
 
        emp = next((e for e in dept_block["employees"] if e["emp_id"] == eid), None)
        if not emp:
            emp = {
                "emp_id": eid,
                "emp_name": ename,
                "account_manager": acc_mgr,
                **empty_shift_totals(),
                "total": 0,
            }
            dept_block["employees"].append(emp)
            dept_block["dept_head_count"] += 1
            client_block["client_head_count"] += 1
            month_block["month_total"]["total_head_count"] += 1
 
        emp[stype] += total
        emp["total"] += total
        dept_block[f"dept_{stype}"] += total
        dept_block["dept_total"] += total
        client_block[f"client_{stype}"] += total
        client_block["client_total"] += total
        month_block["month_total"][stype] += total
        month_block["month_total"]["total_allowance"] += total
 
 
    if is_default_latest_month_request(payload):
        cache.set(
            LATEST_MONTH_KEY,
            {
                "_cached_month": months[0].strftime("%Y-%m"),
                "data": response,
            },
            expire=CACHE_TTL,
        )
 
    return response
 
 