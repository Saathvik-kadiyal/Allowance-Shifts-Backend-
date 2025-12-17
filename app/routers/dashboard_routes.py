from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from db import get_db
from utils.dependencies import get_current_user

from schemas.dashboardschema import (
    HorizontalBarResponse,
    GraphResponse,
    VerticalGraphResponse,
    PieChartClientShift,
    ClientList,
    DashboardFilterRequest
)

from services.dashboard_service import (
    get_horizontal_bar_service,
    get_graph_service,
    get_vertical_bar_service,
    get_piechart_shift_summary,
    get_all_clients_service,
    get_client_dashboard_summary
)


router = APIRouter(prefix="/dashboard", tags=["Dashboard"])

@router.get("/horizontal-bar", response_model=dict)
def get_horizontal_bar(
    start_month: str | None = None,
    end_month: str | None = None,
    top: int | None = None,
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user)
):
    return get_horizontal_bar_service(db, start_month, end_month, top)

@router.get("/graph", response_model=dict)
def get_graph(
    client_name: str,
    start_month: str | None = None,
    end_month: str | None = None,
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user)
):
    return get_graph_service(db, client_name, start_month, end_month)

@router.get("/clients", response_model=ClientList)
def get_clients(db: Session = Depends(get_db)):
    return get_all_clients_service(db)


@router.get("/piechart", response_model=list[PieChartClientShift])
def get_piechart(
    start_month: str | None = None,
    end_month: str | None = None,
    top: str | None = None,
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user)
):
    return get_piechart_shift_summary(db, start_month, end_month, top)


@router.get("/vertical-bar", response_model=list[VerticalGraphResponse])
def get_vertical_bar(
    start_month: str | None = None,
    end_month: str | None = None,
    top: str | None = None,
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user)
):
    return get_vertical_bar_service(db, start_month, end_month, top)

@router.post("/client-allowance-summary")
def client_dashboard_summary(
    payload: DashboardFilterRequest,
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user)
):
    return get_client_dashboard_summary(db, payload)