from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from schemas.access_schema import AccessStatsResponse, RefAvgIncrementalResponse, RefAnalysisDateItem, RefAnalysisDateResponse
from services.access_services import (
    build_ref_response_date,
    build_ref_response_hour,
    build_ref_avg_incremental_response,
    build_ref_analysis_date_response,
)
from repositories.ref_media_bootstrap_repository import create_and_populate_ref_media_dias_table


router = APIRouter(prefix="/access/stats", tags=["Access Stats"])


def parse_br_date(value: str) -> datetime:
    try:
        return datetime.strptime(value, "%d/%m/%Y")
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Data inválida: '{value}'. Use o formato dd/mm/yyyy."
        )


@router.get("/ref/bydate", response_model=AccessStatsResponse)
def get_refeitorio_date(
    start_date: str = Query(..., description="Formato dd/mm/yyyy"),
    end_date: str = Query(..., description="Formato dd/mm/yyyy"),
    device_client_id: Optional[str] = Query(None),
    is_unique: int = Query(..., description="0 = total, 1 = único"),
    event_type_id: int = Query(16777985),
):
    start_date_dt = parse_br_date(start_date)
    end_date_dt = parse_br_date(end_date)

    if end_date_dt <= start_date_dt:
        raise HTTPException(status_code=400, detail="end_date deve ser maior que start_date")

    if is_unique not in (0, 1):
        raise HTTPException(status_code=400, detail="is_unique deve ser 0 ou 1.")

    try:
        return build_ref_response_date(
            device_client_id=device_client_id,
            start_date=start_date_dt,
            end_date=end_date_dt,
            is_unique=is_unique,
            event_type_id=event_type_id,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")


@router.get("/ref/byhour", response_model=AccessStatsResponse)
def get_refeitorio_hour(
    start_date: str = Query(..., description="Formato dd/mm/yyyy"),
    end_date: str = Query(..., description="Formato dd/mm/yyyy"),
    device_client_id: Optional[str] = Query(None),
    is_unique: int = Query(..., description="0 = total, 1 = único"),
    event_type_id: int = Query(16777985),
):
    start_date_dt = parse_br_date(start_date)
    end_date_dt = parse_br_date(end_date)

    if end_date_dt <= start_date_dt:
        raise HTTPException(status_code=400, detail="end_date deve ser maior que start_date")

    if is_unique not in (0, 1):
        raise HTTPException(status_code=400, detail="is_unique deve ser 0 ou 1.")

    try:
        return build_ref_response_hour(
            device_client_id=device_client_id,
            start_date=start_date_dt,
            end_date=end_date_dt,
            is_unique=is_unique,
            event_type_id=event_type_id,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

# Atualiza a tabela com a média incremental 
@router.post("/ref/avgbyday", response_model=RefAvgIncrementalResponse)
def refresh_refeitorio_avg_incremental(
    start_date: str = Query(..., description="Formato dd/mm/yyyy"),
    end_date: str = Query(..., description="Formato dd/mm/yyyy"),
    device_client_id: Optional[str] = Query(None),
    is_unique: int = Query(..., description="0 = total, 1 = único"),
    event_type_id: int = Query(16777985),
):
    start_date_dt = parse_br_date(start_date)
    end_date_dt = parse_br_date(end_date)

    if end_date_dt <= start_date_dt:
        raise HTTPException(status_code=400, detail="end_date deve ser maior que start_date")

    if is_unique not in (0, 1):
        raise HTTPException(status_code=400, detail="is_unique deve ser 0 ou 1.")

    try:
        # 1) atualiza a tabela histórica
        create_and_populate_ref_media_dias_table(
            device_client_id=device_client_id,
            start_date=start_date_dt,
            end_date=end_date_dt,
            is_unique=is_unique,
            event_type_id=event_type_id,
        )

        # 2) retorna a média incremental já considerando a base atualizada
        return build_ref_avg_incremental_response(
            device_client_id=device_client_id,
            start_date=start_date_dt,
            end_date=end_date_dt,
            is_unique=is_unique,
            event_type_id=event_type_id,
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")
    

@router.get("/ref/analysis_date", response_model=RefAnalysisDateResponse)
def get_refeitorio_analysis_date(
    start_date: str = Query(..., description="Formato dd/mm/yyyy"),
    end_date: str = Query(..., description="Formato dd/mm/yyyy"),
    device_client_id: Optional[str] = Query(None),
    is_unique: int = Query(..., description="0 = total, 1 = único"),
    event_type_id: int = Query(16777985),
):
    start_date_dt = parse_br_date(start_date)
    end_date_dt = parse_br_date(end_date)

    if end_date_dt <= start_date_dt:
        raise HTTPException(status_code=400, detail="end_date deve ser maior que start_date")

    if is_unique not in (0, 1):
        raise HTTPException(status_code=400, detail="is_unique deve ser 0 ou 1.")

    try:
        return build_ref_analysis_date_response(
            device_client_id=device_client_id,
            start_date=start_date_dt,
            end_date=end_date_dt,
            is_unique=is_unique,
            event_type_id=event_type_id,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")