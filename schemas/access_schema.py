from typing import Any, Dict, List
from pydantic import BaseModel


class AccessStatsResponse(BaseModel):
    locale: str
    date: Dict[str, str]
    clients: Dict[str, Any]
    summary: Dict[str, int]


class RefAvgIncrementalItem(BaseModel):
    client_id: str
    periodo: str
    media_inicial: float
    dias_iniciais: int
    media_periodo_consultado: float
    dias_novos: int
    media_atualizada: float
    dias_totais: int


class RefAvgIncrementalResponse(BaseModel):
    locale: str
    date: Dict[str, str]
    metric: str
    items: List[RefAvgIncrementalItem]
    summary: Dict[str, Any]

class RefAnalysisDateItem(BaseModel):
    client_id: str
    event_date: str
    periodo: str
    valor_dia: float
    media_historica: float
    diferenca_percentual: float
    validation: str


class RefAnalysisDateResponse(BaseModel):
    locale: str
    date: Dict[str, str]
    metric: str
    items: List[RefAnalysisDateItem]
    summary: Dict[str, Any]
