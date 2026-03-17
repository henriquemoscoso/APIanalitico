from collections import OrderedDict
from datetime import datetime
from typing import Optional

import pandas as pd

from repositories.load_tables_ref_by_date import load_ref_by_date
from repositories.load_tables_ref_by_hour import load_ref_by_hour

from repositories.ref_media_bootstrap_repository import (
    ref_media_dias_table_exists,
    create_and_populate_ref_media_dias_table,
    load_ref_media_dias_iniciais,
)


def build_ref_response_date(
    *,
    device_client_id: Optional[str] = None,
    start_date: datetime,
    end_date: datetime,
    is_unique: int,
    event_type_id: int = 16777985,
) -> dict:
    df = load_ref_by_date(
        device_client_id=device_client_id,
        start_date=start_date,
        end_date=end_date,
        is_unique=is_unique,
        event_type_id=event_type_id,
    )

    metric_name = "unico" if is_unique == 1 else "total"

    if df.empty:
        return {
            "locale": "pt-BR",
            "date": {
                "start_date": start_date.strftime("%d/%m/%Y"),
                "end_date": end_date.strftime("%d/%m/%Y"),
            },
            "clients": {},
            "summary": {
                metric_name: 0,
            },
        }

    df = df.copy()

    required_columns = ["EventDate", "ClientId", "Periodo"]
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Colunas ausentes no DataFrame: {missing}")

    if is_unique == 1:
        if "TotalUnico" not in df.columns:
            raise ValueError("A consulta não retornou a coluna 'TotalUnico'.")
        df["MetricValue"] = pd.to_numeric(df["TotalUnico"], errors="coerce").fillna(0).astype(int)
    else:
        if "Total" in df.columns:
            df["MetricValue"] = pd.to_numeric(df["Total"], errors="coerce").fillna(0).astype(int)
        elif "TotalEventos" in df.columns:
            df["MetricValue"] = pd.to_numeric(df["TotalEventos"], errors="coerce").fillna(0).astype(int)
        else:
            raise ValueError("A consulta não retornou 'Total' nem 'TotalEventos'.")

    df["EventDate"] = pd.to_datetime(df["EventDate"])
    df["ClientId"] = df["ClientId"].astype(str).str.strip()
    df["Periodo"] = df["Periodo"].astype(str).str.strip()

    df = df.sort_values(["EventDate", "ClientId", "Periodo"])

    clients = OrderedDict()
    summary_value = 0

    for _, row in df.iterrows():
        client_id = row["ClientId"]
        month_key = row["EventDate"].strftime("%Y-%m")
        day_key = row["EventDate"].strftime("%Y-%m-%d")
        periodo = row["Periodo"]
        value = int(row["MetricValue"])

        clients.setdefault(
            client_id,
            {
                "months": OrderedDict(),
                "summary": {
                    metric_name: 0,
                },
            },
        )

        clients[client_id]["months"].setdefault(
            month_key,
            {
                "days": OrderedDict(),
                metric_name: 0,
            },
        )

        clients[client_id]["months"][month_key]["days"].setdefault(day_key, OrderedDict())

        clients[client_id]["months"][month_key]["days"][day_key][periodo] = {
            metric_name: value,
        }

        clients[client_id]["months"][month_key][metric_name] += value
        clients[client_id]["summary"][metric_name] += value
        summary_value += value

    return {
        "locale": "pt-BR",
        "date": {
            "start_date": start_date.strftime("%d/%m/%Y"),
            "end_date": end_date.strftime("%d/%m/%Y"),
        },
        "clients": clients,
        "summary": {
            metric_name: summary_value,
        },
    }


def build_ref_response_hour(
    *,
    device_client_id: Optional[str] = None,
    start_date: datetime,
    end_date: datetime,
    is_unique: int,
    event_type_id: int = 16777985,
) -> dict:
    df = load_ref_by_hour(
        device_client_id=device_client_id,
        start_date=start_date,
        end_date=end_date,
        is_unique=is_unique,
        event_type_id=event_type_id,
    )

    metric_name = "unico" if is_unique == 1 else "total"

    if df.empty:
        return {
            "locale": "pt-BR",
            "date": {
                "start_date": start_date.strftime("%d/%m/%Y"),
                "end_date": end_date.strftime("%d/%m/%Y"),
            },
            "clients": {},
            "summary": {
                metric_name: 0,
            },
        }

    df = df.copy()

    required_columns = ["EventDate", "EventHour", "ClientId", "Periodo"]
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Colunas ausentes no DataFrame: {missing}")

    if is_unique == 1:
        if "TotalUnico" not in df.columns:
            raise ValueError("A consulta não retornou a coluna 'TotalUnico'.")
        df["MetricValue"] = pd.to_numeric(df["TotalUnico"], errors="coerce").fillna(0).astype(int)
    else:
        if "Total" in df.columns:
            df["MetricValue"] = pd.to_numeric(df["Total"], errors="coerce").fillna(0).astype(int)
        elif "TotalEventos" in df.columns:
            df["MetricValue"] = pd.to_numeric(df["TotalEventos"], errors="coerce").fillna(0).astype(int)
        else:
            raise ValueError("A consulta não retornou 'Total' nem 'TotalEventos'.")

    df["EventDate"] = pd.to_datetime(df["EventDate"])
    df["EventHour"] = pd.to_numeric(df["EventHour"], errors="coerce").fillna(0).astype(int)
    df["ClientId"] = df["ClientId"].astype(str).str.strip()
    df["Periodo"] = df["Periodo"].astype(str).str.strip()

    df = df.sort_values(["EventDate", "EventHour", "ClientId", "Periodo"])

    clients = OrderedDict()
    summary_value = 0

    for _, row in df.iterrows():
        client_id = row["ClientId"]
        month_key = row["EventDate"].strftime("%Y-%m")
        day_key = row["EventDate"].strftime("%Y-%m-%d")
        hour_key = f"{int(row['EventHour']):02d}:00"
        periodo = row["Periodo"]
        value = int(row["MetricValue"])

        clients.setdefault(
            client_id,
            {
                "months": OrderedDict(),
                "summary": {
                    metric_name: 0,
                },
            },
        )

        clients[client_id]["months"].setdefault(
            month_key,
            {
                "days": OrderedDict(),
                metric_name: 0,
            },
        )

        clients[client_id]["months"][month_key]["days"].setdefault(day_key, OrderedDict())
        clients[client_id]["months"][month_key]["days"][day_key].setdefault(hour_key, OrderedDict())

        clients[client_id]["months"][month_key]["days"][day_key][hour_key][periodo] = {
            metric_name: value,
        }

        clients[client_id]["months"][month_key][metric_name] += value
        clients[client_id]["summary"][metric_name] += value
        summary_value += value

    return {
        "locale": "pt-BR",
        "date": {
            "start_date": start_date.strftime("%d/%m/%Y"),
            "end_date": end_date.strftime("%d/%m/%Y"),
        },
        "clients": clients,
        "summary": {
            metric_name: summary_value,
        },
    }

def build_ref_avg_incremental_response(
    *,
    device_client_id: Optional[str] = None,
    start_date: datetime,
    end_date: datetime,
    is_unique: int,
    event_type_id: int = 16777985,
) -> dict:
    metric_name = "unico" if is_unique == 1 else "total"

    # Se a tabela ainda não existir, cria e popula com os dados já existentes
    if not ref_media_dias_table_exists():
        create_and_populate_ref_media_dias_table(
            device_client_id=device_client_id,
            start_date=start_date,
            end_date=end_date,
            is_unique=is_unique,
            event_type_id=event_type_id,
        )

    # Dados novos consultados na chamada atual
    df = load_ref_by_date(
        device_client_id=device_client_id,
        start_date=start_date,
        end_date=end_date,
        is_unique=is_unique,
        event_type_id=event_type_id,
    )

    # Base histórica já salva no banco
    df_base = load_ref_media_dias_iniciais(
        is_unique=is_unique,
        device_client_id=device_client_id,
    )

    if df.empty:
        return {
            "locale": "pt-BR",
            "date": {
                "start_date": start_date.strftime("%d/%m/%Y"),
                "end_date": end_date.strftime("%d/%m/%Y"),
            },
            "metric": metric_name,
            "items": [],
            "summary": {
                "grupos_processados": 0,
            },
        }

    df = df.copy()

    required_columns = ["EventDate", "ClientId", "Periodo"]
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Colunas ausentes no DataFrame: {missing}")

    if is_unique == 1:
        if "TotalUnico" not in df.columns:
            raise ValueError("A consulta não retornou a coluna 'TotalUnico'.")
        df["MetricValue"] = pd.to_numeric(df["TotalUnico"], errors="coerce").fillna(0).astype(float)
    else:
        if "Total" in df.columns:
            df["MetricValue"] = pd.to_numeric(df["Total"], errors="coerce").fillna(0).astype(float)
        elif "TotalEventos" in df.columns:
            df["MetricValue"] = pd.to_numeric(df["TotalEventos"], errors="coerce").fillna(0).astype(float)
        else:
            raise ValueError("A consulta não retornou 'Total' nem 'TotalEventos'.")

    df["EventDate"] = pd.to_datetime(df["EventDate"]).dt.normalize()
    df["ClientId"] = df["ClientId"].astype(str).str.strip()
    df["Periodo"] = df["Periodo"].astype(str).str.strip()

    # consolida os dados novos por dia/client/período
    df_grouped = (
        df.groupby(["ClientId", "Periodo", "EventDate"], as_index=False)["MetricValue"]
        .sum()
    )

    # média e quantidade de dias NOVOS consultados
    df_avg_new = (
        df_grouped.groupby(["ClientId", "Periodo"], as_index=False)
        .agg(
            media_periodo_consultado=("MetricValue", "mean"),
            dias_novos=("EventDate", "nunique"),
        )
    )

    # renomeia a base histórica para fazer merge
    df_base = df_base.rename(
        columns={
            "Media": "media_inicial",
            "Dias": "dias_iniciais",
        }
    )

    merged = df_avg_new.merge(
        df_base[["ClientId", "Periodo", "media_inicial", "dias_iniciais"]],
        on=["ClientId", "Periodo"],
        how="left",
    )

    # se aparecer algum grupo novo que ainda não está na tabela
    merged["media_inicial"] = merged["media_inicial"].fillna(0.0).astype(float)
    merged["dias_iniciais"] = merged["dias_iniciais"].fillna(0).astype(int)

    merged["dias_totais"] = merged["dias_iniciais"] + merged["dias_novos"]

    merged["media_atualizada"] = merged.apply(
        lambda row: (
            (
                (row["media_inicial"] * row["dias_iniciais"]) +
                (row["media_periodo_consultado"] * row["dias_novos"])
            ) / row["dias_totais"]
        ) if row["dias_totais"] > 0 else 0.0,
        axis=1,
    )

    items = []

    for _, row in merged.sort_values(["ClientId", "Periodo"]).iterrows():
        items.append({
            "client_id": row["ClientId"],
            "periodo": row["Periodo"],
            "media_inicial": round(float(row["media_inicial"]), 2),
            "dias_iniciais": int(row["dias_iniciais"]),
            "media_periodo_consultado": round(float(row["media_periodo_consultado"]), 2),
            "dias_novos": int(row["dias_novos"]),
            "media_atualizada": round(float(row["media_atualizada"]), 2),
            "dias_totais": int(row["dias_totais"]),
        })

    return {
        "locale": "pt-BR",
        "date": {
            "start_date": start_date.strftime("%d/%m/%Y"),
            "end_date": end_date.strftime("%d/%m/%Y"),
        },
        "metric": metric_name,
        "items": items,
        "summary": {
            "grupos_processados": len(items),
        },
    }