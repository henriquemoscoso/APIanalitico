from __future__ import annotations

from datetime import datetime
from typing import Optional

import pandas as pd

from repositories.db import get_conn
from repositories.load_tables_ref_by_date import load_ref_by_date

TABLE_NAME = "dbo.RefMediaDiasIniciais"


def ref_media_dias_table_exists() -> bool:
    conn = get_conn()
    try:
        query = """
            SELECT 1
            FROM sys.tables t
            INNER JOIN sys.schemas s
                ON s.schema_id = t.schema_id
            WHERE t.name = 'RefMediaDiasIniciais'
              AND s.name = 'dbo'
        """
        df = pd.read_sql_query(query, conn)
        return not df.empty
    finally:
        conn.close()


def create_ref_media_dias_table() -> None:
    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
        IF OBJECT_ID('{TABLE_NAME}', 'U') IS NULL
        BEGIN
            CREATE TABLE {TABLE_NAME} (
                ClientId   VARCHAR(50)    NOT NULL,
                Periodo    VARCHAR(50)    NOT NULL,
                Media      DECIMAL(18,4)  NOT NULL,
                Dias       INT            NOT NULL,
                IsUnique   BIT            NOT NULL,
                DataInicio DATETIME       NOT NULL,
                DataFim    DATETIME       NOT NULL,
                UpdatedAt  DATETIME       NOT NULL DEFAULT GETDATE(),
                CONSTRAINT PK_RefMediaDiasIniciais
                    PRIMARY KEY (ClientId, Periodo, IsUnique)
            );
        END
        """)
        conn.commit()
    finally:
        cursor.close()
        conn.close()


def build_ref_media_dias_dataframe(
    *,
    device_client_id: Optional[str] = None,
    start_date: datetime,
    end_date: datetime,
    is_unique: int,
    event_type_id: int = 16777985,
) -> pd.DataFrame:
    df = load_ref_by_date(
        device_client_id=device_client_id,
        start_date=start_date,
        end_date=end_date,
        is_unique=is_unique,
        event_type_id=event_type_id,
    )

    if df.empty:
        return pd.DataFrame(
            columns=[
                "ClientId",
                "Periodo",
                "Media",
                "Dias",
                "IsUnique",
                "DataInicio",
                "DataFim",
            ]
        )

    required_columns = ["EventDate", "ClientId", "Periodo"]
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Colunas ausentes no DataFrame: {missing}")

    df = df.copy()

    if is_unique == 1:
        if "TotalUnico" not in df.columns:
            raise ValueError("A consulta não retornou a coluna 'TotalUnico'.")
        df["MetricValue"] = pd.to_numeric(df["TotalUnico"], errors="coerce").fillna(0)
    else:
        if "Total" in df.columns:
            df["MetricValue"] = pd.to_numeric(df["Total"], errors="coerce").fillna(0)
        elif "TotalEventos" in df.columns:
            df["MetricValue"] = pd.to_numeric(df["TotalEventos"], errors="coerce").fillna(0)
        else:
            raise ValueError("A consulta não retornou 'Total' nem 'TotalEventos'.")

    df["EventDate"] = pd.to_datetime(df["EventDate"]).dt.normalize()
    df["ClientId"] = df["ClientId"].astype(str).str.strip()
    df["Periodo"] = df["Periodo"].astype(str).str.strip()

    daily = (
        df.groupby(["ClientId", "Periodo", "EventDate"], as_index=False)["MetricValue"]
        .sum()
    )

    result = (
        daily.groupby(["ClientId", "Periodo"], as_index=False)
        .agg(
            Media=("MetricValue", "mean"),
            Dias=("EventDate", "nunique"),
        )
        .sort_values(["ClientId", "Periodo"])
        .reset_index(drop=True)
    )

    result["Media"] = result["Media"].round(4)
    result["Dias"] = result["Dias"].astype(int)
    result["IsUnique"] = int(is_unique)
    result["DataInicio"] = start_date
    result["DataFim"] = end_date

    return result[
        ["ClientId", "Periodo", "Media", "Dias", "IsUnique", "DataInicio", "DataFim"]
    ]


def upsert_ref_media_dias_dataframe(df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    conn = get_conn()
    cursor = conn.cursor()

    try:
        for _, row in df.iterrows():
            cursor.execute(f"""
                MERGE {TABLE_NAME} AS target
                USING (
                    SELECT
                        ? AS ClientId,
                        ? AS Periodo,
                        ? AS IsUnique
                ) AS source
                ON target.ClientId = source.ClientId
                   AND target.Periodo = source.Periodo
                   AND target.IsUnique = source.IsUnique

                WHEN MATCHED THEN
                    UPDATE SET
                        Media = ?,
                        Dias = ?,
                        DataInicio = ?,
                        DataFim = ?,
                        UpdatedAt = GETDATE()

                WHEN NOT MATCHED THEN
                    INSERT (
                        ClientId,
                        Periodo,
                        Media,
                        Dias,
                        IsUnique,
                        DataInicio,
                        DataFim,
                        UpdatedAt
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, GETDATE());
            """,
                row["ClientId"],
                row["Periodo"],
                int(row["IsUnique"]),
                float(row["Media"]),
                int(row["Dias"]),
                row["DataInicio"],
                row["DataFim"],
                row["ClientId"],
                row["Periodo"],
                float(row["Media"]),
                int(row["Dias"]),
                int(row["IsUnique"]),
                row["DataInicio"],
                row["DataFim"],
            )

        conn.commit()
        return len(df)
    finally:
        cursor.close()
        conn.close()


def create_and_populate_ref_media_dias_table(
    *,
    device_client_id: Optional[str] = None,
    start_date: datetime,
    end_date: datetime,
    is_unique: int,
    event_type_id: int = 16777985,
) -> dict:
    create_ref_media_dias_table()

    df_result = build_ref_media_dias_dataframe(
        device_client_id=device_client_id,
        start_date=start_date,
        end_date=end_date,
        is_unique=is_unique,
        event_type_id=event_type_id,
    )

    affected_rows = upsert_ref_media_dias_dataframe(df_result)

    return {
        "table_name": TABLE_NAME,
        "rows_written": affected_rows,
        "is_unique": is_unique,
        "start_date": start_date.strftime("%d/%m/%Y"),
        "end_date": end_date.strftime("%d/%m/%Y"),
        "items": df_result.to_dict(orient="records"),
    }


def load_ref_media_dias_iniciais(
    *,
    is_unique: int,
    device_client_id: Optional[str] = None,
) -> pd.DataFrame:
    conn = get_conn()
    try:
        query = f"""
            SELECT
                ClientId,
                Periodo,
                Media,
                Dias,
                IsUnique
            FROM {TABLE_NAME}
            WHERE IsUnique = ?
        """

        params = [is_unique]

        if device_client_id:
            query += " AND ClientId = ?"
            params.append(device_client_id)

        df = pd.read_sql_query(query, conn, params=params)

        if df.empty:
            return pd.DataFrame(columns=["ClientId", "Periodo", "Media", "Dias", "IsUnique"])

        df = df.copy()
        df["ClientId"] = df["ClientId"].astype(str).str.strip()
        df["Periodo"] = df["Periodo"].astype(str).str.strip()
        df["Media"] = pd.to_numeric(df["Media"], errors="coerce").fillna(0).astype(float)
        df["Dias"] = pd.to_numeric(df["Dias"], errors="coerce").fillna(0).astype(int)

        return df
    finally:
        conn.close()

def ref_media_dias_table_exists() -> bool:
    conn = get_conn()
    try:
        query = """
            SELECT 1
            FROM sys.tables t
            INNER JOIN sys.schemas s
                ON s.schema_id = t.schema_id
            WHERE t.name = 'RefMediaDiasIniciais'
              AND s.name = 'dbo'
        """
        df = pd.read_sql_query(query, conn)
        return not df.empty
    finally:
        conn.close()


def load_ref_media_dias_iniciais(
    *,
    is_unique: int,
    device_client_id: Optional[str] = None,
) -> pd.DataFrame:
    conn = get_conn()
    try:
        query = f"""
            SELECT
                ClientId,
                Periodo,
                Media,
                Dias,
                IsUnique
            FROM {TABLE_NAME}
            WHERE IsUnique = ?
        """

        params = [is_unique]

        if device_client_id:
            query += " AND ClientId = ?"
            params.append(device_client_id)

        df = pd.read_sql_query(query, conn, params=params)

        if df.empty:
            return pd.DataFrame(columns=["ClientId", "Periodo", "Media", "Dias", "IsUnique"])

        df = df.copy()
        df["ClientId"] = df["ClientId"].astype(str).str.strip()
        df["Periodo"] = df["Periodo"].astype(str).str.strip()
        df["Media"] = pd.to_numeric(df["Media"], errors="coerce").fillna(0).astype(float)
        df["Dias"] = pd.to_numeric(df["Dias"], errors="coerce").fillna(0).astype(int)

        return df
    finally:
        conn.close()

def load_ref_media_dias_historica(
    *,
    is_unique: int,
    device_client_id: Optional[str] = None,
) -> pd.DataFrame:
    conn = get_conn()
    try:
        query = """
            SELECT
                LTRIM(RTRIM(ClientId)) AS ClientId,
                LTRIM(RTRIM(Periodo)) AS Periodo,
                CAST(Media AS float) AS Media,
                CAST(Dias AS int) AS Dias,
                IsUnique
            FROM dbo.RefMediaDiasIniciais
            WHERE IsUnique = ?
              AND (? IS NULL OR LTRIM(RTRIM(ClientId)) = LTRIM(RTRIM(?)))
        """

        params = [is_unique, device_client_id, device_client_id]
        df = pd.read_sql_query(query, conn, params=params)

        if df.empty:
            return df

        df["ClientId"] = df["ClientId"].astype(str).str.strip()
        df["Periodo"] = df["Periodo"].astype(str).str.strip()
        df["Media"] = pd.to_numeric(df["Media"], errors="coerce").fillna(0).astype(float)
        df["Dias"] = pd.to_numeric(df["Dias"], errors="coerce").fillna(0).astype(int)

        return df
    finally:
        conn.close()