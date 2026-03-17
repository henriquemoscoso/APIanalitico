from datetime import datetime
from typing import Optional

import pandas as pd

from repositories.db import get_conn


def load_ref_by_hour(
    *,
    device_client_id: Optional[str] = None,
    start_date: datetime | str,
    end_date: datetime | str,
    is_unique: int,
    event_type_id: int = 16777985,
) -> pd.DataFrame:
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%d/%m/%Y")

    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%d/%m/%Y")

    if end_date <= start_date:
        raise ValueError("end_date deve ser maior que start_date.")

    conn = get_conn()
    try:
        query = """
        SELECT *
        FROM [Arthos.Events].dbo.fnGetRefeitorioByHour(?, ?, ?, ?)
        """

        params = [
            start_date,
            end_date,
            device_client_id,
            is_unique,
        ]

        df = pd.read_sql_query(query, conn, params=params)

        if df.empty:
            return df

        return df

    finally:
        conn.close()