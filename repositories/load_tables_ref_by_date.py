from datetime import datetime
from typing import Optional

import pandas as pd

from repositories.db import get_conn


def load_ref_by_date(
    *,
    device_client_id: Optional[str] = None,
    start_date: datetime,
    end_date: datetime,
    is_unique: int,
    event_type_id: int = 16777985
) -> pd.DataFrame:

    if end_date <= start_date:
        raise ValueError("end_date deve ser maior que start_date.")

    conn = get_conn()
    try:
        query = "select * from [Arthos.Events].dbo.fnGetRefeitorioByDate(?, ?, ?, ?, ?)"

        params = [
            start_date,
            end_date,
            device_client_id,
            is_unique,
            event_type_id,
        ]

        df = pd.read_sql_query(query, conn, params=params)

        if df.empty:
            return df

        return df

    finally:
        conn.close()
