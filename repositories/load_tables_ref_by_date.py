# %%
# import os

# os.chdir(r"C:\Users\henri\OneDrive\RMtech\APIanalitico")
# %% 
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
        query = "select * from [Arthos.Events].dbo.fnGetRefeitorioByDate(?, ?, ?, ?)"

        params = [
            start_date,
            end_date,
            device_client_id,
            is_unique
        ]

        df = pd.read_sql_query(query, conn, params=params)

        if df.empty:
            return df

        df = df.copy()

        # df["TipoAcesso"] = (
        #     df["TipoAcesso"]
        #     .astype("string")
        #     .fillna("Refeitorio")
        #     .str.strip()
        #     .replace("", "Refeitorio")
        # )

        # df["TipoRefeicao"] = (
        #     df["TipoRefeicao"]
        #     .astype("string")
        #     .fillna("outofscope")
        #     .str.strip()
        #     .replace("", "outofscope")
        # )

        # if "DeviceClientId" in df.columns:
        #     df["DeviceClientId"] = (
        #         df["DeviceClientId"]
        #         .astype("string")
        #         .fillna("UNKNOWN")
        #         .str.strip()
        #         .replace("", "UNKNOWN")
        #     )

        return df

    finally:
        conn.close()
# # %%
# df = table_load_access_events_by_client(start_date="01/02/2026",end_date="02/02/2026",
#                                         is_unique=0)
# df
# %%
