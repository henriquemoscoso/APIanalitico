from __future__ import annotations

import argparse
from datetime import date
import pandas as pd

# ajuste o import conforme o seu layout
from database_connection.load_tables import table_load_access_events_by_device


def main() -> None:
    parser = argparse.ArgumentParser(description="Exporta para CSV o resultado bruto do SQL (por DeviceId).")
    parser.add_argument("--device-id", required=True, help="DeviceId (ex.: 00132B76FE186599)")
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD (data local GMT-3)")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD (data local GMT-3)")
    parser.add_argument("--out", default=None, help="Caminho do CSV de saída")
    parser.add_argument("--sep", default=",", help="Separador CSV (use ';' pro Excel pt-BR)")
    args = parser.parse_args()

    device_id = args.device_id.strip()
    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)

    df = table_load_access_events_by_device(
        device_id=device_id,
        start_date=start_date,
        end_date=end_date,
    )

    # Ordena para facilitar comparação com SQL/Excel
    sort_cols = [c for c in ["DataUTC", "DataLocal", "PersonId"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols)

    out_path = args.out or f"access_events_{device_id}_{start_date.isoformat()}_{end_date.isoformat()}.csv"

    df.to_csv(out_path, index=False, sep=args.sep)
    print(f"[OK] CSV gerado: {out_path}  (linhas: {len(df)})")


if __name__ == "__main__":
    main()