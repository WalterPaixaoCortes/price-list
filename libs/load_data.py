"""Load a CSV file into the SQLite database ../app.db into table `prices`.

Usage:
        python load_data.py --csv data.csv --db ../app.db --table prices

Notes:
- The script will use pandas to read the CSV and SQLAlchemy to write to the DB.
- If the `prices` table does not exist it will be created from the CSV headers/types.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

import pandas as pd

try:
    from sqlalchemy import create_engine
except Exception:
    create_engine = None


def main(argv=None):
    p = argparse.ArgumentParser(description="Load CSV into SQLite DB table")
    p.add_argument("--csv", required=True, help="Path to CSV file to load")
    p.add_argument("--db", default="../app.db", help="SQLite DB file path")
    p.add_argument("--table", default="prices", help="Target table name")
    p.add_argument(
        "--if-exists", choices=("fail", "replace", "append"), default="append"
    )
    args = p.parse_args(argv)

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"CSV file not found: {csv_path}")
        return 2

    if create_engine is None:
        print("sqlalchemy is required. Install with: pip install sqlalchemy")
        return 3

    try:
        df = pd.read_csv(csv_path)
    except Exception as exc:
        print(f"Failed to read CSV: {exc}")
        return 4

    db_uri = f"sqlite:///{Path(args.db).resolve()}"
    engine = create_engine(db_uri)

    try:
        df.to_sql(args.table, con=engine, if_exists=args.if_exists, index=False)
    except Exception as exc:
        print(f"Failed to write to DB: {exc}")
        return 5

    print(f"Wrote {len(df)} rows to table '{args.table}' in database {args.db}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
