#!/usr/bin/env python3
"""Generate JSON schemas for tabular files in a directory.

Scans recursively for CSV and Excel files and writes a JSON array with
objects of the form:

  {
    "filename": "path/relative/to/root/file.csv",
    "columns": [
      {"columnname": "Col A", "dfcolumnname": ""},
      ...
    ]
  }

Usage:
  python generate_schemas.py --path . --output schemas.json

Requires: pandas, openpyxl (for Excel files).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import List, Optional

import typer


def get_columns(path: Path) -> Optional[List[str]]:
    """Return list of column names for supported file types, or None if unsupported."""
    try:
        import pandas as pd
    except Exception as exc:  # pragma: no cover - helpful runtime message
        raise RuntimeError(
            "Missing dependency: pandas. Install with 'pip install pandas openpyxl'"
        ) from exc

    suffix = path.suffix.lower()
    try:
        if suffix == ".csv":
            df = pd.read_csv(path, nrows=0)
        elif suffix in (".xls", ".xlsx"):
            df = pd.read_excel(path, nrows=0)
        else:
            return None
    except Exception:
        # If a file can't be parsed, skip it gracefully
        return []

    return list(df.columns)


def scan_dir(root: Path, include_empty: bool = False) -> List[dict]:
    results = []
    for p in sorted(root.rglob("*")):
        if not p.is_file():
            continue

        cols = get_columns(p)
        if cols is None:
            continue

        if not cols and not include_empty:
            continue

        rel = str(p.relative_to(root))
        results.append(
            {
                "filename": rel,
                "columns": [{"columnname": c, "dfcolumnname": ""} for c in cols],
            }
        )

    return results


app = typer.Typer()


@app.command()
def main(
    path: str = typer.Option(".", "-p", "--path", help="Root directory to scan"),
    output: str = typer.Option(
        "schemas.json", "-o", "--output", help="Output JSON file path"
    ),
    include_empty: bool = typer.Option(
        False, "--include-empty", help="Include files with no detected columns"
    ),
) -> None:
    """Generate JSON file describing columns of CSV/Excel files in a folder."""
    root = Path(path).resolve()
    if not root.exists() or not root.is_dir():
        typer.echo(f"Provided path does not exist or is not a directory: {root}")
        raise typer.Exit(code=2)

    try:
        results = scan_dir(root, include_empty=include_empty)
    except RuntimeError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=3)

    out_path = Path(output)
    out_path.write_text(json.dumps(results, indent=2, ensure_ascii=False))
    typer.echo(f"Wrote {len(results)} entries to {out_path}")


if __name__ == "__main__":
    app()
