"""
Import ISIC division codes from an Excel file into the isic_divisions table.

Usage:
    python phase_2/import_isic_divisions.py [--db PATH] [--xlsx PATH]

If --xlsx is omitted the script searches for *ISIC*.xlsx in:
    1. data/reference/
    2. repo root
    3. ~/Downloads/

The 'Divisions' sheet is expected to have no header row with columns:
    col 0: ISIC code  (e.g. 'A01', 'B05', 'C10')
    col 1: division number (numeric, derived from code)
    col 2: division title

Only division-level codes (one letter + two digits, e.g. A01) are imported.
Section headers (A, B …) and group/class codes (A011, A0111 …) are skipped.
"""

import argparse
import re
import sqlite3
import sys
from pathlib import Path

import pandas as pd


DB_DEFAULT = "23727550-sq26.db"
SHEET_NAME = "Divisions"
_DIVISION_RE = re.compile(r"^[A-Z]\d{2}$")

_SEARCH_DIRS = [
    Path("data/reference"),
    Path("."),
    Path.home() / "Downloads",
]


def find_xlsx(hint: str | None = None) -> Path:
    if hint:
        p = Path(hint)
        if p.exists():
            return p
        raise FileNotFoundError(f"Specified Excel file not found: {hint}")

    for directory in _SEARCH_DIRS:
        if not directory.is_dir():
            continue
        matches = sorted(directory.glob("*ISIC*.xlsx")) + sorted(
            directory.glob("*isic*.xlsx")
        )
        if matches:
            return matches[0]

    raise FileNotFoundError(
        "ISIC Excel file not found. Searched:\n"
        + "\n".join(f"  {d}" for d in _SEARCH_DIRS)
        + "\nPlace the file in data/reference/ or pass --xlsx PATH."
    )


def import_divisions(db_path: str, xlsx_path: Path) -> int:
    try:
        df = pd.read_excel(xlsx_path, sheet_name=SHEET_NAME, header=None, dtype=str)
    except Exception as exc:
        raise RuntimeError(
            f"Cannot read sheet '{SHEET_NAME}' from {xlsx_path}: {exc}"
        ) from exc

    rows: list[tuple] = []
    for _, row in df.iterrows():
        code = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ""
        if not _DIVISION_RE.match(code):
            continue

        title_raw = row.iloc[2] if len(row) > 2 else None
        title = str(title_raw).strip() if pd.notna(title_raw) else ""
        if not title:
            continue

        section_code = code[0]
        division = int(code[1:])

        rows.append((code, section_code, division, title, None))

    if not rows:
        raise RuntimeError(
            f"No division-level rows (pattern [A-Z]{{2 digits}}) found "
            f"in sheet '{SHEET_NAME}' of {xlsx_path}"
        )

    conn = sqlite3.connect(db_path)
    try:
        conn.executemany(
            "INSERT OR REPLACE INTO isic_divisions "
            "(code, section_code, division, title, description) "
            "VALUES (?, ?, ?, ?, ?)",
            rows,
        )
        conn.commit()
    finally:
        conn.close()

    return len(rows)


def row_count(db_path: str) -> int:
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute("SELECT COUNT(*) FROM isic_divisions").fetchone()[0]
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import ISIC divisions from Excel into SQLite."
    )
    parser.add_argument("--db", default=DB_DEFAULT, help="Path to SQLite database")
    parser.add_argument("--xlsx", default=None, help="Path to ISIC Excel file")
    args = parser.parse_args()

    try:
        xlsx_path = find_xlsx(args.xlsx)
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    print(f"  Excel file : {xlsx_path}")

    try:
        n = import_divisions(args.db, xlsx_path)
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    total = row_count(args.db)
    print(f"  Imported   : {n} rows")
    print(f"  Total in DB: {total} rows")


if __name__ == "__main__":
    main()
