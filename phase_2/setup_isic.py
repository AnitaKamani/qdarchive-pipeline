"""
Phase 2 one-command setup: apply migration + import ISIC divisions.

Usage:
    python phase_2/setup_isic.py [--db PATH] [--xlsx PATH]

Steps:
    1. Apply phase2_migration.sql (idempotent).
    2. Locate the ISIC Excel file and import the Divisions sheet.
    3. Verify isic_divisions has rows and projects.project_type exists.
    4. Print the first 10 imported divisions.
    5. Print a PASS/FAIL summary.
"""

import argparse
import sqlite3
import sys
from pathlib import Path

# Allow running as `python phase_2/setup_isic.py` from the repo root.
_here = Path(__file__).parent
if str(_here) not in sys.path:
    sys.path.insert(0, str(_here))

from apply_migration import apply as run_migration, column_exists, SQL_DEFAULT, DB_DEFAULT
from import_isic_divisions import find_xlsx, import_divisions, row_count


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 2 full ISIC setup.")
    parser.add_argument("--db", default=DB_DEFAULT, help="Path to SQLite database")
    parser.add_argument("--xlsx", default=None, help="Path to ISIC Excel file (auto-detected if omitted)")
    args = parser.parse_args()

    results: dict[str, str] = {}

    print("=" * 62)
    print("Phase 2 ISIC Setup")
    print("=" * 62)

    # ── Step 1: Migration ────────────────────────────────────────────────────
    print("\n[1/3] Applying database migration...")
    try:
        run_migration(args.db, SQL_DEFAULT, dry_run=False)
        results["migration"] = "PASS"
    except RuntimeError as exc:
        print(f"  ERROR: {exc}", file=sys.stderr)
        results["migration"] = "FAIL"
        _print_summary(results)
        sys.exit(1)

    # ── Step 2: Import ISIC divisions ────────────────────────────────────────
    print("\n[2/3] Importing ISIC divisions...")
    try:
        xlsx_path = find_xlsx(args.xlsx)
        print(f"  Excel file : {xlsx_path}")
        n_imported = import_divisions(args.db, xlsx_path)
        print(f"  Imported   : {n_imported} rows")
        results["isic_import"] = "PASS"
    except (FileNotFoundError, RuntimeError) as exc:
        print(f"  ERROR: {exc}", file=sys.stderr)
        results["isic_import"] = "FAIL"
        _print_summary(results)
        sys.exit(1)

    # ── Step 3: Verify ───────────────────────────────────────────────────────
    print("\n[3/3] Verifying...")
    conn = sqlite3.connect(args.db)
    try:
        total = row_count(args.db)
        has_col = column_exists(conn, "projects", "project_type")
        first10 = conn.execute(
            "SELECT code, section_code, division, title "
            "FROM isic_divisions ORDER BY code LIMIT 10"
        ).fetchall()
    finally:
        conn.close()

    results["isic_rows"]            = "PASS" if total > 0  else "FAIL"
    results["project_type_column"]  = "PASS" if has_col    else "FAIL"

    print(f"  isic_divisions row count : {total}")
    print(f"  projects.project_type    : {'EXISTS' if has_col else 'MISSING'}")

    print()
    print("  First 10 ISIC divisions:")
    print(f"  {'Code':<8} {'Section':<9} {'Div':<5} Title")
    print("  " + "-" * 68)
    for code, section, division, title in first10:
        print(f"  {code:<8} {str(section):<9} {str(division):<5} {title}")

    _print_summary(results)

    if any(v == "FAIL" for v in results.values()):
        sys.exit(1)


def _print_summary(results: dict[str, str]) -> None:
    print()
    print("=" * 62)
    print("Summary")
    print("=" * 62)
    for key, status in results.items():
        mark = "PASS" if status == "PASS" else "FAIL"
        print(f"  [{mark}] {key}")
    overall = "PASS" if all(v == "PASS" for v in results.values()) else "FAIL"
    print("-" * 62)
    print(f"  Overall: {overall}")
    print("=" * 62)


if __name__ == "__main__":
    main()
