"""
Validate that the combined database is ready for ISIC classification.

Checks:
  1. combined_projects.project_type is filled for all rows
  2. isic_divisions has exactly 87 rows
  3. classification_inputs has PROJECT rows
  4. classification_inputs target_ids match combined_projects.global_project_id
  5. Required report files exist on disk

Usage:
    python phase_2/check_classification_preparation.py [--db PATH]
"""

import csv
import sqlite3
import sys
from pathlib import Path

DB_DEFAULT = "23727550-sq26-combined.db"
VALIDATION_REPORT = "reports/classification_preparation_validation.csv"
EXPECTED_ISIC_ROWS = 87

REQUIRED_REPORTS = [
    "reports/project_type_summary.csv",
    "reports/project_type_by_student.csv",
    "reports/project_type_examples.csv",
    "reports/merge_report.csv",
]


def run_checks(db_path: str) -> list[dict]:
    conn = sqlite3.connect(db_path)
    checks: list[dict] = []

    def add(name: str, passed: bool, detail: str = "") -> None:
        checks.append({"check": name, "status": "PASS" if passed else "FAIL", "detail": detail})

    # 1. project_type filled
    null_count = conn.execute(
        "SELECT COUNT(*) FROM combined_projects WHERE project_type IS NULL"
    ).fetchone()[0]
    total = conn.execute("SELECT COUNT(*) FROM combined_projects").fetchone()[0]
    add(
        "project_type filled for all rows",
        null_count == 0,
        f"{total - null_count:,}/{total:,} rows classified",
    )

    # 2. isic_divisions count
    isic_count = conn.execute("SELECT COUNT(*) FROM isic_divisions").fetchone()[0]
    add(
        "isic_divisions row count",
        isic_count == EXPECTED_ISIC_ROWS,
        f"{isic_count} rows (expected {EXPECTED_ISIC_ROWS})",
    )

    # 3. classification_inputs has PROJECT rows
    proj_inputs = conn.execute(
        "SELECT COUNT(*) FROM classification_inputs WHERE target_type = 'PROJECT'"
    ).fetchone()[0]
    add(
        "classification_inputs has PROJECT rows",
        proj_inputs > 0,
        f"{proj_inputs:,} PROJECT inputs",
    )

    # 4. target_ids match combined_projects
    orphans = conn.execute(
        "SELECT COUNT(*) FROM classification_inputs ci "
        "WHERE ci.target_type = 'PROJECT' AND NOT EXISTS ("
        "  SELECT 1 FROM combined_projects cp "
        "  WHERE cp.global_project_id = ci.target_id"
        ")"
    ).fetchone()[0]
    add(
        "PROJECT target_ids match combined_projects",
        orphans == 0,
        f"{orphans} orphan inputs",
    )

    conn.close()

    # 5. Required reports on disk
    for path in REQUIRED_REPORTS:
        add(f"report exists: {path}", Path(path).exists())

    return checks


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Validate classification preparation.")
    parser.add_argument("--db", default=DB_DEFAULT)
    args = parser.parse_args()

    print("Validating classification preparation...")
    checks = run_checks(args.db)

    Path("reports").mkdir(parents=True, exist_ok=True)
    with open(VALIDATION_REPORT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["check", "status", "detail"])
        w.writeheader()
        w.writerows(checks)

    print("\nValidation results:")
    all_pass = True
    for c in checks:
        icon = "PASS" if c["status"] == "PASS" else "FAIL"
        detail = f" ({c['detail']})" if c["detail"] else ""
        print(f"  [{icon}] {c['check']}{detail}")
        if c["status"] != "PASS":
            all_pass = False

    print()
    print(f"  {'PASS — all checks passed.' if all_pass else 'FAIL — see above.'}")
    print(f"  Report: {VALIDATION_REPORT}")

    if not all_pass:
        sys.exit(1)


if __name__ == "__main__":
    main()
