"""
Validate the ISIC classification results in project_classifications.

Checks:
  1. project_classifications has rows
  2. All primary_class_code values exist in isic_divisions
  3. All classified project_id values exist in combined_projects
  4. Count breakdown by primary_class_code

Usage:
    python phase_2/check_isic_classification.py [--db PATH]
"""

import csv
import sqlite3
import sys
from pathlib import Path

DB_DEFAULT = "23727550-sq26-combined.db"
VALIDATION_REPORT = "reports/isic_classification_validation.csv"


def run_checks(db_path: str) -> list[dict]:
    conn = sqlite3.connect(db_path)
    checks: list[dict] = []

    def add(name: str, passed: bool, detail: str = "") -> None:
        checks.append({"check": name, "status": "PASS" if passed else "FAIL", "detail": detail})

    # 1. project_classifications has rows
    total = conn.execute("SELECT COUNT(*) FROM project_classifications").fetchone()[0]
    add("project_classifications has rows", total > 0, f"{total:,} rows")

    # 2. All primary_class_code values in isic_divisions (exclude model_error rows where code is NULL)
    invalid_codes = conn.execute(
        "SELECT COUNT(*) FROM project_classifications "
        "WHERE primary_class_code IS NOT NULL "
        "AND primary_class_code NOT IN (SELECT code FROM isic_divisions)"
    ).fetchone()[0]
    add(
        "all primary_class_codes in isic_divisions",
        invalid_codes == 0,
        f"{invalid_codes} invalid codes",
    )

    # 3. All project_id values exist in combined_projects
    orphan_pids = conn.execute(
        "SELECT COUNT(*) FROM project_classifications pc "
        "WHERE NOT EXISTS ("
        "  SELECT 1 FROM combined_projects cp WHERE cp.global_project_id = pc.project_id"
        ")"
    ).fetchone()[0]
    add(
        "all project_ids in combined_projects",
        orphan_pids == 0,
        f"{orphan_pids} orphan project_ids",
    )

    # 4. Code breakdown (informational — always PASS)
    code_rows = conn.execute(
        "SELECT pc.primary_class_code, id.title, COUNT(*) AS cnt "
        "FROM project_classifications pc "
        "LEFT JOIN isic_divisions id ON id.code = pc.primary_class_code "
        "WHERE pc.primary_class_code IS NOT NULL "
        "GROUP BY pc.primary_class_code ORDER BY cnt DESC LIMIT 10"
    ).fetchall()
    top = "; ".join(f"{r[0]}({r[2]})" for r in code_rows[:5])
    add("top primary_class_codes (informational)", True, top or "no classified rows")

    conn.close()
    return checks


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Validate ISIC classification results.")
    parser.add_argument("--db", default=DB_DEFAULT)
    args = parser.parse_args()

    print("Validating ISIC classification...")
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
