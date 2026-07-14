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


def run_checks(db_path: str, method: str | None = None) -> list[dict]:
    conn = sqlite3.connect(db_path)
    checks: list[dict] = []

    def add(name: str, passed: bool, detail: str = "") -> None:
        checks.append({"check": name, "status": "PASS" if passed else "FAIL", "detail": detail})

    method_filter = " AND method = ?" if method else ""
    method_params = [method] if method else []

    # 1. classified rows exist (for the given method, if any)
    total = conn.execute(
        f"SELECT COUNT(*) FROM project_classifications WHERE 1=1{method_filter}",
        method_params,
    ).fetchone()[0]
    label = f"project_classifications has rows (method={method})" if method else "project_classifications has rows"
    add(label, total > 0, f"{total:,} rows")

    # 2. All primary_class_code values in isic_divisions (exclude model_error rows where code is NULL)
    invalid_codes = conn.execute(
        "SELECT COUNT(*) FROM project_classifications "
        f"WHERE primary_class_code IS NOT NULL{method_filter} "
        "AND primary_class_code NOT IN (SELECT code FROM isic_divisions)",
        method_params,
    ).fetchone()[0]
    add(
        "all primary_class_codes in isic_divisions",
        invalid_codes == 0,
        f"{invalid_codes} invalid codes",
    )

    # 3. All project_id values exist in combined_projects
    orphan_pids = conn.execute(
        "SELECT COUNT(*) FROM project_classifications pc "
        f"WHERE 1=1{method_filter} AND NOT EXISTS ("
        "  SELECT 1 FROM combined_projects cp WHERE cp.global_project_id = pc.project_id"
        ")",
        method_params,
    ).fetchone()[0]
    add(
        "all project_ids in combined_projects",
        orphan_pids == 0,
        f"{orphan_pids} orphan project_ids",
    )

    # 4. No NULL primary_class_code among rows classified by this method
    # (model_error rows use a different method value, so this checks only successful rows)
    if method:
        null_primary = conn.execute(
            "SELECT COUNT(*) FROM project_classifications "
            "WHERE method = ? AND primary_class_code IS NULL",
            [method],
        ).fetchone()[0]
        add(
            f"no NULL primary_class_code for method={method}",
            null_primary == 0,
            f"{null_primary} NULL rows",
        )

    # 5. model_error rows reported separately (not treated as successful classifications)
    model_error_count = conn.execute(
        "SELECT COUNT(*) FROM project_classifications WHERE method = 'model_error'"
    ).fetchone()[0]
    add("model_error rows (informational, not successful)", True, f"{model_error_count:,} rows")

    # 6. remaining unclassified PROJECT inputs for this method
    if method:
        remaining = conn.execute(
            "SELECT COUNT(*) FROM classification_inputs ci "
            "WHERE ci.target_type = 'PROJECT' AND NOT EXISTS ("
            "  SELECT 1 FROM project_classifications pc "
            "  WHERE pc.project_id = COALESCE(ci.project_id, ci.target_id) AND pc.method = ?"
            ")",
            [method],
        ).fetchone()[0]
        add(f"remaining unclassified PROJECT inputs (method={method})", True, f"{remaining:,} remaining")

    # 7. Code breakdown (informational — always PASS)
    code_rows = conn.execute(
        "SELECT pc.primary_class_code, id.title, COUNT(*) AS cnt "
        "FROM project_classifications pc "
        "LEFT JOIN isic_divisions id ON id.code = pc.primary_class_code "
        f"WHERE pc.primary_class_code IS NOT NULL{method_filter} "
        "GROUP BY pc.primary_class_code ORDER BY cnt DESC LIMIT 10",
        method_params,
    ).fetchall()
    top = "; ".join(f"{r[0]}({r[2]})" for r in code_rows[:5])
    add("top primary_class_codes (informational)", True, top or "no classified rows")

    conn.close()
    return checks


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Validate ISIC classification results.")
    parser.add_argument("--db", default=DB_DEFAULT)
    parser.add_argument("--method", default=None, help="Restrict checks to a single method, e.g. openai:gpt-4o-mini")
    args = parser.parse_args()

    print("Validating ISIC classification..." + (f" (method={args.method})" if args.method else ""))
    checks = run_checks(args.db, method=args.method)

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
