"""
Export a random manual evaluation sample from classified projects.

Usage:
    python phase_2/export_evaluation_sample.py [--db PATH] [--method METHOD]
        [--limit N] [--output PATH]
"""

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

DB_DEFAULT = "23727550-sq26-combined.db"
METHOD_DEFAULT = "openai:gpt-4o-mini"
LIMIT_DEFAULT = 100
OUTPUT_DEFAULT = "reports/manual_evaluation_sample.csv"

FIELDS = [
    "project_id",
    "source_student_id",
    "project_type",
    "project_title",
    "project_description_short",
    "predicted_class_code",
    "predicted_class_title",
    "confidence",
    "reason",
    "manual_label_correct",
    "manual_notes",
]

_SQL = """\
SELECT
    pc.project_id,
    cp.source_student_id,
    cp.project_type,
    cp.title,
    SUBSTR(COALESCE(cp.description, ''), 1, 300),
    pc.primary_class_code,
    id.title,
    pc.confidence,
    pc.reason
FROM project_classifications pc
JOIN combined_projects cp ON cp.global_project_id = pc.project_id
LEFT JOIN isic_divisions id ON id.code = pc.primary_class_code
WHERE pc.method = ?
ORDER BY RANDOM()
LIMIT ?
"""


def export(db_path: str, method: str, limit: int, output: str) -> int:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)

    total_classified = conn.execute(
        "SELECT COUNT(*) FROM project_classifications WHERE method = ?", (method,)
    ).fetchone()[0]

    if total_classified == 0:
        print(f"ERROR: no rows found for method='{method}'.", file=sys.stderr)
        print(f"  Available methods:", file=sys.stderr)
        for (m,) in conn.execute(
            "SELECT DISTINCT method FROM project_classifications ORDER BY method"
        ):
            print(f"    {m}", file=sys.stderr)
        conn.close()
        return 0

    rows = conn.execute(_SQL, (method, limit)).fetchall()
    conn.close()

    Path(output).parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow({
                "project_id": r[0],
                "source_student_id": r[1],
                "project_type": r[2],
                "project_title": (r[3] or "")[:200],
                "project_description_short": (r[4] or "").strip(),
                "predicted_class_code": r[5],
                "predicted_class_title": r[6] or "",
                "confidence": r[7],
                "reason": r[8] or "",
                "manual_label_correct": "",
                "manual_notes": "",
            })

    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Export manual evaluation sample.")
    parser.add_argument("--db", default=DB_DEFAULT)
    parser.add_argument("--method", default=METHOD_DEFAULT)
    parser.add_argument("--limit", type=int, default=LIMIT_DEFAULT)
    parser.add_argument("--output", default=OUTPUT_DEFAULT)
    args = parser.parse_args()

    n = export(args.db, args.method, args.limit, args.output)
    if n == 0:
        sys.exit(1)

    print(f"Exported {n} rows  →  {args.output}")
    print(f"  method : {args.method}")
    print(f"  fill in manual_label_correct (yes/no) and manual_notes, then re-import for evaluation.")


if __name__ == "__main__":
    main()
