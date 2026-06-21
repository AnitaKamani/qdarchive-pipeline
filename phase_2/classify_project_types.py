"""
Classify combined_projects.project_type using file extension rules.

Rules (priority order):
  QDA_PROJECT   — any related file has a QDA tool extension
  QD_PROJECT    — any related file has a qualitative data extension
  OTHER_PROJECT — has files but none match the above
  NOT_A_PROJECT — no files or no useful file information

Usage:
    python phase_2/classify_project_types.py [--db PATH] [--dry-run]
"""

import csv
import sqlite3
from pathlib import Path

DB_DEFAULT = "23727550-sq26-combined.db"
SUMMARY_REPORT = "reports/project_type_summary.csv"
BY_STUDENT_REPORT = "reports/project_type_by_student.csv"
EXAMPLES_REPORT = "reports/project_type_examples.csv"

# file_name LIKE patterns (matched with the leading dot)
_QDA_FNAME = (
    ".qdpx", ".qdpx.zip", ".qda", ".rqda",
    ".nvp", ".nvivo", ".mx20", ".mx22", ".atlproj", ".atlasti",
)
# file_type exact values (bare extension, no dot)
_QDA_FTYPE = (
    "qdpx", "qda", "rqda", "nvp", "nvivo", "mx20", "mx22", "atlproj", "atlasti",
)

_QD_FNAME = (
    ".txt", ".pdf", ".rtf", ".doc", ".docx", ".odt",
    ".csv", ".tsv", ".xls", ".xlsx",
    ".mp3", ".wav", ".m4a", ".aac",
    ".mp4", ".mov", ".avi", ".mkv", ".webm",
    ".xml", ".json",
)
_QD_FTYPE = (
    "txt", "pdf", "rtf", "doc", "docx", "odt",
    "csv", "tsv", "xls", "xlsx",
    "mp3", "wav", "m4a", "aac",
    "mp4", "mov", "avi", "mkv", "webm",
    "xml", "json",
)


def _ext_sql(fname_exts: tuple, ftype_vals: tuple) -> str:
    fname_parts = " OR ".join(f"LOWER(cf.file_name) LIKE '%{e}'" for e in fname_exts)
    ftype_in = ", ".join(f"'{v}'" for v in ftype_vals)
    return f"({fname_parts} OR LOWER(cf.file_type) IN ({ftype_in}))"


_QDA_CHECK = _ext_sql(_QDA_FNAME, _QDA_FTYPE)
_QD_CHECK = _ext_sql(_QD_FNAME, _QD_FTYPE)

_CLASSIFY_SQL = f"""
WITH file_flags AS (
    SELECT
        global_project_id,
        MAX(CASE WHEN {_QDA_CHECK} THEN 1 ELSE 0 END) AS has_qda,
        MAX(CASE WHEN {_QD_CHECK}  THEN 1 ELSE 0 END) AS has_qd,
        COUNT(*) AS file_count
    FROM combined_files cf
    GROUP BY global_project_id
)
SELECT
    cp.global_project_id,
    COALESCE(
        CASE
            WHEN ff.has_qda = 1      THEN 'QDA_PROJECT'
            WHEN ff.has_qd  = 1      THEN 'QD_PROJECT'
            WHEN ff.file_count > 0   THEN 'OTHER_PROJECT'
            ELSE                          'NOT_A_PROJECT'
        END,
        'NOT_A_PROJECT'
    ) AS project_type
FROM combined_projects cp
LEFT JOIN file_flags ff ON ff.global_project_id = cp.global_project_id
"""


def classify_all(db_path: str, dry_run: bool = False) -> dict[str, int]:
    """Classify all combined_projects by file extension rules. Returns {type: count}."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode = WAL")

    print("  Running file extension analysis...", flush=True)
    rows = conn.execute(_CLASSIFY_SQL).fetchall()  # [(global_project_id, project_type)]

    counts: dict[str, int] = {}
    for _, pt in rows:
        counts[pt] = counts.get(pt, 0) + 1

    if not dry_run:
        print(f"  Updating {len(rows):,} project_type values...", flush=True)
        conn.executemany(
            "UPDATE combined_projects SET project_type = ? WHERE global_project_id = ?",
            [(pt, gid) for gid, pt in rows],
        )
        conn.commit()
        _write_reports(conn)

    conn.close()
    return counts


def _write_reports(conn: sqlite3.Connection) -> None:
    Path("reports").mkdir(parents=True, exist_ok=True)

    rows = conn.execute(
        "SELECT project_type, COUNT(*) FROM combined_projects "
        "GROUP BY project_type ORDER BY COUNT(*) DESC"
    ).fetchall()
    _csv(
        SUMMARY_REPORT,
        ["project_type", "project_count"],
        [{"project_type": r[0], "project_count": r[1]} for r in rows],
    )

    rows = conn.execute(
        "SELECT source_student_id, project_type, COUNT(*) "
        "FROM combined_projects GROUP BY source_student_id, project_type "
        "ORDER BY source_student_id, project_type"
    ).fetchall()
    _csv(
        BY_STUDENT_REPORT,
        ["source_student_id", "project_type", "project_count"],
        [{"source_student_id": r[0], "project_type": r[1], "project_count": r[2]} for r in rows],
    )

    example_rows = []
    for pt in ("QDA_PROJECT", "QD_PROJECT", "OTHER_PROJECT", "NOT_A_PROJECT"):
        projs = conn.execute(
            "SELECT global_project_id, source_student_id, title "
            "FROM combined_projects WHERE project_type = ? LIMIT 3",
            (pt,),
        ).fetchall()
        for gid, sid, title in projs:
            files = conn.execute(
                "SELECT file_name FROM combined_files "
                "WHERE global_project_id = ? AND file_name IS NOT NULL LIMIT 5",
                (gid,),
            ).fetchall()
            example_rows.append({
                "project_type": pt,
                "global_project_id": gid,
                "source_student_id": sid,
                "title": (title or "")[:100],
                "example_files": "; ".join(r[0] for r in files),
            })
    _csv(
        EXAMPLES_REPORT,
        ["project_type", "global_project_id", "source_student_id", "title", "example_files"],
        example_rows,
    )


def _csv(path: str, fields: list, rows: list) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Classify combined_projects by project type.")
    parser.add_argument("--db", default=DB_DEFAULT)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print("Classifying project types...")
    counts = classify_all(args.db, args.dry_run)

    print("\nProject type counts:")
    for pt in ("QDA_PROJECT", "QD_PROJECT", "OTHER_PROJECT", "NOT_A_PROJECT"):
        print(f"  {pt:<20} : {counts.get(pt, 0):,}")

    if not args.dry_run:
        print(f"\nReports written:")
        print(f"  {SUMMARY_REPORT}")
        print(f"  {BY_STUDENT_REPORT}")
        print(f"  {EXAMPLES_REPORT}")


if __name__ == "__main__":
    main()
