"""
Inspect student metadata SQLite databases and write a schema report.

Usage:
    python phase_2/inspect_student_dbs.py [--in-dir PATH] [--report PATH]

For each .db file in in-dir:
    - Validates SQLite magic bytes.
    - Lists tables and row counts.
    - Flags presence of: projects, files, keywords, licenses, person_role.
"""

import csv
import sqlite3
import sys
from pathlib import Path

IN_DIR_DEFAULT = "data/student_metadata"
REPORT_DEFAULT = "reports/student_db_schema_report.csv"
SQLITE_MAGIC = b"SQLite format 3"
IMPORTANT = {"projects", "files", "keywords", "licenses", "person_role"}


def _check_magic(db_path: Path) -> bool:
    try:
        with open(db_path, "rb") as f:
            return f.read(len(SQLITE_MAGIC)) == SQLITE_MAGIC
    except OSError:
        return False


def inspect_db(db_path: Path) -> dict:
    if not _check_magic(db_path):
        return dict(
            is_valid_sqlite=False,
            has_projects=False,
            has_files=False,
            project_count=0,
            file_count=0,
            tables="",
            error="not a SQLite file",
        )

    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = None
        tables = [
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
        ]
        # Normalize to lowercase for comparison; SQLite queries are case-insensitive.
        table_lower = {t.lower() for t in tables}
        has_projects = "projects" in table_lower
        has_files = "files" in table_lower

        n_proj = (
            conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
            if has_projects else 0
        )
        n_files = (
            conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
            if has_files else 0
        )
        conn.close()

        return dict(
            is_valid_sqlite=True,
            has_projects=has_projects,
            has_files=has_files,
            project_count=n_proj,
            file_count=n_files,
            tables=";".join(tables),
            error="",
        )
    except Exception as exc:
        return dict(
            is_valid_sqlite=True,
            has_projects=False,
            has_files=False,
            project_count=0,
            file_count=0,
            tables="",
            error=str(exc)[:200],
        )


def inspect_all(
    in_dir: str = IN_DIR_DEFAULT,
    report_path: str = REPORT_DEFAULT,
) -> tuple[int, int, list[Path]]:
    """
    Inspect all .db files in in_dir.
    Returns (n_valid, n_invalid, list_of_valid_db_paths).
    A DB is 'valid' if it is SQLite and has a projects table.
    """
    Path(report_path).parent.mkdir(parents=True, exist_ok=True)
    db_files = sorted(Path(in_dir).glob("*.db"))

    if not db_files:
        print(f"  No .db files found in {in_dir}")
        return 0, 0, []

    report_rows: list[dict] = []
    n_valid = n_invalid = 0
    valid_paths: list[Path] = []

    for db_path in db_files:
        student_id = db_path.stem
        result = inspect_db(db_path)

        ok = result["is_valid_sqlite"] and result["has_projects"]
        if ok:
            n_valid += 1
            valid_paths.append(db_path)
            tag = "OK"
        else:
            n_invalid += 1
            tag = "INVALID"

        label = (
            f"projects={result['project_count']}, files={result['file_count']}, "
            f"tables=[{result['tables']}]"
        ) if result["is_valid_sqlite"] else result["error"]
        print(f"  [{tag}] {student_id}: {label}")

        report_rows.append(dict(
            student_id=student_id,
            db_path=str(db_path),
            is_valid_sqlite=result["is_valid_sqlite"],
            has_projects=result["has_projects"],
            has_files=result["has_files"],
            project_count=result["project_count"],
            file_count=result["file_count"],
            tables=result["tables"],
            error=result["error"],
        ))

    fields = [
        "student_id", "db_path", "is_valid_sqlite", "has_projects",
        "has_files", "project_count", "file_count", "tables", "error",
    ]
    with open(report_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(report_rows)

    return n_valid, n_invalid, valid_paths


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Inspect student metadata SQLite databases.")
    parser.add_argument("--in-dir", default=IN_DIR_DEFAULT)
    parser.add_argument("--report", default=REPORT_DEFAULT)
    args = parser.parse_args()

    print("Inspecting student databases...")
    n_valid, n_invalid, _ = inspect_all(args.in_dir, args.report)

    print(f"\nInspection summary:")
    print(f"  Valid   : {n_valid}")
    print(f"  Invalid : {n_invalid}")
    print(f"  Report  : {args.report}")

    if n_valid == 0:
        print("ERROR: no valid databases found.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
