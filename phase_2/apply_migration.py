"""
Apply the Phase 2 migration to the QDArchive SQLite database.

Fully idempotent: safe to run more than once.

Usage:
    python phase_2/apply_migration.py [--db PATH] [--sql PATH] [--dry-run]

Steps performed:
    1. Add projects.project_type TEXT (skipped if column already exists).
    2. Drop legacy index names created by an earlier migration run, if present.
    3. Execute phase2_migration.sql (all CREATE TABLE / INDEX use IF NOT EXISTS).
"""

import argparse
import sqlite3
import sys
from pathlib import Path


SQL_DEFAULT = Path(__file__).parent / "phase2_migration.sql"
DB_DEFAULT = "23727550-sq26.db"

# Indexes renamed between migration runs; drop old names so the SQL can
# create the canonical names without leaving duplicates.
_LEGACY_INDEXES = (
    "idx_project_classifications_primary_code",
    "idx_file_classifications_primary_code",
)


def column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    names = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    return column in names


def apply(db_path: str, sql_path: Path, dry_run: bool) -> None:
    sql = sql_path.read_text(encoding="utf-8")

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")

    try:
        needs_col = not column_exists(conn, "projects", "project_type")

        if dry_run:
            print(f"[dry-run] database  : {db_path}")
            print(f"[dry-run] sql file  : {sql_path}")
            print(
                f"[dry-run] ADD COLUMN projects.project_type : "
                f"{'yes' if needs_col else 'skip (already exists)'}"
            )
            print("[dry-run] No changes written.")
            return

        if needs_col:
            conn.execute("ALTER TABLE projects ADD COLUMN project_type TEXT")
            print("  + ALTER TABLE projects ADD COLUMN project_type TEXT")
        else:
            print("  ~ projects.project_type already exists, skipping ALTER TABLE")

        for idx in _LEGACY_INDEXES:
            conn.execute(f"DROP INDEX IF EXISTS [{idx}]")

        conn.executescript(sql)
        print("  + phase2_migration.sql applied (CREATE TABLE / INDEX IF NOT EXISTS)")

        tables = [
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
        ]
        print(f"  tables : {', '.join(tables)}")

    except Exception as exc:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise RuntimeError(str(exc)) from exc
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Apply the Phase 2 migration to the QDArchive SQLite database."
    )
    parser.add_argument("--db", default=DB_DEFAULT, help="Path to SQLite database")
    parser.add_argument(
        "--sql",
        default=str(SQL_DEFAULT),
        help="Path to migration SQL file",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without writing anything",
    )
    args = parser.parse_args()

    try:
        apply(args.db, Path(args.sql), args.dry_run)
        print("\nMigration complete.")
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
