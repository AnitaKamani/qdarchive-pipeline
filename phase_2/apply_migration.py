"""
Apply the Phase 2 migration to the QDArchive SQLite database.

Fully idempotent: safe to run more than once.

Usage:
    python phase_2/apply_migration.py [--db PATH] [--sql PATH] [--dry-run]

Steps performed:
    1. Add projects.project_type TEXT (skipped if column already exists).
    2. Execute phase2_migration.sql (all CREATE TABLE / INDEX use IF NOT EXISTS).
"""

import argparse
import sqlite3
import sys
from pathlib import Path


SQL_DEFAULT = Path(__file__).parent / "phase2_migration.sql"
DB_DEFAULT = "23727550-sq26.db"


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
            print(f"[dry-run] ADD COLUMN projects.project_type : {'yes' if needs_col else 'skip (already exists)'}")
            print("[dry-run] No changes written.")
            return

        if needs_col:
            conn.execute("ALTER TABLE projects ADD COLUMN project_type TEXT")
            print("  + ALTER TABLE projects ADD COLUMN project_type TEXT")
        else:
            print("  ~ projects.project_type already exists, skipping ALTER TABLE")

        conn.executescript(sql)
        print("  + phase2_migration.sql applied (CREATE TABLE / INDEX IF NOT EXISTS)")

        print("\nMigration complete.")
        print(f"  database : {db_path}")

        # Report new tables present after migration
        tables = [
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
        ]
        print(f"  tables   : {', '.join(tables)}")

    except Exception as exc:
        conn.execute("ROLLBACK")
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
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

    apply(args.db, Path(args.sql), args.dry_run)


if __name__ == "__main__":
    main()
