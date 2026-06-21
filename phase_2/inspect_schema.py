"""
Inspect the QDArchive SQLite database schema and write a markdown report.

Usage:
    python phase_2/inspect_schema.py [--db PATH]

The database is opened read-only; no modifications are made.
"""

import argparse
import sqlite3
from pathlib import Path
from datetime import datetime


def open_readonly(db_path: str) -> sqlite3.Connection:
    uri = f"file:{db_path}?mode=ro"
    return sqlite3.connect(uri, uri=True)


def get_tables(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    return [r[0] for r in rows]


def get_views(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='view' ORDER BY name"
    ).fetchall()
    return [r[0] for r in rows]


def get_columns(conn: sqlite3.Connection, table: str) -> list[dict]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    # cid, name, type, notnull, dflt_value, pk
    return [
        {
            "cid": r[0],
            "name": r[1],
            "type": r[2] or "—",
            "not_null": bool(r[3]),
            "default": r[4],
            "pk": r[5],
        }
        for r in rows
    ]


def get_foreign_keys(conn: sqlite3.Connection, table: str) -> list[dict]:
    rows = conn.execute(f"PRAGMA foreign_key_list({table})").fetchall()
    # id, seq, table, from, to, on_update, on_delete, match
    return [
        {
            "from_col": r[3],
            "to_table": r[2],
            "to_col": r[4],
            "on_update": r[5],
            "on_delete": r[6],
        }
        for r in rows
    ]


def get_row_count(conn: sqlite3.Connection, table: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM [{table}]").fetchone()[0]


def get_check_constraints(conn: sqlite3.Connection, table: str) -> list[str]:
    """Extract CHECK(...) constraint text from the CREATE TABLE statement."""
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    if not row or not row[0]:
        return []
    sql = row[0]
    constraints = []
    import re
    for match in re.finditer(r"CHECK\s*\(([^)]+)\)", sql, re.IGNORECASE):
        constraints.append(match.group(1).strip())
    return constraints


def get_view_sql(conn: sqlite3.Connection, view: str) -> str:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='view' AND name=?", (view,)
    ).fetchone()
    return row[0] if row else ""


def build_report(db_path: str) -> str:
    conn = open_readonly(db_path)
    tables = get_tables(conn)
    views = get_views(conn)

    lines: list[str] = []

    lines.append("# QDArchive Database Schema Overview")
    lines.append("")
    lines.append(f"**Database:** `{db_path}`  ")
    lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  ")
    lines.append(f"**Tables:** {len(tables)}  ")
    lines.append(f"**Views:** {len(views)}")
    lines.append("")
    lines.append("---")
    lines.append("")

    # ── Tables ──────────────────────────────────────────────────────────────
    lines.append("## Tables")
    lines.append("")

    for table in tables:
        columns = get_columns(conn, table)
        foreign_keys = get_foreign_keys(conn, table)
        checks = get_check_constraints(conn, table)
        row_count = get_row_count(conn, table)

        lines.append(f"### `{table}`")
        lines.append("")
        lines.append(f"**Row count:** {row_count:,}")
        lines.append("")

        # Columns table
        lines.append("| # | Column | Type | PK | Nullable | Default |")
        lines.append("|---|--------|------|----|----------|---------|")
        for col in columns:
            pk_mark = f"PK{col['pk']}" if col["pk"] else ""
            nullable = "NOT NULL" if col["not_null"] else "nullable"
            default = f"`{col['default']}`" if col["default"] is not None else "—"
            lines.append(
                f"| {col['cid']} | `{col['name']}` | `{col['type']}` "
                f"| {pk_mark} | {nullable} | {default} |"
            )
        lines.append("")

        # Foreign keys
        if foreign_keys:
            lines.append("**Foreign keys:**")
            lines.append("")
            lines.append("| Column | References | On Update | On Delete |")
            lines.append("|--------|------------|-----------|-----------|")
            for fk in foreign_keys:
                lines.append(
                    f"| `{fk['from_col']}` | `{fk['to_table']}({fk['to_col']})` "
                    f"| {fk['on_update']} | {fk['on_delete']} |"
                )
            lines.append("")

        # CHECK constraints
        if checks:
            lines.append("**CHECK constraints:**")
            lines.append("")
            for chk in checks:
                lines.append(f"- `{chk}`")
            lines.append("")

        lines.append("---")
        lines.append("")

    # ── Views ────────────────────────────────────────────────────────────────
    lines.append("## Views")
    lines.append("")

    for view in views:
        sql = get_view_sql(conn, view)
        lines.append(f"### `{view}`")
        lines.append("")
        lines.append("```sql")
        lines.append(sql)
        lines.append("```")
        lines.append("")

    conn.close()
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Inspect QDArchive SQLite schema and write a markdown report."
    )
    parser.add_argument(
        "--db",
        default="23727550-sq26.db",
        help="Path to the SQLite database (default: 23727550-sq26.db)",
    )
    parser.add_argument(
        "--out",
        default="phase_2/schema_overview.md",
        help="Output markdown file (default: phase_2/schema_overview.md)",
    )
    args = parser.parse_args()

    report = build_report(args.db)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(report, encoding="utf-8")
    print(f"Report written to {out_path}")


if __name__ == "__main__":
    main()
