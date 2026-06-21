"""
Merge student metadata SQLite databases into a single combined database.

Usage:
    python phase_2/merge_student_dbs.py [--in-dir PATH] [--combined-db PATH]

Combined tables created:
    combined_projects, combined_files, combined_keywords,
    combined_licenses, combined_person_role

Each row carries source_student_id so provenance is preserved.
Projects are de-duplicated within each student by UNIQUE(source_student_id,
source_project_id). Cross-student duplicates are detected and reported
separately in possible_duplicates.csv but NOT removed from the combined DB.
"""

import csv
import sqlite3
import sys
from pathlib import Path

IN_DIR_DEFAULT = "data/student_metadata"
COMBINED_DB_DEFAULT = "23727550-sq26-combined.db"
MERGE_REPORT_DEFAULT = "reports/merge_report.csv"
DUP_REPORT_DEFAULT = "reports/possible_duplicates.csv"

_SCHEMA = """\
PRAGMA foreign_keys = OFF;

CREATE TABLE IF NOT EXISTS combined_projects (
    global_project_id           INTEGER PRIMARY KEY AUTOINCREMENT,
    source_student_id           TEXT,
    source_project_id           INTEGER,
    repository_id               INTEGER,
    repository_url              TEXT,
    project_url                 TEXT,
    version                     TEXT,
    title                       TEXT,
    description                 TEXT,
    language                    TEXT,
    doi                         TEXT,
    upload_date                 TEXT,
    download_date               TEXT,
    download_repository_folder  TEXT,
    download_project_folder     TEXT,
    download_version_folder     TEXT,
    download_method             TEXT,
    project_type                TEXT,
    UNIQUE (source_student_id, source_project_id)
);

CREATE TABLE IF NOT EXISTS combined_files (
    global_file_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    global_project_id   INTEGER,
    source_student_id   TEXT,
    source_file_id      INTEGER,
    source_project_id   INTEGER,
    file_name           TEXT,
    file_type           TEXT,
    file_url            TEXT,
    file_size           INTEGER,
    zip_path            TEXT,
    status              TEXT,
    UNIQUE (source_student_id, source_file_id)
);

CREATE TABLE IF NOT EXISTS combined_keywords (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    global_project_id   INTEGER,
    source_student_id   TEXT,
    keyword             TEXT
);

CREATE TABLE IF NOT EXISTS combined_licenses (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    global_project_id   INTEGER,
    source_student_id   TEXT,
    license             TEXT
);

CREATE TABLE IF NOT EXISTS combined_person_role (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    global_project_id   INTEGER,
    source_student_id   TEXT,
    name                TEXT,
    role                TEXT
);
"""

# Desired columns from source projects (order matches INSERT below)
_PROJ_COLS = [
    "repository_id", "repository_url", "project_url", "version",
    "title", "description", "language", "doi", "upload_date",
    "download_date", "download_repository_folder", "download_project_folder",
    "download_version_folder", "download_method", "project_type",
]
_FILE_COLS = [
    "file_name", "file_type", "file_url", "file_size", "zip_path", "status",
]


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}


def _safe_select(available: set[str], desired: list[str]) -> str:
    parts = [f'"{c}"' if c in available else f'NULL AS "{c}"' for c in desired]
    return ", ".join(parts)


def _merge_source(
    dst: sqlite3.Connection,
    src: sqlite3.Connection,
    student_id: str,
) -> dict[str, int]:
    """Merge one source into the combined DB. Returns counts."""
    # Normalize table names to lowercase; SQLite queries are case-insensitive.
    src_tables = {
        r[0].lower()
        for r in src.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    if "projects" not in src_tables:
        raise ValueError("missing 'projects' table")
    if "files" not in src_tables:
        raise ValueError("missing 'files' table")

    src_proj_cols = _table_columns(src, "projects")
    src_file_cols = _table_columns(src, "files")

    proj_select = _safe_select(src_proj_cols, _PROJ_COLS)
    file_select = _safe_select(src_file_cols, _FILE_COLS)

    proj_map: dict[int, int] = {}  # source_project_id → global_project_id
    n_proj = n_files = n_kw = n_lic = n_pr = 0

    proj_insert = (
        f"INSERT OR IGNORE INTO combined_projects "
        f"(source_student_id, source_project_id, {', '.join(_PROJ_COLS)}) "
        f"VALUES (?, ?, {', '.join('?' for _ in _PROJ_COLS)})"
    )
    for p_row in src.execute(f"SELECT id, {proj_select} FROM projects"):
        src_pid = p_row[0]
        values = p_row[1:]
        cur = dst.execute(proj_insert, (student_id, src_pid) + values)
        if cur.rowcount == 1:
            global_pid = cur.lastrowid
            n_proj += 1
        else:
            existing = dst.execute(
                "SELECT global_project_id FROM combined_projects "
                "WHERE source_student_id=? AND source_project_id=?",
                (student_id, src_pid),
            ).fetchone()
            global_pid = existing[0] if existing else None
        if global_pid:
            proj_map[src_pid] = global_pid

    file_insert = (
        f"INSERT OR IGNORE INTO combined_files "
        f"(global_project_id, source_student_id, source_file_id, source_project_id, "
        f"{', '.join(_FILE_COLS)}) "
        f"VALUES (?, ?, ?, ?, {', '.join('?' for _ in _FILE_COLS)})"
    )
    for f_row in src.execute(f"SELECT id, project_id, {file_select} FROM files"):
        src_fid, src_pid = f_row[0], f_row[1]
        global_pid = proj_map.get(src_pid)
        if global_pid is None:
            continue
        if dst.execute(file_insert, (global_pid, student_id, src_fid, src_pid) + f_row[2:]).rowcount == 1:
            n_files += 1

    if "keywords" in src_tables and "keyword" in _table_columns(src, "keywords"):
        for r in src.execute("SELECT project_id, keyword FROM keywords"):
            gp = proj_map.get(r[0])
            if gp:
                dst.execute(
                    "INSERT INTO combined_keywords "
                    "(global_project_id, source_student_id, keyword) VALUES (?,?,?)",
                    (gp, student_id, r[1]),
                )
                n_kw += 1

    if "licenses" in src_tables and "license" in _table_columns(src, "licenses"):
        for r in src.execute("SELECT project_id, license FROM licenses"):
            gp = proj_map.get(r[0])
            if gp:
                dst.execute(
                    "INSERT INTO combined_licenses "
                    "(global_project_id, source_student_id, license) VALUES (?,?,?)",
                    (gp, student_id, r[1]),
                )
                n_lic += 1

    # person_role table may be uppercase (PERSON_ROLE) — SQLite queries are case-insensitive
    if "person_role" in src_tables:
        pr_cols = _table_columns(src, "person_role")
        if "name" in pr_cols and "role" in pr_cols:
            for r in src.execute("SELECT project_id, name, role FROM person_role"):
                gp = proj_map.get(r[0])
                if gp:
                    dst.execute(
                        "INSERT INTO combined_person_role "
                        "(global_project_id, source_student_id, name, role) VALUES (?,?,?,?)",
                        (gp, student_id, r[1], r[2]),
                    )
                    n_pr += 1

    return dict(projects=n_proj, files=n_files, keywords=n_kw, licenses=n_lic, person_roles=n_pr)


def merge_all(
    in_dir: str = IN_DIR_DEFAULT,
    combined_db: str = COMBINED_DB_DEFAULT,
    merge_report: str = MERGE_REPORT_DEFAULT,
    dup_report: str = DUP_REPORT_DEFAULT,
) -> dict[str, int]:
    """Merge all .db files in in_dir into combined_db. Returns final table counts."""
    Path(merge_report).parent.mkdir(parents=True, exist_ok=True)

    dst = sqlite3.connect(combined_db)
    dst.executescript(_SCHEMA)
    dst.commit()

    db_files = sorted(Path(in_dir).glob("*.db"))
    merge_rows: list[dict] = []

    for db_path in db_files:
        student_id = db_path.stem
        print(f"  Merging {student_id}...", end="  ", flush=True)
        try:
            src = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            counts = _merge_source(dst, src, student_id)
            src.close()
            dst.commit()
            status, error = "merged", ""
            print(
                f"projects={counts['projects']} files={counts['files']} "
                f"kw={counts['keywords']} lic={counts['licenses']} pr={counts['person_roles']}"
            )
        except Exception as exc:
            counts = dict(projects=0, files=0, keywords=0, licenses=0, person_roles=0)
            status = "failed"
            error = str(exc)[:200]
            print(f"FAILED: {error}")

        merge_rows.append(dict(
            student_id=student_id,
            status=status,
            projects_imported=counts["projects"],
            files_imported=counts["files"],
            keywords_imported=counts["keywords"],
            licenses_imported=counts["licenses"],
            person_roles_imported=counts["person_roles"],
            error=error,
        ))

    # ── Duplicate detection ──────────────────────────────────────────────────
    dup_rows: list[dict] = []

    for dup_type, col in (("same_project_url", "project_url"), ("same_doi", "doi")):
        for r in dst.execute(
            f"SELECT {col}, COUNT(*) AS cnt, "
            f"GROUP_CONCAT(source_student_id, ';'), GROUP_CONCAT(global_project_id, ';') "
            f"FROM combined_projects "
            f"WHERE {col} IS NOT NULL AND {col} != '' "
            f"GROUP BY {col} HAVING cnt > 1"
        ):
            dup_rows.append(dict(
                duplicate_type=dup_type,
                value=r[0],
                count=r[1],
                source_students=r[2],
                global_project_ids=r[3],
            ))

    # ── Final counts ─────────────────────────────────────────────────────────
    totals = {}
    for tbl in ("combined_projects", "combined_files", "combined_keywords",
                "combined_licenses", "combined_person_role"):
        totals[tbl] = dst.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
    dst.close()

    # ── Write reports ─────────────────────────────────────────────────────────
    merge_fields = [
        "student_id", "status", "projects_imported", "files_imported",
        "keywords_imported", "licenses_imported", "person_roles_imported", "error",
    ]
    with open(merge_report, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=merge_fields)
        w.writeheader()
        w.writerows(merge_rows)

    dup_fields = ["duplicate_type", "value", "count", "source_students", "global_project_ids"]
    with open(dup_report, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=dup_fields)
        w.writeheader()
        w.writerows(dup_rows)

    totals["possible_duplicates"] = len(dup_rows)
    return totals


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Merge student metadata databases.")
    parser.add_argument("--in-dir", default=IN_DIR_DEFAULT)
    parser.add_argument("--combined-db", default=COMBINED_DB_DEFAULT)
    parser.add_argument("--merge-report", default=MERGE_REPORT_DEFAULT)
    parser.add_argument("--dup-report", default=DUP_REPORT_DEFAULT)
    args = parser.parse_args()

    print("Merging student databases...")
    totals = merge_all(args.in_dir, args.combined_db, args.merge_report, args.dup_report)

    print(f"\nMerge complete:")
    for k, v in totals.items():
        print(f"  {k:<30} : {v:,}")
    print(f"  Merge report      : {args.merge_report}")
    print(f"  Duplicate report  : {args.dup_report}")

    if totals.get("combined_projects", 0) == 0:
        print("ERROR: zero projects merged.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
