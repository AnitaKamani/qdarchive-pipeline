"""
One-command entry point for classification preparation (Milestone 3).

Steps:
    1. Ensure Phase 2 tables exist in combined DB
    2. Copy isic_divisions from source DB (if not present)
    3. Classify project types by file extension rules
    4. Build PROJECT classification_inputs

Usage:
    python phase_2/prepare_classification.py [--db PATH] [--source-db PATH]
"""

import sqlite3
import sys
from pathlib import Path

_here = Path(__file__).parent
if str(_here) not in sys.path:
    sys.path.insert(0, str(_here))

from classify_project_types import classify_all
from build_classification_inputs import build_inputs

DB_DEFAULT = "23727550-sq26-combined.db"
SOURCE_DB_DEFAULT = "23727550-sq26.db"

_PHASE2_SCHEMA = """\
PRAGMA foreign_keys = OFF;

CREATE TABLE IF NOT EXISTS isic_divisions (
    code        TEXT PRIMARY KEY,
    section_code TEXT,
    division    INTEGER,
    title       TEXT NOT NULL,
    description TEXT
);

CREATE TABLE IF NOT EXISTS classification_inputs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    target_type TEXT    NOT NULL,
    target_id   INTEGER NOT NULL,
    project_id  INTEGER,
    input_text  TEXT    NOT NULL,
    created_at  TEXT    DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (target_type, target_id)
);

CREATE TABLE IF NOT EXISTS project_classifications (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id          INTEGER NOT NULL,
    primary_class_code  TEXT,
    secondary_class_code TEXT,
    tags                TEXT,
    confidence          REAL,
    method              TEXT,
    reason              TEXT,
    created_at          TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (project_id, method)
);

CREATE TABLE IF NOT EXISTS file_classifications (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id             INTEGER NOT NULL,
    project_id          INTEGER NOT NULL,
    primary_class_code  TEXT,
    secondary_class_code TEXT,
    tags                TEXT,
    confidence          REAL,
    method              TEXT,
    reason              TEXT,
    created_at          TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (file_id, method)
);

CREATE INDEX IF NOT EXISTS idx_classification_inputs_target
    ON classification_inputs (target_type, target_id);
CREATE INDEX IF NOT EXISTS idx_project_classifications_project
    ON project_classifications (project_id);
CREATE INDEX IF NOT EXISTS idx_file_classifications_file
    ON file_classifications (file_id);
"""


def ensure_phase2_tables(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.executescript(_PHASE2_SCHEMA)
    # Guard for project_type column (may already exist on pre-built combined DBs)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(combined_projects)")}
    if "project_type" not in cols:
        conn.execute("ALTER TABLE combined_projects ADD COLUMN project_type TEXT")
        conn.commit()
        print("  Added project_type column to combined_projects.")
    conn.close()


def copy_isic_divisions(db_path: str, source_db_path: str) -> int:
    """Copy isic_divisions from source_db into db if the table is empty."""
    conn = sqlite3.connect(db_path)
    existing = conn.execute("SELECT COUNT(*) FROM isic_divisions").fetchone()[0]
    if existing > 0:
        print(f"  isic_divisions already has {existing} rows — skipping copy.")
        conn.close()
        return existing

    src = sqlite3.connect(f"file:{source_db_path}?mode=ro", uri=True)
    rows = src.execute(
        "SELECT code, section_code, division, title, description FROM isic_divisions"
    ).fetchall()
    src.close()

    conn.executemany(
        "INSERT OR IGNORE INTO isic_divisions "
        "(code, section_code, division, title, description) VALUES (?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()
    return len(rows)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Prepare combined DB for ISIC classification.")
    parser.add_argument("--db", default=DB_DEFAULT)
    parser.add_argument("--source-db", default=SOURCE_DB_DEFAULT)
    args = parser.parse_args()

    results: dict[str, str] = {}

    _banner("Phase 3 Classification Preparation")

    # ── Step 1: ensure tables ──────────────────────────────────────────────────
    _section("1/4", "Ensuring Phase 2 tables in combined DB")
    try:
        ensure_phase2_tables(args.db)
        print("  Tables ready.")
        results["setup_tables"] = "PASS"
    except Exception as exc:
        print(f"  ERROR: {exc}", file=sys.stderr)
        results["setup_tables"] = "FAIL"
        _summary(results)
        sys.exit(1)

    # ── Step 2: copy isic_divisions ────────────────────────────────────────────
    _section("2/4", "Copying ISIC divisions")
    try:
        n = copy_isic_divisions(args.db, args.source_db)
        print(f"  {n} ISIC divisions available.")
        results["copy_isic_divisions"] = "PASS"
    except Exception as exc:
        print(f"  ERROR: {exc}", file=sys.stderr)
        results["copy_isic_divisions"] = "FAIL"
        _summary(results)
        sys.exit(1)

    # ── Step 3: classify project types ─────────────────────────────────────────
    _section("3/4", "Classifying project types")
    try:
        counts = classify_all(args.db, dry_run=False)
        print("\n  Project type counts:")
        for pt in ("QDA_PROJECT", "QD_PROJECT", "OTHER_PROJECT", "NOT_A_PROJECT"):
            print(f"    {pt:<20} : {counts.get(pt, 0):,}")
        results["classify_project_types"] = "PASS"
    except Exception as exc:
        print(f"  ERROR: {exc}", file=sys.stderr)
        results["classify_project_types"] = "FAIL"

    # ── Step 4: build classification inputs ────────────────────────────────────
    _section("4/4", "Building PROJECT classification_inputs")
    try:
        input_counts = build_inputs(
            args.db,
            target_type="PROJECT",
            limit=None,
            include_types=["QDA_PROJECT", "QD_PROJECT"],
        )
        results["build_classification_inputs"] = (
            "PASS" if input_counts.get("PROJECT", 0) > 0 else "FAIL"
        )
    except Exception as exc:
        print(f"  ERROR: {exc}", file=sys.stderr)
        results["build_classification_inputs"] = "FAIL"

    _summary(results)
    if any(v == "FAIL" for v in results.values()):
        sys.exit(1)


def _banner(title: str) -> None:
    print("=" * 64)
    print(title)
    print("=" * 64)


def _section(step: str, title: str) -> None:
    print(f"\n[{step}] {title}...")


def _summary(results: dict[str, str]) -> None:
    print()
    print("=" * 64)
    print("Summary")
    print("=" * 64)
    for key, status in results.items():
        print(f"  [{'PASS' if status == 'PASS' else 'FAIL'}] {key}")
    overall = "PASS" if all(v == "PASS" for v in results.values()) else "FAIL"
    print("-" * 64)
    print(f"  Overall: {overall}")
    print("=" * 64)


if __name__ == "__main__":
    main()
