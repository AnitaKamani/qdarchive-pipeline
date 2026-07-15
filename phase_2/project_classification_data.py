"""
Shared, read-only data access for the final classification deliverables
(export_project_classification_table.py, generate_project_classification_report.py,
build_final_deliverables.py).

Never writes to the database. Never calls an API. Applies the cross-model
selection rule agreed for the final deliverables:

    For each eligible project, use exactly one successful production
    classification. Prefer --preferred-method (default openai:gpt-4.1-mini)
    if it succeeded; otherwise fall back to --fallback-method (default
    openai:gpt-4o-mini); otherwise the project is unclassified.
    local-dry-run and model_error rows are never considered.

Schema decisions (see docs/ for context, printed here so every entry point
states them before generating output):

  - The project key is combined_projects.global_project_id.
  - "Eligible" projects are combined_projects.project_type IN
    ('QDA_PROJECT', 'QD_PROJECT') — verified identical to the
    classification_inputs(target_type='PROJECT') population (38,032 rows,
    0 rows differ either direction).
  - repository_id is a plain, never-NULL INTEGER column on combined_projects,
    but low-cardinality (17 distinct values across all eligible projects):
    it identifies the source data repository/archive, not a single project.
  - no_project_files counts ALL combined_files rows linked via
    global_project_id, regardless of `status`. combined_files.status uses
    16 different, inconsistently-cased spellings of "success"/"failed"
    across student sources with no documented canonical set, so filtering
    to "successful" files would be an arbitrary, unspecified normalization.
    The field name ("number of project files") maps most directly onto the
    full file manifest, independent of download outcome.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

ELIGIBLE_PROJECT_TYPES = ("QDA_PROJECT", "QD_PROJECT")
IGNORED_METHODS = ("local-dry-run", "model_error")

DEFAULT_PREFERRED_METHOD = "openai:gpt-4.1-mini"
DEFAULT_FALLBACK_METHOD = "openai:gpt-4o-mini"

ISIC_LABEL_SEPARATOR = " — "


def connect_readonly(db_path: str) -> sqlite3.Connection:
    """Open the database strictly read-only via a SQLite URI, so an
    accidental write anywhere in these scripts raises rather than silently
    mutating production data."""
    uri = Path(db_path).resolve().as_uri() + "?mode=ro"
    return sqlite3.connect(uri, uri=True)


def load_isic_titles(conn: sqlite3.Connection) -> dict[str, str]:
    rows = conn.execute("SELECT code, title FROM isic_divisions").fetchall()
    return {code: title for code, title in rows}


def isic_label(code: str | None, titles: dict[str, str]) -> str:
    """'R86 — Human health activities', or '' if code is empty/unknown."""
    if not code:
        return ""
    title = titles.get(code)
    if title is None:
        return code
    return f"{code}{ISIC_LABEL_SEPARATOR}{title}"


def print_schema_decisions(preferred_method: str, fallback_method: str, include_unclassified: bool) -> None:
    print("Schema decisions:")
    print("  project key            : combined_projects.global_project_id")
    print(f"  eligible project_type  : {' / '.join(ELIGIBLE_PROJECT_TYPES)}")
    print("  repository_id          : combined_projects.repository_id (never NULL; identifies source archive)")
    print("  file linkage           : combined_files.global_project_id -> combined_projects.global_project_id")
    print("  no_project_files       : ALL linked combined_files rows, regardless of status")
    print(f"  cross-model preference : {preferred_method}, fallback {fallback_method}")
    print("  ignored methods        : " + ", ".join(IGNORED_METHODS))
    print(f"  unclassified projects  : {'included (empty classes)' if include_unclassified else 'excluded (default)'}")


def fetch_project_rows(
    conn: sqlite3.Connection,
    preferred_method: str = DEFAULT_PREFERRED_METHOD,
    fallback_method: str = DEFAULT_FALLBACK_METHOD,
    include_unclassified: bool = False,
) -> list[dict]:
    """One row per eligible project, cross-model classification already
    resolved to a single primary/secondary class (or None if unclassified).

    Deterministic order: repository_id, project_type, project_title, then
    global_project_id as the final tie-breaker.
    """
    file_counts = dict(
        conn.execute(
            "SELECT global_project_id, COUNT(*) FROM combined_files "
            "WHERE global_project_id IS NOT NULL GROUP BY global_project_id"
        ).fetchall()
    )

    placeholders = ", ".join("?" for _ in ELIGIBLE_PROJECT_TYPES)
    query = (
        "SELECT cp.global_project_id, cp.repository_id, cp.project_type, cp.title, "
        "pref.primary_class_code, pref.secondary_class_code, "
        "fb.primary_class_code, fb.secondary_class_code "
        "FROM combined_projects cp "
        "LEFT JOIN project_classifications pref "
        "  ON pref.project_id = cp.global_project_id AND pref.method = ? AND pref.primary_class_code IS NOT NULL "
        "LEFT JOIN project_classifications fb "
        "  ON fb.project_id = cp.global_project_id AND fb.method = ? AND fb.primary_class_code IS NOT NULL "
        f"WHERE cp.project_type IN ({placeholders}) "
        "ORDER BY cp.repository_id, cp.project_type, cp.title, cp.global_project_id"
    )
    rows = conn.execute(query, (preferred_method, fallback_method, *ELIGIBLE_PROJECT_TYPES)).fetchall()

    result: list[dict] = []
    for (
        global_project_id, repository_id, project_type, title,
        pref_primary, pref_secondary, fb_primary, fb_secondary,
    ) in rows:
        if pref_primary is not None:
            primary, secondary, method_used = pref_primary, pref_secondary, preferred_method
        elif fb_primary is not None:
            primary, secondary, method_used = fb_primary, fb_secondary, fallback_method
        else:
            primary, secondary, method_used = None, None, None

        if method_used is None and not include_unclassified:
            continue

        result.append({
            "global_project_id": global_project_id,
            "repository_id": repository_id,
            "project_type": project_type,
            "project_title": title or "",
            "primary_class_code": primary,
            "secondary_class_code": secondary,
            "method_used": method_used,
            "no_project_files": file_counts.get(global_project_id, 0),
        })

    return result


def coverage_counts(conn: sqlite3.Connection, methods: tuple[str, ...]) -> tuple[int, int, int]:
    """(total, covered, remaining) eligible PROJECT inputs, where 'covered'
    means a successful project_classifications row exists under any of
    `methods`. Mirrors the resume-filtering logic used by the classifier
    itself, so this reporting matches what the pipeline considers done."""
    total = conn.execute(
        "SELECT COUNT(*) FROM classification_inputs WHERE target_type = 'PROJECT'"
    ).fetchone()[0]
    placeholders = ", ".join("?" for _ in methods)
    remaining = conn.execute(
        "SELECT COUNT(*) FROM classification_inputs ci "
        "WHERE ci.target_type = 'PROJECT' AND NOT EXISTS ("
        "SELECT 1 FROM project_classifications pc "
        "WHERE pc.project_id = ci.project_id AND pc.method IN "
        f"({placeholders}))",
        methods,
    ).fetchone()[0]
    return total, total - remaining, remaining
