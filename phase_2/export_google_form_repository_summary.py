"""
Export one row per repository, ready for manual entry into the "SQ26 Results
Data" Google Form.

Read-only: opens the database via a SQLite read-only URI, makes no API calls,
and does not touch Phase 1, the classification pipeline, or the schema. Uses
the same cross-model selection rule as export_project_classification_table.py
and generate_project_classification_report.py (see project_classification_data.py):
prefer --preferred-method, fall back to --fallback-method, ignore
local-dry-run/model_error, and apply the same EXCLUDED_REPOSITORY_IDS filter.

Only repositories that actually have at least one combined_projects row are
emitted — there is nothing to enter into the form for a repository this
student's combined database has no data for. Repository 17 ("N/A" in the
mapping below) is never emitted even if it somehow had data, since it is not
a real repository slot.

database_link is only ever the exact GitHub URL to 23727550-sq26-classification.db
on the current branch, and only when that exact file is confirmed present in
the current commit (git ls-tree). If it is not committed, the field is left
blank and a prominent warning is printed — never an invented or guessed URL.

Usage:
    python phase_2/export_google_form_repository_summary.py [options]

Options:
    --db                PATH   default: 23727550-sq26-combined.db
    --output-csv        PATH   default: reports/23727550-sq26-google-form-summary.csv
    --output-txt        PATH   default: reports/23727550-sq26-google-form-summary.txt
    --preferred-method  METHOD default: openai:gpt-4.1-mini
    --fallback-method   METHOD default: openai:gpt-4o-mini
"""

from __future__ import annotations

import argparse
import csv
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path

_here = Path(__file__).parent
if str(_here) not in sys.path:
    sys.path.insert(0, str(_here))

from project_classification_data import (
    DEFAULT_FALLBACK_METHOD,
    DEFAULT_PREFERRED_METHOD,
    EXCLUDED_REPOSITORY_IDS,
    connect_readonly,
    fetch_project_rows,
    load_isic_titles,
    print_schema_decisions,
)

DB_DEFAULT = "23727550-sq26-combined.db"
OUTPUT_CSV_DEFAULT = "reports/23727550-sq26-google-form-summary.csv"
OUTPUT_TXT_DEFAULT = "reports/23727550-sq26-google-form-summary.txt"
VALIDATION_REPORT = "reports/google_form_repository_summary_validation.csv"
STUDENT_ID = "23727550"

# The exact file the Google Form's database_link field must point to. Not
# produced by this script and not assumed to exist — see resolve_database_link().
CLASSIFICATION_DB_FILENAME = "23727550-sq26-classification.db"

PROJECT_TYPES = ("QDA_PROJECT", "QD_PROJECT", "OTHER_PROJECT", "NOT_A_PROJECT")

# Course-wide repository_id -> name mapping for the combined SQ26 database
# (consistent across all merged students' data; not the same thing as a
# single harvester's own config.py REPOS list). 17 is a reserved/unused slot.
REPOSITORY_NAMES: dict[int, str] = {
    1: "zenodo",
    2: "dryad",
    3: "uk-data-service",
    4: "syracuse-qualitative-data-repository",
    5: "dans",
    6: "dataverse-no",
    7: "ada",
    8: "sada",
    9: "hihsn",
    10: "harvard-dataverse",
    11: "finnish-social-science-data-archive",
    12: "aussda",
    13: "cessda",
    14: "databray",
    15: "icpsr",
    16: "open-data-uni-halle",
    17: "N/A",
    18: "harvard-murray-archive",
    19: "columbia-oral-history-archive",
    20: "sikt",
}
NOT_APPLICABLE_REPOSITORY_IDS = {rid for rid, name in REPOSITORY_NAMES.items() if name == "N/A"}

HEADERS = [
    "student_id", "database_link", "repository_id", "repository_name",
    "total_projects_found", "no_qda_project", "no_qd_project", "no_other_project", "no_not_a_project",
    "most_common_class_code", "most_common_class_title", "most_common_class_form_value",
    "project_comment", "classification_comment",
]


# ---------------------------------------------------------------------------
# database_link resolution
# ---------------------------------------------------------------------------

def resolve_database_link(repo_root: Path) -> tuple[str, list[str]]:
    """The exact GitHub URL to CLASSIFICATION_DB_FILENAME on the current
    branch, but only if that exact file is confirmed committed at HEAD.
    Never guesses: any failure to prove the file's presence leaves the link
    blank and returns a warning describing why."""
    warnings: list[str] = []

    def _git(*args: str) -> str | None:
        try:
            result = subprocess.run(
                ["git", *args], cwd=repo_root, capture_output=True, text=True, check=True,
            )
            return result.stdout.strip()
        except Exception as exc:
            warnings.append(f"git {' '.join(args)} failed: {exc}")
            return None

    remote = _git("remote", "get-url", "origin")
    if remote is None:
        return "", warnings
    if remote.startswith("git@github.com:"):
        path = remote[len("git@github.com:"):]
    elif remote.startswith("https://github.com/"):
        path = remote[len("https://github.com/"):]
    else:
        warnings.append(f"origin remote is not a recognizable GitHub URL: {remote}")
        return "", warnings
    if path.endswith(".git"):
        path = path[: -len(".git")]

    branch = _git("rev-parse", "--abbrev-ref", "HEAD")
    if branch is None:
        return "", warnings

    tracked = _git("ls-tree", "-r", "--name-only", "HEAD")
    if tracked is None:
        return "", warnings

    if CLASSIFICATION_DB_FILENAME not in tracked.splitlines():
        warnings.append(
            f"{CLASSIFICATION_DB_FILENAME} is not committed on branch '{branch}' at HEAD — "
            "database_link left blank rather than inventing a URL."
        )
        return "", warnings

    return f"https://github.com/{path}/blob/{branch}/{CLASSIFICATION_DB_FILENAME}", warnings


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def _reconcile_project_type_totals(conn) -> None:
    """Requirement 3: total_projects_found (all combined_projects rows for a
    repository) must equal the sum of its four project_type counts, for
    every repository_id actually present in the table — not just the ones
    this export ends up reporting. Aborts hard on any mismatch rather than
    silently reconciling, since a mismatch would mean an unexpected
    project_type value (e.g. NULL or a typo) is hiding rows from the count."""
    raw_totals = dict(conn.execute(
        "SELECT repository_id, COUNT(*) FROM combined_projects GROUP BY repository_id"
    ).fetchall())
    typed = defaultdict(dict)
    for repository_id, project_type, count in conn.execute(
        "SELECT repository_id, project_type, COUNT(*) FROM combined_projects "
        "GROUP BY repository_id, project_type"
    ).fetchall():
        typed[repository_id][project_type] = count

    mismatches = []
    for repository_id, total in raw_totals.items():
        typed_sum = sum(typed[repository_id].get(pt, 0) for pt in PROJECT_TYPES)
        if typed_sum != total:
            mismatches.append((repository_id, total, typed_sum))
    if mismatches:
        raise SystemExit(
            "ABORT: total_projects_found != sum(no_qda_project, no_qd_project, no_other_project, "
            f"no_not_a_project) for {len(mismatches)} repository_id value(s): {mismatches}"
        )


def _project_comment(no_qda: int, no_qd: int, no_other: int, no_not_a: int) -> str:
    eligible = no_qda + no_qd
    excluded = no_other + no_not_a
    if eligible == 0:
        return "No eligible QDA/QD projects were found."
    if excluded == 0:
        if no_qda == no_qd:
            return "QDA_PROJECT and QD_PROJECT were equally represented."
        majority = "QDA_PROJECT" if no_qda > no_qd else "QD_PROJECT"
        return f"Most records were classified as {majority}."
    return "The repository contains a mixture of eligible and excluded project types."


def _classification_comment(
    class_counts: Counter, divisions: dict[str, int], titles: dict[str, str],
    eligible_count: int, classified_count: int,
) -> tuple[str, str, str, str]:
    """Returns (code, title, form_value, comment). code/title/form_value are
    '' when there are zero successfully classified eligible projects."""
    if not class_counts:
        if eligible_count == 0:
            return "", "", "", "No eligible QDA/QD projects were found in this repository."
        return "", "", "", (
            "No successfully classified eligible projects in this repository "
            f"({eligible_count:,} eligible project(s) found, 0 classified by the accepted "
            "production models)."
        )

    max_count = max(class_counts.values())
    tied = sorted((c for c, n in class_counts.items() if n == max_count), key=lambda c: divisions[c])
    best = tied[0]
    code, title = best, titles[best]
    form_value = f"{code} - {divisions[code]} - {title}"

    comment = (
        f"Most common class among successfully classified eligible projects: "
        f"{code} — {title} ({max_count:,} project(s))."
    )
    if len(tied) > 1:
        others = ", ".join(f"{c} — {titles[c]}" for c in tied[1:])
        comment += f" Tied with {others} at {max_count:,} project(s) each."
    if classified_count < eligible_count:
        comment += (
            f" {classified_count:,} of {eligible_count:,} eligible projects classified so far; "
            "coverage is incomplete."
        )
    return code, title, form_value, comment


def build_rows(
    conn, preferred_method: str, fallback_method: str, database_link: str,
) -> tuple[list[dict], set[str]]:
    _reconcile_project_type_totals(conn)

    titles = load_isic_titles(conn)
    divisions = dict(conn.execute("SELECT code, division FROM isic_divisions").fetchall())
    valid_form_values = {f"{code} - {divisions[code]} - {title}" for code, title in titles.items()}

    raw_totals = dict(conn.execute(
        "SELECT repository_id, COUNT(*) FROM combined_projects GROUP BY repository_id"
    ).fetchall())
    typed_counts = defaultdict(dict)
    for repository_id, project_type, count in conn.execute(
        "SELECT repository_id, project_type, COUNT(*) FROM combined_projects "
        "GROUP BY repository_id, project_type"
    ).fetchall():
        typed_counts[repository_id][project_type] = count

    eligible_rows = fetch_project_rows(
        conn, preferred_method=preferred_method, fallback_method=fallback_method, include_unclassified=True,
    )
    eligible_count_by_repo: dict[int, int] = defaultdict(int)
    classified_count_by_repo: dict[int, int] = defaultdict(int)
    class_counts_by_repo: dict[int, Counter] = defaultdict(Counter)
    for row in eligible_rows:
        repo_id = row["repository_id"]
        eligible_count_by_repo[repo_id] += 1
        if row["method_used"] is not None:
            classified_count_by_repo[repo_id] += 1
            class_counts_by_repo[repo_id][row["primary_class_code"]] += 1

    rows_out: list[dict] = []
    for repository_id, repository_name in REPOSITORY_NAMES.items():
        if repository_id in NOT_APPLICABLE_REPOSITORY_IDS or repository_id in EXCLUDED_REPOSITORY_IDS:
            continue
        total = raw_totals.get(repository_id, 0)
        if total == 0:
            continue  # nothing to enter into the form for a repository with no data

        counts = typed_counts.get(repository_id, {})
        no_qda = counts.get("QDA_PROJECT", 0)
        no_qd = counts.get("QD_PROJECT", 0)
        no_other = counts.get("OTHER_PROJECT", 0)
        no_not_a = counts.get("NOT_A_PROJECT", 0)

        eligible_count = eligible_count_by_repo.get(repository_id, 0)
        classified_count = classified_count_by_repo.get(repository_id, 0)
        class_counts = class_counts_by_repo.get(repository_id, Counter())

        code, class_title, form_value, classification_comment = _classification_comment(
            class_counts, divisions, titles, eligible_count, classified_count,
        )
        project_comment = _project_comment(no_qda, no_qd, no_other, no_not_a)

        rows_out.append({
            "student_id": STUDENT_ID,
            "database_link": database_link,
            "repository_id": repository_id,
            "repository_name": repository_name,
            "total_projects_found": total,
            "no_qda_project": no_qda,
            "no_qd_project": no_qd,
            "no_other_project": no_other,
            "no_not_a_project": no_not_a,
            "most_common_class_code": code,
            "most_common_class_title": class_title,
            "most_common_class_form_value": form_value,
            "project_comment": project_comment,
            "classification_comment": classification_comment,
        })

    return rows_out, valid_form_values


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------

def write_csv(rows: list[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=HEADERS)
        w.writeheader()
        w.writerows(rows)


def write_txt(rows: list[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    blocks = []
    for row in rows:
        most_common = row["most_common_class_form_value"] or "(none — see classification comment)"
        blocks.append(
            f"Repository: {row['repository_id']} {row['repository_name']}\n"
            f"Student ID: {row['student_id']}\n"
            f"Database link: {row['database_link']}\n"
            f"Total projects: {row['total_projects_found']}\n"
            f"QDA_PROJECT: {row['no_qda_project']}\n"
            f"QD_PROJECT: {row['no_qd_project']}\n"
            f"OTHER_PROJECT: {row['no_other_project']}\n"
            f"NOT_A_PROJECT: {row['no_not_a_project']}\n"
            f"Most common class: {most_common}\n"
            f"Project comment: {row['project_comment']}\n"
            f"Classification comment: {row['classification_comment']}\n"
        )
    output_path.write_text(("\n" + "-" * 64 + "\n\n").join(blocks) + "\n", encoding="utf-8")


def write_validation_report(checks: list[dict], path: str | Path = VALIDATION_REPORT) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["check", "status", "detail"])
        w.writeheader()
        w.writerows(checks)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate(rows: list[dict], valid_form_values: set[str], valid_codes: set[str]) -> list[dict]:
    checks: list[dict] = []

    def add(name: str, passed: bool, detail: str = "") -> None:
        checks.append({"check": name, "status": "PASS" if passed else "FAIL", "detail": detail})

    add("exact required headers", HEADERS == [
        "student_id", "database_link", "repository_id", "repository_name",
        "total_projects_found", "no_qda_project", "no_qd_project", "no_other_project", "no_not_a_project",
        "most_common_class_code", "most_common_class_title", "most_common_class_form_value",
        "project_comment", "classification_comment",
    ], ", ".join(HEADERS))

    repo_ids = [r["repository_id"] for r in rows]
    add("every included repository appears once", len(repo_ids) == len(set(repo_ids)),
        f"{len(repo_ids)} rows, {len(set(repo_ids))} distinct repository_id")
    add("no duplicate repository rows", len(repo_ids) == len(set(repo_ids)),
        "0 duplicates" if len(repo_ids) == len(set(repo_ids)) else "duplicates present")

    recon_fail = [
        r["repository_id"] for r in rows
        if r["total_projects_found"] != r["no_qda_project"] + r["no_qd_project"]
        + r["no_other_project"] + r["no_not_a_project"]
    ]
    add("project counts reconcile for every reported repository", len(recon_fail) == 0,
        "all reconcile" if not recon_fail else f"mismatched: {recon_fail}")

    bad_codes = [r["most_common_class_code"] for r in rows
                 if r["most_common_class_code"] and r["most_common_class_code"] not in valid_codes]
    add("no invalid ISIC codes among most_common_class_code", len(bad_codes) == 0,
        "none" if not bad_codes else f"invalid: {bad_codes}")

    bad_forms = [r["most_common_class_form_value"] for r in rows
                 if r["most_common_class_form_value"] and r["most_common_class_form_value"] not in valid_form_values]
    add("every non-blank form value matches one of the 87 class options", len(bad_forms) == 0,
        "none" if not bad_forms else f"invalid: {bad_forms}")

    excluded_present = [r["repository_id"] for r in rows if r["repository_id"] in EXCLUDED_REPOSITORY_IDS]
    add("excluded repository_id(s) absent from output", len(excluded_present) == 0,
        "0 rows" if not excluded_present else f"present: {excluded_present}")

    na_present = [r["repository_id"] for r in rows if r["repository_id"] in NOT_APPLICABLE_REPOSITORY_IDS]
    add("N/A repository slot(s) absent from output", len(na_present) == 0,
        "0 rows" if not na_present else f"present: {na_present}")

    return checks


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run(
    db_path: str,
    output_csv: str,
    output_txt: str,
    preferred_method: str,
    fallback_method: str,
    repo_root: str | Path | None = None,
    validation_report_path: str | Path | None = None,
) -> tuple[list[dict], list[dict], list[str]]:
    """Returns (rows, checks, database_link_warnings)."""
    repo_root = Path(repo_root) if repo_root is not None else Path(__file__).resolve().parent.parent

    conn = connect_readonly(db_path)
    print_schema_decisions(preferred_method, fallback_method, include_unclassified=True)

    database_link, link_warnings = resolve_database_link(repo_root)
    rows, valid_form_values = build_rows(conn, preferred_method, fallback_method, database_link)
    titles = load_isic_titles(conn)
    conn.close()

    write_csv(rows, Path(output_csv))
    write_txt(rows, Path(output_txt))

    checks = validate(rows, valid_form_values, set(titles.keys()))
    write_validation_report(checks, validation_report_path if validation_report_path is not None else VALIDATION_REPORT)

    return rows, checks, link_warnings


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Export one row per repository for the "SQ26 Results Data" Google Form.',
    )
    parser.add_argument("--db", default=DB_DEFAULT)
    parser.add_argument("--output-csv", default=OUTPUT_CSV_DEFAULT)
    parser.add_argument("--output-txt", default=OUTPUT_TXT_DEFAULT)
    parser.add_argument("--preferred-method", default=DEFAULT_PREFERRED_METHOD)
    parser.add_argument("--fallback-method", default=DEFAULT_FALLBACK_METHOD)
    args = parser.parse_args()

    print("=" * 64)
    print("Google Form Repository Summary Export")
    print("=" * 64)

    rows, checks, link_warnings = run(
        db_path=args.db,
        output_csv=args.output_csv,
        output_txt=args.output_txt,
        preferred_method=args.preferred_method,
        fallback_method=args.fallback_method,
    )

    print(f"\n{len(rows)} repository row(s) written.")
    print(f"CSV: {args.output_csv}")
    print(f"TXT: {args.output_txt}")

    if link_warnings:
        print("\n" + "!" * 64)
        print("WARNING: database_link could not be constructed/proven:")
        for w in link_warnings:
            print(f"  - {w}")
        print("!" * 64)

    print("\nValidation:")
    all_pass = True
    for c in checks:
        detail = f" ({c['detail']})" if c["detail"] else ""
        print(f"  [{c['status']}] {c['check']}{detail}")
        if c["status"] != "PASS":
            all_pass = False
    print(f"\n  Report: {VALIDATION_REPORT}")
    print("=" * 64)

    if not all_pass:
        sys.exit(1)


if __name__ == "__main__":
    main()
