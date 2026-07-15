"""
One-command regeneration of every derived report/export from the current
database state.

This script does not implement any chart, evaluation, XLSX, or PDF logic
itself — it discovers and calls the functions already defined in the
existing generator scripts:

    evaluate_isic_results.py               evaluate()
    plot_isic_evaluation.py                build_top20_divisions() and friends
    export_project_classification_table.py run()
    generate_project_classification_report.py  generate_report(), validate(),
                                                write_validation_report()
    build_final_deliverables.py            validate_selection(),
                                            reopen_verify_xlsx(), reopen_verify_pdf()
    check_isic_classification.py           run_checks(), run_combined_checks()

Every one of those functions was already written as a plain, side-effect-
scoped Python function returning structured data (not just a CLI entry
point), so all of them are used here via direct import rather than
subprocess — there was no case in this pipeline where invoking a script as
a subprocess was actually necessary.

Three of the scripts above originally hardcoded their output location to
"reports/" at the module level (no parameter to redirect it). To make
--reports-dir genuinely work end-to-end rather than silently writing to the
wrong place, this change added one optional, backward-compatible parameter
to each (their own CLI and any other existing caller behave exactly as
before when it's omitted):

    evaluate_isic_results.evaluate(db_path, reports_dir=None)
    export_project_classification_table.run(..., validation_report_path=None)
    export_project_classification_table.write_validation_report(checks, path=...)
    generate_project_classification_report.write_validation_report(checks, path=...)

Read-only against the database throughout (every connection uses
project_classification_data.connect_readonly, a SQLite read-only URI
connection) and never calls an API. Row counts across the tables this
pipeline touches are snapshotted before and after and compared, so any
accidental write would be caught rather than silently passed.

Usage:
    python phase_2/regenerate_all_outputs.py --db 23727550-sq26-combined.db
    ./regenerate_outputs.sh

Options:
    --db                PATH   default: 23727550-sq26-combined.db
    --reports-dir       PATH   default: reports
    --student-id        ID     default: 23727550 (used only to name the XLSX/PDF files)
    --skip-evaluation          skip regenerating the 4 evaluation CSVs
    --skip-figures             skip regenerating reports/figures/
    --skip-xlsx                skip regenerating both XLSX variants
    --skip-pdf                 skip regenerating the PDF report
    --skip-validation          skip refreshing isic_classification_validation.csv /
                                isic_combined_coverage.csv (the reopen-verify step
                                and the pre-flight selection check always still run)
    --continue-on-error        run every non-skipped stage even if an earlier one
                                fails, and report all failures at the end
    --dry-run                  print the plan (stages, commands, output paths) and exit
                                without touching any file
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from datetime import datetime
from pathlib import Path

_here = Path(__file__).parent
if str(_here) not in sys.path:
    sys.path.insert(0, str(_here))

import build_final_deliverables as deliverables
import check_isic_classification as classification_checks
import evaluate_isic_results
import export_project_classification_table as xlsx_exporter
import generate_project_classification_report as pdf_reporter
import plot_isic_evaluation
from project_classification_data import (
    DEFAULT_FALLBACK_METHOD,
    DEFAULT_PREFERRED_METHOD,
    connect_readonly,
)

DB_DEFAULT = "23727550-sq26-combined.db"
REPORTS_DIR_DEFAULT = "reports"
STUDENT_ID_DEFAULT = "23727550"

# Tables this pipeline reads; snapshotted before/after to prove nothing wrote to the DB.
SNAPSHOT_TABLES = [
    "combined_projects", "combined_files", "project_classifications",
    "classification_inputs", "isic_divisions",
]

FIGURE_BUILDERS = [
    plot_isic_evaluation.build_top20_divisions,
    plot_isic_evaluation.build_confidence_distribution,
    plot_isic_evaluation.build_classification_coverage,
    plot_isic_evaluation.build_model_agreement,
    plot_isic_evaluation.build_concurrency_throughput,
]
ALWAYS_EXPECTED_FIGURES = [
    "isic_top20_divisions", "isic_confidence_distribution",
    "classification_coverage", "model_agreement",
]


class StageError(Exception):
    """Raised to abort the run when a stage fails and --continue-on-error is not set."""


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _file_info(path: Path) -> str:
    if not path.exists():
        return f"{path}  (missing)"
    stat = path.stat()
    modified = datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
    return f"{path}  ({stat.st_size:,} bytes, modified {modified})"


def _snapshot_row_counts(db_path: str) -> dict[str, int]:
    conn = connect_readonly(db_path)
    counts = {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in SNAPSHOT_TABLES}
    conn.close()
    return counts


def _check_csv_has_rows(label: str, path: Path) -> dict:
    if not path.exists():
        return {"check": label, "status": "FAIL", "detail": f"missing: {path}"}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        first_row = next(reader, None)
    if header is None:
        return {"check": label, "status": "FAIL", "detail": "no header row"}
    if first_row is None:
        return {"check": label, "status": "INFO", "detail": f"header present, 0 data rows ({path})"}
    return {"check": label, "status": "PASS", "detail": f"header + data present ({path})"}


def _write_checks_csv(path: Path, checks: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["check", "status", "detail"])
        w.writeheader()
        w.writerows(checks)


def _print_checks(checks: list[dict]) -> None:
    for c in checks:
        detail = f" ({c['detail']})" if c.get("detail") else ""
        print(f"    [{c['status']}] {c['check']}{detail}")


def _run_stage(
    name: str, skip: bool, continue_on_error: bool, stage_log: list[dict], fn,
) -> list[dict] | None:
    print(f"\n{'-' * 64}")
    print(f"Stage: {name}  [{_now_str()}]")
    print("-" * 64)

    if skip:
        print("  SKIPPED (flag)")
        stage_log.append({"stage": name, "status": "SKIPPED", "elapsed": 0.0})
        return None

    t0 = time.perf_counter()
    try:
        checks = fn() or []
    except Exception as exc:
        elapsed = time.perf_counter() - t0
        print(f"  FAILED after {elapsed:.2f}s: {exc}", file=sys.stderr)
        stage_log.append({"stage": name, "status": "FAILED", "elapsed": elapsed, "error": str(exc)})
        if not continue_on_error:
            raise StageError(name) from exc
        return None

    elapsed = time.perf_counter() - t0
    failed = [c for c in checks if c.get("status") == "FAIL"]
    print(f"  {len(checks)} check(s), {len(failed)} failed — completed in {elapsed:.2f}s")
    _print_checks(checks)
    stage_log.append({
        "stage": name, "status": "FAILED" if failed else "OK", "elapsed": elapsed,
    })
    if failed and not continue_on_error:
        raise StageError(name)
    return checks


# ---------------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------------

def _print_plan(paths: dict[str, Path], args: argparse.Namespace) -> None:
    print("Planned stages (in order):")
    plan = [
        ("1. Validate DB + accepted classifications", False, "build_final_deliverables.validate_selection()"),
        ("2. Evaluation CSV reports", args.skip_evaluation, "evaluate_isic_results.evaluate()"),
        ("3. Evaluation charts/figures", args.skip_figures, "plot_isic_evaluation.build_*()"),
        ("4. Classified-only XLSX", args.skip_xlsx, "export_project_classification_table.run(include_unclassified=False)"),
        ("5. Full XLSX (incl. unclassified)", args.skip_xlsx, "export_project_classification_table.run(include_unclassified=True)"),
        ("6. PDF report", args.skip_pdf, "generate_project_classification_report.generate_report()"),
        ("7. Classification validation refresh", args.skip_validation, "check_isic_classification.run_checks()/run_combined_checks()"),
        ("8. Reopen + verify XLSX/PDF", False, "build_final_deliverables.reopen_verify_xlsx()/reopen_verify_pdf()"),
        ("9. Final PASS/FAIL summary", False, "(printed by this script)"),
    ]
    for label, skip, call in plan:
        marker = "SKIP" if skip else "RUN "
        print(f"  [{marker}] {label:<42} -> {call}")

    print("\nOutput paths that would be written:")
    for label, path in paths.items():
        print(f"  {label:<28}: {path}")
    print("\n(--dry-run: no files were read or written, no database connection was opened)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Regenerate every derived report/export (evaluation CSVs, figures, "
                    "XLSX tables, PDF report, validation CSVs) from the current database "
                    "state. Does not rerun classification and does not call any API.",
    )
    parser.add_argument("--db", default=DB_DEFAULT)
    parser.add_argument("--reports-dir", default=REPORTS_DIR_DEFAULT)
    parser.add_argument("--student-id", default=STUDENT_ID_DEFAULT)
    parser.add_argument("--skip-evaluation", action="store_true")
    parser.add_argument("--skip-figures", action="store_true")
    parser.add_argument("--skip-xlsx", action="store_true")
    parser.add_argument("--skip-pdf", action="store_true")
    parser.add_argument("--skip-validation", action="store_true")
    parser.add_argument("--continue-on-error", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    db_path = Path(args.db)
    reports_dir = Path(args.reports_dir)
    figures_dir = reports_dir / "figures"
    table_basename = f"{args.student_id}-sq26-project-classification-table"

    paths = {
        "isic_evaluation_summary.csv": reports_dir / "isic_evaluation_summary.csv",
        "isic_division_distribution.csv": reports_dir / "isic_division_distribution.csv",
        "isic_confidence_distribution.csv": reports_dir / "isic_confidence_distribution.csv",
        "isic_model_statistics.csv": reports_dir / "isic_model_statistics.csv",
        "figures directory": figures_dir,
        "classified-only XLSX": reports_dir / f"{table_basename}.xlsx",
        "full XLSX (incl. unclassified)": reports_dir / f"{table_basename}-full.xlsx",
        "PDF report": reports_dir / f"{args.student_id}-sq26-project-classification-report.pdf",
        "table validation CSV": reports_dir / "project_classification_table_validation.csv",
        "full-table validation CSV": reports_dir / "project_classification_table_full_validation.csv",
        "report validation CSV": reports_dir / "project_classification_report_validation.csv",
        "classification validation CSV": reports_dir / "isic_classification_validation.csv",
        "combined coverage CSV": reports_dir / "isic_combined_coverage.csv",
    }

    print("=" * 64)
    print("Regenerate All Outputs")
    print("=" * 64)
    print(f"  db          : {db_path}")
    print(f"  reports-dir : {reports_dir}")
    print(f"  student-id  : {args.student_id}")
    print(f"  started     : {_now_str()} (local time)")

    if args.dry_run:
        print()
        _print_plan(paths, args)
        return

    if not db_path.exists():
        print(f"\nERROR: database not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    reports_dir.mkdir(parents=True, exist_ok=True)

    run_t0 = time.perf_counter()
    before_counts = _snapshot_row_counts(str(db_path))

    context: dict = {}
    stage_log: list[dict] = []
    all_checks: list[dict] = []
    aborted_at: str | None = None

    def stage_validate() -> list[dict]:
        conn = connect_readonly(str(db_path))
        try:
            return deliverables.validate_selection(conn, DEFAULT_PREFERRED_METHOD, DEFAULT_FALLBACK_METHOD)
        finally:
            conn.close()

    def stage_evaluation() -> list[dict]:
        result = evaluate_isic_results.evaluate(str(db_path), reports_dir=reports_dir)
        context["evaluation_result"] = result
        checks = [
            {"check": f"evaluation integrity: {name}", "status": "PASS" if value == 0 else "FAIL", "detail": str(value)}
            for name, value in result["checks"].items()
        ]
        for label, path in result["report_paths"].items():
            checks.append(_check_csv_has_rows(f"{label} CSV has header and data", path))
        return checks

    def stage_figures() -> list[dict]:
        checks = []
        created = []
        for build in FIGURE_BUILDERS:
            result = build(reports_dir, figures_dir)
            if result["status"] == "created":
                created.append(result["name"])
                checks.append({"check": f"figure: {result['name']}", "status": "PASS", "detail": str(result["png"])})
            else:
                checks.append({"check": f"figure: {result['name']}", "status": "INFO", "detail": result["reason"]})
        context["figures_created"] = created
        missing = [n for n in ALWAYS_EXPECTED_FIGURES if n not in created]
        checks.append({
            "check": "figures directory contains expected outputs",
            "status": "PASS" if not missing else "FAIL",
            "detail": "all present" if not missing else f"missing: {missing}",
        })
        return checks

    def stage_xlsx_classified() -> list[dict]:
        rows, checks = xlsx_exporter.run(
            db_path=str(db_path), output_path=str(paths["classified-only XLSX"]), include_unclassified=False,
            preferred_method=DEFAULT_PREFERRED_METHOD, fallback_method=DEFAULT_FALLBACK_METHOD,
            validation_report_path=paths["table validation CSV"],
        )
        context["xlsx_classified_rows"] = len(rows)
        return checks

    def stage_xlsx_full() -> list[dict]:
        rows, checks = xlsx_exporter.run(
            db_path=str(db_path), output_path=str(paths["full XLSX (incl. unclassified)"]), include_unclassified=True,
            preferred_method=DEFAULT_PREFERRED_METHOD, fallback_method=DEFAULT_FALLBACK_METHOD,
            validation_report_path=paths["full-table validation CSV"],
        )
        context["xlsx_full_rows"] = len(rows)
        return checks

    def stage_pdf() -> list[dict]:
        result = pdf_reporter.generate_report(
            db_path=str(db_path), output_path=str(paths["PDF report"]),
            preferred_method=DEFAULT_PREFERRED_METHOD, fallback_method=DEFAULT_FALLBACK_METHOD,
            top_n=pdf_reporter.TOP_N_DEFAULT,
        )
        checks = pdf_reporter.validate(result, set(result["titles"].keys()))
        pdf_reporter.write_validation_report(checks, paths["report validation CSV"])
        context["pdf_result"] = result
        return checks

    def stage_classification_validation() -> list[dict]:
        checks_a = classification_checks.run_checks(str(db_path))
        _write_checks_csv(paths["classification validation CSV"], checks_a)
        checks_b = classification_checks.run_combined_checks(
            str(db_path), [DEFAULT_PREFERRED_METHOD, DEFAULT_FALLBACK_METHOD],
        )
        _write_checks_csv(paths["combined coverage CSV"], checks_b)
        return checks_a + checks_b

    def stage_reopen_verify() -> list[dict]:
        checks = []
        if "xlsx_classified_rows" in context:
            checks += deliverables.reopen_verify_xlsx(paths["classified-only XLSX"], context["xlsx_classified_rows"])
        if "xlsx_full_rows" in context:
            checks += deliverables.reopen_verify_xlsx(paths["full XLSX (incl. unclassified)"], context["xlsx_full_rows"])
        pdf_result = context.get("pdf_result")
        if pdf_result is not None:
            pdf_path = paths["PDF report"]
            checks += deliverables.reopen_verify_pdf(pdf_path, pdf_result["page_count"])
            try:
                from pypdf import PdfReader
                n_pages = len(PdfReader(str(pdf_path)).pages)
                checks.append({"check": "PDF has at least one page", "status": "PASS" if n_pages >= 1 else "FAIL",
                               "detail": f"{n_pages} pages"})
            except Exception as exc:
                checks.append({"check": "PDF has at least one page", "status": "FAIL", "detail": str(exc)})
        return checks

    try:
        all_checks += _run_stage("1. Validate DB + accepted classifications", False,
                                  args.continue_on_error, stage_log, stage_validate) or []
        all_checks += _run_stage("2. Evaluation CSV reports", args.skip_evaluation,
                                  args.continue_on_error, stage_log, stage_evaluation) or []
        all_checks += _run_stage("3. Evaluation charts/figures", args.skip_figures,
                                  args.continue_on_error, stage_log, stage_figures) or []
        all_checks += _run_stage("4. Classified-only XLSX", args.skip_xlsx,
                                  args.continue_on_error, stage_log, stage_xlsx_classified) or []
        all_checks += _run_stage("5. Full XLSX (incl. unclassified)", args.skip_xlsx,
                                  args.continue_on_error, stage_log, stage_xlsx_full) or []
        all_checks += _run_stage("6. PDF report", args.skip_pdf,
                                  args.continue_on_error, stage_log, stage_pdf) or []
        all_checks += _run_stage("7. Classification validation refresh", args.skip_validation,
                                  args.continue_on_error, stage_log, stage_classification_validation) or []
        all_checks += _run_stage("8. Reopen + verify XLSX/PDF", False,
                                  args.continue_on_error, stage_log, stage_reopen_verify) or []
    except StageError as exc:
        aborted_at = str(exc)

    after_counts = _snapshot_row_counts(str(db_path))
    for table in SNAPSHOT_TABLES:
        same = before_counts[table] == after_counts[table]
        all_checks.append({
            "check": f"row count unchanged: {table}", "status": "PASS" if same else "FAIL",
            "detail": f"{before_counts[table]:,} -> {after_counts[table]:,}",
        })

    total_elapsed = time.perf_counter() - run_t0

    print(f"\n{'=' * 64}")
    print("9. Final Summary")
    print("=" * 64)
    print(f"  finished    : {_now_str()} (local time)")
    print(f"  total time  : {total_elapsed:.2f}s")
    if aborted_at:
        print(f"  ABORTED at stage: {aborted_at} (default stop-on-first-failure; "
              f"pass --continue-on-error to run remaining stages anyway)")

    print("\nStage log:")
    for entry in stage_log:
        print(f"  [{entry['status']:<8}] {entry['stage']:<42} {entry['elapsed']:.2f}s")

    print("\nGenerated file inventory:")
    for label, path in paths.items():
        print(f"  {label:<32}: {_file_info(path)}")

    failed_checks = [c for c in all_checks if c.get("status") == "FAIL"]
    info_checks = [c for c in all_checks if c.get("status") == "INFO"]
    pass_checks = [c for c in all_checks if c.get("status") == "PASS"]
    overall = "PASS" if not failed_checks and not aborted_at else "FAIL"

    print(f"\nOverall: {overall}")
    print(f"  checks — pass: {len(pass_checks)}  fail: {len(failed_checks)}  info: {len(info_checks)}")
    if failed_checks:
        print("\nFailed checks:")
        _print_checks(failed_checks)
    print("=" * 64)

    if overall != "PASS":
        sys.exit(1)


if __name__ == "__main__":
    main()
