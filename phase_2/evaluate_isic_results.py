"""
Evaluate the ISIC classification results already stored in project_classifications.

Read-only: opens the database in SQLite URI read-only mode, never writes to it,
makes no API calls, and does not touch the classification pipeline, prompts,
or schema. All metrics are derived purely from the existing rows, so the
reports are reproducible on demand by re-running this script.

Outputs (reports/, overwritten only by this script):
    isic_evaluation_summary.csv       coverage, model comparison, integrity checks
    isic_division_distribution.csv    all 87 ISIC divisions ranked by count
    isic_confidence_distribution.csv  confidence histogram (0.1-wide buckets)
    isic_model_statistics.csv         per-model confidence statistics

Usage:
    python phase_2/evaluate_isic_results.py [--db PATH]
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
import statistics
import sys
from pathlib import Path

DB_DEFAULT = "23727550-sq26-combined.db"
REPORTS_DIR = Path("reports")

SUMMARY_REPORT = REPORTS_DIR / "isic_evaluation_summary.csv"
DIVISION_REPORT = REPORTS_DIR / "isic_division_distribution.csv"
CONFIDENCE_REPORT = REPORTS_DIR / "isic_confidence_distribution.csv"
MODEL_STATS_REPORT = REPORTS_DIR / "isic_model_statistics.csv"

# The two production models this evaluation compares. Kept in sync with
# RESUME_ACROSS_MODELS_METHODS in run_isic_classification.py.
ACCEPTED_METHODS = ["openai:gpt-4o-mini", "openai:gpt-4.1-mini"]
MODEL_ERROR_METHOD = "model_error"
TOP_N_DIVISIONS = 20
HISTOGRAM_BUCKET_WIDTH = 0.1
HISTOGRAM_BUCKET_COUNT = 10


def _connect_readonly(db_path: str) -> sqlite3.Connection:
    """Open the database strictly read-only via a SQLite URI, so an accidental
    write anywhere in this script raises rather than silently mutating
    production data."""
    uri = Path(db_path).resolve().as_uri() + "?mode=ro"
    return sqlite3.connect(uri, uri=True)


# ---------------------------------------------------------------------------
# Coverage
# ---------------------------------------------------------------------------

def _total_project_inputs(conn: sqlite3.Connection) -> int:
    return conn.execute(
        "SELECT COUNT(*) FROM classification_inputs WHERE target_type = 'PROJECT'"
    ).fetchone()[0]


def _method_success_counts(conn: sqlite3.Connection) -> dict[str, int]:
    """Successful (non-error) row count for every method present, including
    non-accepted ones (e.g. local-dry-run) for informational context."""
    rows = conn.execute(
        "SELECT method, COUNT(*) FROM project_classifications "
        "WHERE primary_class_code IS NOT NULL GROUP BY method ORDER BY COUNT(*) DESC"
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def _coverage_counts(conn: sqlite3.Connection, methods: list[str]) -> tuple[int, int, int]:
    """(total, completed, remaining) PROJECT inputs, where 'completed' means a
    project_classifications row exists under any of `methods`. Mirrors the
    same COALESCE(project_id, target_id) join used by the classifier's own
    resume filtering, so this evaluation matches what the pipeline considers
    already-done."""
    total = _total_project_inputs(conn)
    placeholders = ", ".join("?" for _ in methods)
    remaining = conn.execute(
        "SELECT COUNT(*) FROM classification_inputs ci "
        "WHERE ci.target_type = 'PROJECT' AND NOT EXISTS ("
        "SELECT 1 FROM project_classifications pc "
        "WHERE pc.project_id = COALESCE(ci.project_id, ci.target_id) "
        f"AND pc.method IN ({placeholders}))",
        methods,
    ).fetchone()[0]
    return total, total - remaining, remaining


# ---------------------------------------------------------------------------
# Confidence statistics
# ---------------------------------------------------------------------------

def _confidence_values(conn: sqlite3.Connection, method: str | None) -> list[float]:
    if method is None:
        placeholders = ", ".join("?" for _ in ACCEPTED_METHODS)
        rows = conn.execute(
            f"SELECT confidence FROM project_classifications "
            f"WHERE method IN ({placeholders}) AND confidence IS NOT NULL",
            ACCEPTED_METHODS,
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT confidence FROM project_classifications "
            "WHERE method = ? AND confidence IS NOT NULL",
            (method,),
        ).fetchall()
    return [r[0] for r in rows]


def _confidence_stats(values: list[float]) -> dict:
    n = len(values)
    if n == 0:
        return {"count": 0, "min": "", "max": "", "mean": "", "median": "", "stddev": "", "q1": "", "q3": ""}
    if n == 1:
        v = round(values[0], 4)
        return {"count": 1, "min": v, "max": v, "mean": v, "median": v, "stddev": 0.0, "q1": v, "q3": v}

    q1, _q2, q3 = statistics.quantiles(values, n=4, method="inclusive")
    return {
        "count": n,
        "min": round(min(values), 4),
        "max": round(max(values), 4),
        "mean": round(statistics.fmean(values), 4),
        "median": round(statistics.median(values), 4),
        "stddev": round(statistics.stdev(values), 4),
        "q1": round(q1, 4),
        "q3": round(q3, 4),
    }


def _confidence_histogram(values: list[float]) -> list[dict]:
    counts = [0] * HISTOGRAM_BUCKET_COUNT
    for v in values:
        idx = int(v / HISTOGRAM_BUCKET_WIDTH)
        idx = max(0, min(idx, HISTOGRAM_BUCKET_COUNT - 1))
        counts[idx] += 1

    total = len(values)
    rows = []
    for i in range(HISTOGRAM_BUCKET_COUNT):
        lo = round(i * HISTOGRAM_BUCKET_WIDTH, 2)
        hi = round((i + 1) * HISTOGRAM_BUCKET_WIDTH, 2)
        closing = "]" if i == HISTOGRAM_BUCKET_COUNT - 1 else ")"
        rows.append({
            "bucket": f"[{lo:.1f}, {hi:.1f}{closing}",
            "count": counts[i],
            "percentage": round(counts[i] / total * 100, 2) if total else 0.0,
        })
    return rows


# ---------------------------------------------------------------------------
# Division distribution
# ---------------------------------------------------------------------------

def _division_distribution(conn: sqlite3.Connection) -> list[dict]:
    """All 87 ISIC divisions with their share of successful classifications
    under the accepted production methods, ranked descending by count (rank 1
    is the row 'Top 20 most common ISIC divisions' should read down from)."""
    divisions = conn.execute("SELECT code, title FROM isic_divisions ORDER BY code").fetchall()
    placeholders = ", ".join("?" for _ in ACCEPTED_METHODS)
    count_rows = conn.execute(
        f"SELECT primary_class_code, COUNT(*) FROM project_classifications "
        f"WHERE method IN ({placeholders}) AND primary_class_code IS NOT NULL "
        f"GROUP BY primary_class_code",
        ACCEPTED_METHODS,
    ).fetchall()
    counts = dict(count_rows)
    total_classified = sum(counts.values())

    rows = []
    for code, title in divisions:
        cnt = counts.get(code, 0)
        rows.append({
            "code": code,
            "title": title,
            "count": cnt,
            "percentage": round(cnt / total_classified * 100, 2) if total_classified else 0.0,
        })
    rows.sort(key=lambda r: (-r["count"], r["code"]))
    for i, r in enumerate(rows, start=1):
        r["rank"] = i
    return rows


# ---------------------------------------------------------------------------
# Cross-model comparison
# ---------------------------------------------------------------------------

def _model_comparison(conn: sqlite3.Connection) -> dict:
    method_a, method_b = ACCEPTED_METHODS
    overlap = conn.execute(
        "SELECT COUNT(*) FROM ("
        "  SELECT project_id FROM project_classifications"
        "  WHERE method IN (?, ?) GROUP BY project_id HAVING COUNT(DISTINCT method) = 2"
        ")",
        (method_a, method_b),
    ).fetchone()[0]

    agree = conn.execute(
        "SELECT COUNT(*) FROM project_classifications a "
        "JOIN project_classifications b ON a.project_id = b.project_id "
        "WHERE a.method = ? AND b.method = ? AND a.primary_class_code = b.primary_class_code",
        (method_a, method_b),
    ).fetchone()[0]

    return {
        "both_models_count": overlap,
        "agreement_count": agree,
        "agreement_percent": round(agree / overlap * 100, 2) if overlap else 0.0,
        "disagreement_count": overlap - agree,
    }


# ---------------------------------------------------------------------------
# Integrity checks
# ---------------------------------------------------------------------------

def _integrity_checks(conn: sqlite3.Connection) -> dict:
    duplicate_rows = conn.execute(
        "SELECT COUNT(*) FROM ("
        "  SELECT project_id, method FROM project_classifications"
        "  GROUP BY project_id, method HAVING COUNT(*) > 1"
        ")"
    ).fetchone()[0]

    invalid_codes = conn.execute(
        "SELECT COUNT(*) FROM project_classifications "
        "WHERE primary_class_code IS NOT NULL "
        "AND primary_class_code NOT IN (SELECT code FROM isic_divisions)"
    ).fetchone()[0]

    orphan_project_ids = conn.execute(
        "SELECT COUNT(*) FROM project_classifications pc "
        "WHERE NOT EXISTS (SELECT 1 FROM combined_projects cp WHERE cp.global_project_id = pc.project_id)"
    ).fetchone()[0]

    return {
        "duplicate_project_method_rows": duplicate_rows,
        "invalid_primary_class_codes": invalid_codes,
        "orphan_project_ids": orphan_project_ids,
    }


# ---------------------------------------------------------------------------
# Report writers
# ---------------------------------------------------------------------------

def _write_summary_report(metrics: list[tuple[str, object]], path: Path = SUMMARY_REPORT) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["metric", "value"])
        w.writerows(metrics)


def _write_division_report(rows: list[dict], path: Path = DIVISION_REPORT) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["rank", "code", "title", "count", "percentage"])
        w.writeheader()
        w.writerows(rows)


def _write_confidence_report(rows_by_method: dict[str, list[dict]], path: Path = CONFIDENCE_REPORT) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["method", "bucket", "count", "percentage"])
        w.writeheader()
        for method, rows in rows_by_method.items():
            for r in rows:
                w.writerow({"method": method, **r})


def _write_model_stats_report(rows: list[dict], path: Path = MODEL_STATS_REPORT) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["method", "count", "coverage_percent", "min", "max", "mean", "median", "stddev", "q1", "q3"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def evaluate(db_path: str, reports_dir: str | Path | None = None) -> dict:
    """reports_dir overrides where the four CSVs are written; the module-level
    defaults (reports/) are used when omitted, so existing callers (this
    script's own CLI) are unaffected."""
    conn = _connect_readonly(db_path)

    reports_path = Path(reports_dir) if reports_dir is not None else REPORTS_DIR
    summary_path = reports_path / "isic_evaluation_summary.csv"
    division_path = reports_path / "isic_division_distribution.csv"
    confidence_path = reports_path / "isic_confidence_distribution.csv"
    model_stats_path = reports_path / "isic_model_statistics.csv"

    total_inputs = _total_project_inputs(conn)
    method_success_counts = _method_success_counts(conn)
    total_covered, covered, remaining = _coverage_counts(conn, ACCEPTED_METHODS)
    model_error_count = conn.execute(
        "SELECT COUNT(*) FROM project_classifications WHERE method = ?", (MODEL_ERROR_METHOD,)
    ).fetchone()[0]

    comparison = _model_comparison(conn)
    checks = _integrity_checks(conn)
    division_rows = _division_distribution(conn)

    confidence_by_method: dict[str, list[dict]] = {}
    model_stats_rows: list[dict] = []
    for method in ACCEPTED_METHODS:
        values = _confidence_values(conn, method)
        confidence_by_method[method] = _confidence_histogram(values)
        stats = _confidence_stats(values)
        model_stats_rows.append({
            "method": method,
            "count": stats["count"],
            "coverage_percent": round(stats["count"] / total_inputs * 100, 2) if total_inputs else 0.0,
            **{k: stats[k] for k in ("min", "max", "mean", "median", "stddev", "q1", "q3")},
        })
    combined_values = _confidence_values(conn, None)
    confidence_by_method["combined"] = _confidence_histogram(combined_values)
    combined_stats = _confidence_stats(combined_values)
    model_stats_rows.append({
        "method": "combined",
        "count": combined_stats["count"],
        "coverage_percent": round(covered / total_inputs * 100, 2) if total_inputs else 0.0,
        **{k: combined_stats[k] for k in ("min", "max", "mean", "median", "stddev", "q1", "q3")},
    })

    other_methods = {
        m: c for m, c in method_success_counts.items()
        if m not in ACCEPTED_METHODS
    }
    other_methods_label = "; ".join(f"{m}={c:,}" for m, c in other_methods.items()) or "none"

    summary_metrics: list[tuple[str, object]] = [
        ("total_project_inputs", total_inputs),
        *[(f"classified_by_{m}", method_success_counts.get(m, 0)) for m in ACCEPTED_METHODS],
        ("other_method_success_counts", other_methods_label),
        ("overall_coverage_count", covered),
        ("overall_coverage_percent", round(covered / total_inputs * 100, 2) if total_inputs else 0.0),
        ("remaining_unclassified", remaining),
        ("model_error_count", model_error_count),
        ("projects_classified_by_both_models", comparison["both_models_count"]),
        ("agreement_count", comparison["agreement_count"]),
        ("agreement_percent", comparison["agreement_percent"]),
        ("disagreement_count", comparison["disagreement_count"]),
        ("duplicate_project_method_rows", checks["duplicate_project_method_rows"]),
        ("invalid_primary_class_codes", checks["invalid_primary_class_codes"]),
        ("orphan_project_ids", checks["orphan_project_ids"]),
    ]

    _write_summary_report(summary_metrics, summary_path)
    _write_division_report(division_rows, division_path)
    _write_confidence_report(confidence_by_method, confidence_path)
    _write_model_stats_report(model_stats_rows, model_stats_path)

    conn.close()

    return {
        "total_inputs": total_inputs,
        "method_success_counts": method_success_counts,
        "covered": covered,
        "remaining": remaining,
        "model_error_count": model_error_count,
        "comparison": comparison,
        "checks": checks,
        "division_rows": division_rows,
        "model_stats_rows": model_stats_rows,
        "report_paths": {
            "summary": summary_path,
            "division": division_path,
            "confidence": confidence_path,
            "model_stats": model_stats_path,
        },
    }


def _print_summary(result: dict) -> None:
    print("=" * 64)
    print("ISIC Classification Evaluation")
    print("=" * 64)

    print(f"Total PROJECT inputs             : {result['total_inputs']:,}")
    for method in ACCEPTED_METHODS:
        print(f"  classified by {method:<22}: {result['method_success_counts'].get(method, 0):,}")
    print(f"Overall coverage (either model)   : {result['covered']:,} "
          f"({round(result['covered'] / result['total_inputs'] * 100, 2) if result['total_inputs'] else 0.0}%)")
    print(f"Remaining unclassified            : {result['remaining']:,}")
    print(f"model_error rows                  : {result['model_error_count']:,}")

    print()
    print("Cross-model comparison:")
    c = result["comparison"]
    print(f"  classified by both models        : {c['both_models_count']:,}")
    print(f"  agreement                        : {c['agreement_count']:,} ({c['agreement_percent']}%)")
    print(f"  disagreement                     : {c['disagreement_count']:,}")

    print()
    print("Integrity checks:")
    checks = result["checks"]
    for name, value in checks.items():
        icon = "PASS" if value == 0 else "FAIL"
        print(f"  [{icon}] {name}: {value}")

    print()
    print(f"Top {TOP_N_DIVISIONS} ISIC divisions (by count, accepted methods combined):")
    for row in result["division_rows"][:TOP_N_DIVISIONS]:
        print(f"  {row['rank']:>2}. {row['code']} {row['title'][:40]:<40} "
              f"{row['count']:>6,} ({row['percentage']}%)")

    print()
    print("Confidence statistics (combined, accepted methods):")
    combined = next(r for r in result["model_stats_rows"] if r["method"] == "combined")
    print(f"  count={combined['count']:,}  min={combined['min']}  max={combined['max']}  "
          f"mean={combined['mean']}  median={combined['median']}  stddev={combined['stddev']}  "
          f"q1={combined['q1']}  q3={combined['q3']}")

    print()
    print("Reports written:")
    for path in (SUMMARY_REPORT, DIVISION_REPORT, CONFIDENCE_REPORT, MODEL_STATS_REPORT):
        print(f"  {path}")
    print("=" * 64)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Evaluate existing ISIC classification results (read-only, no API calls)."
    )
    parser.add_argument("--db", default=DB_DEFAULT)
    args = parser.parse_args()

    if not Path(args.db).exists():
        print(f"ERROR: database not found: {args.db}", file=sys.stderr)
        sys.exit(1)

    result = evaluate(args.db)
    _print_summary(result)


if __name__ == "__main__":
    main()
