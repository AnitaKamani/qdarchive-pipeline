"""
Head-to-head comparison of two OpenAI ISIC classification methods on a fixed,
deterministic sample of projects already classified by --method-a.

Selects --sample-size PROJECT rows that have a successful --method-a result
and no --method-b result yet, stratified by (project_type, gpt4o primary
class) so the sample isn't dominated by one project type or domain. The same
project IDs are then classified with --method-b (never resampled, never
falling back to the normal "next unclassified" query), and the two results
are written side by side for manual review — this script does not decide
which model is "better".

Does not change the classification prompt, schema, or validation logic: it
reuses model_isic_classifier.classify_openai_async exactly as-is.

Usage:
    python phase_2/compare_models.py [options]

Options:
    --db            PATH   default: 23727550-sq26-combined.db
    --method-a      METHOD default: openai:gpt-4o-mini
    --method-b      METHOD default: openai:gpt-4.1-mini
    --sample-size   N      default: 100
    --output        PATH   default: reports/model_comparison.csv
    --seed          N      default: 42

If OPENAI_API_KEY is not set, the sample is still selected and exported to
reports/model_comparison_sample_ids.csv, then the exact command to finish the
comparison is printed instead of calling the API.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import random
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path

_here = Path(__file__).parent
if str(_here) not in sys.path:
    sys.path.insert(0, str(_here))

from tqdm import tqdm

from model_isic_classifier import classify_openai_async

DB_DEFAULT = "23727550-sq26-combined.db"
METHOD_A_DEFAULT = "openai:gpt-4o-mini"
METHOD_B_DEFAULT = "openai:gpt-4.1-mini"
SAMPLE_SIZE_DEFAULT = 100
SEED_DEFAULT = 42
OUTPUT_DEFAULT = "reports/model_comparison.csv"

SAMPLE_IDS_REPORT = "reports/model_comparison_sample_ids.csv"
SUMMARY_REPORT = "reports/model_comparison_summary.csv"
MAX_INPUT_CHARS = 6000
MAX_RETRIES = 5
CONCURRENCY = 1  # fixed: this is a small, controlled comparison run, not a bulk job.


# ---------------------------------------------------------------------------
# Sample selection
# ---------------------------------------------------------------------------

def _load_divisions(conn: sqlite3.Connection) -> tuple[list[dict], dict[str, str], set[str]]:
    rows = conn.execute("SELECT code, title FROM isic_divisions ORDER BY code").fetchall()
    divisions = [{"code": r[0], "title": r[1]} for r in rows]
    titles = {d["code"]: d["title"] for d in divisions}
    valid_codes = {d["code"] for d in divisions}
    return divisions, titles, valid_codes


def _load_eligible_pool(conn: sqlite3.Connection, method_a: str, method_b: str) -> list[dict]:
    """PROJECT rows with a successful method_a result and no method_b result yet."""
    rows = conn.execute(
        """
        SELECT pc.project_id, cp.project_type, cp.title,
               pc.primary_class_code, pc.confidence, pc.reason
        FROM project_classifications pc
        JOIN combined_projects cp ON cp.global_project_id = pc.project_id
        WHERE pc.method = ?
          AND pc.primary_class_code IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM project_classifications pc2
              WHERE pc2.project_id = pc.project_id AND pc2.method = ?
          )
        ORDER BY pc.project_id
        """,
        (method_a, method_b),
    ).fetchall()
    return [
        {
            "project_id": r[0],
            "project_type": r[1],
            "title": r[2],
            "gpt4o_primary_code": r[3],
            "gpt4o_confidence": r[4],
            "gpt4o_reason": r[5],
        }
        for r in rows
    ]


def _select_stratified_sample(pool: list[dict], sample_size: int, seed: int) -> list[dict]:
    """Deterministic round-robin sample across (project_type, primary_class_code)
    groups, so no single project type or class code can dominate the sample.
    Reproducible: fixed seed, groups visited in a stable sorted order, and one
    shuffle per group performed in that same fixed order.
    """
    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in pool:
        groups[(row["project_type"], row["gpt4o_primary_code"])].append(row)

    rng = random.Random(seed)
    ordered_keys = sorted(groups.keys())
    for key in ordered_keys:
        rng.shuffle(groups[key])

    selected: list[dict] = []
    made_progress = True
    while len(selected) < sample_size and made_progress:
        made_progress = False
        for key in ordered_keys:
            if len(selected) >= sample_size:
                break
            bucket = groups[key]
            if bucket:
                selected.append(bucket.pop())
                made_progress = True

    selected.sort(key=lambda r: r["project_id"])
    return selected


def _print_pool_size(pool: list[dict], method_a: str, method_b: str) -> None:
    print(f"Eligible pool: {len(pool):,} projects ({method_a} success, no {method_b} yet)")


def _print_sample_breakdown(sample: list[dict], sample_size: int) -> None:
    print(f"Sample size: {len(sample)} (requested {sample_size})")
    if len(sample) < sample_size:
        print(f"  NOTE: eligible pool was exhausted before reaching {sample_size} at draw time.")
    by_type = Counter(r["project_type"] for r in sample)
    print("By project_type:")
    for ptype, count in sorted(by_type.items()):
        print(f"  {ptype}: {count}")
    by_code = Counter(r["gpt4o_primary_code"] for r in sample)
    print(f"Distinct gpt4o primary codes represented: {len(by_code)}")
    for code, count in sorted(by_code.items(), key=lambda kv: (-kv[1], kv[0])):
        print(f"  {code}: {count}")


def _write_sample_ids_report(sample: list[dict], titles: dict[str, str]) -> None:
    Path("reports").mkdir(parents=True, exist_ok=True)
    with open(SAMPLE_IDS_REPORT, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "project_id", "project_type", "title",
            "gpt4o_primary_code", "gpt4o_primary_title", "gpt4o_confidence", "gpt4o_reason",
        ])
        for row in sample:
            w.writerow([
                row["project_id"], row["project_type"], row["title"],
                row["gpt4o_primary_code"], titles.get(row["gpt4o_primary_code"], ""),
                row["gpt4o_confidence"], row["gpt4o_reason"],
            ])
    print(f"Sample IDs written to {SAMPLE_IDS_REPORT}")


def _load_existing_sample(path: Path) -> list[dict]:
    """Reload a previously-drawn sample so repeated invocations (e.g. after a
    Ctrl+C mid-classification) operate on the exact same project IDs, instead
    of redrawing from an eligible pool that has since shrunk (every project
    method-b classifies gets excluded from future eligible-pool queries)."""
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append({
                "project_id": int(r["project_id"]),
                "project_type": r["project_type"],
                "title": r["title"],
                "gpt4o_primary_code": r["gpt4o_primary_code"],
                "gpt4o_confidence": float(r["gpt4o_confidence"]) if r["gpt4o_confidence"] else None,
                "gpt4o_reason": r["gpt4o_reason"],
            })
    return rows


# ---------------------------------------------------------------------------
# Classification of exactly the sampled project IDs (concurrency fixed at 1)
# ---------------------------------------------------------------------------

def _upsert_project_classification(conn: sqlite3.Connection, project_id: int, result: dict, method: str) -> None:
    tags_json = json.dumps(result.get("tags", []))
    conn.execute(
        "INSERT OR REPLACE INTO project_classifications "
        "(project_id, primary_class_code, secondary_class_code, tags, confidence, method, reason) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            project_id,
            result.get("primary_class_code"),
            result.get("secondary_class_code"),
            tags_json,
            result.get("confidence"),
            method,
            result.get("reason", ""),
        ),
    )


async def _classify_sample(
    conn: sqlite3.Connection,
    project_ids: list[int],
    divisions: list[dict],
    valid_codes: set[str],
    model: str,
    method: str,
    api_key: str,
) -> dict[str, int]:
    import openai  # type: ignore

    placeholders = ",".join("?" for _ in project_ids)
    input_rows = conn.execute(
        f"SELECT project_id, input_text FROM classification_inputs "
        f"WHERE target_type = 'PROJECT' AND project_id IN ({placeholders})",
        project_ids,
    ).fetchall()
    text_by_project = {r[0]: r[1] for r in input_rows}

    missing_input = [pid for pid in project_ids if pid not in text_by_project]
    if missing_input:
        print(f"WARNING: {len(missing_input)} sampled project(s) have no classification_inputs row, "
              f"skipping: {missing_input}", file=sys.stderr)

    already = conn.execute(
        f"SELECT project_id FROM project_classifications WHERE method = ? AND project_id IN ({placeholders})",
        [method] + project_ids,
    ).fetchall()
    already_ids = {r[0] for r in already}
    if already_ids:
        print(f"{len(already_ids)} of the sample already classified by {method} (resume-safe skip).")

    todo = [pid for pid in project_ids if pid in text_by_project and pid not in already_ids]
    print(f"Classifying {len(todo)} project(s) with {method} (concurrency={CONCURRENCY})...")

    counters = {"processed": 0, "inserted": 0, "errors": 0, "retries": 0}
    client = openai.AsyncOpenAI(api_key=api_key, max_retries=0)
    try:
        bar = tqdm(todo, desc=f"Classifying ({method})", unit="proj", dynamic_ncols=True)
        for project_id in bar:
            text = text_by_project[project_id][:MAX_INPUT_CHARS]
            result = await classify_openai_async(
                text, valid_codes, divisions,
                client=client, model=model,
                max_input_chars=MAX_INPUT_CHARS, max_retries=MAX_RETRIES,
            )

            is_error = result.get("primary_class_code") is None
            if is_error:
                if result.get("fatal"):
                    bar.close()
                    conn.commit()
                    print(f"\nFATAL: {result.get('reason', '')}", file=sys.stderr)
                    print("  Aborting — already-committed rows are safe.", file=sys.stderr)
                    await client.close()
                    sys.exit(2)
                counters["errors"] += 1
                _upsert_project_classification(conn, project_id, {**result, "primary_class_code": None}, "model_error")
            else:
                _upsert_project_classification(conn, project_id, result, method)
                counters["inserted"] += 1

            counters["processed"] += 1
            counters["retries"] += result.get("retries", 0)
            bar.set_postfix(
                inserted=counters["inserted"], errors=counters["errors"], retries=counters["retries"], refresh=False,
            )
            # Small, controlled comparison batch: commit after every row for maximum
            # Ctrl+C safety rather than the batched interval used by the bulk runner.
            conn.commit()
        bar.close()
    finally:
        await client.close()

    return counters


# ---------------------------------------------------------------------------
# Comparison report + summary
# ---------------------------------------------------------------------------

def _build_comparison_rows(
    conn: sqlite3.Connection,
    project_ids: list[int],
    method_a: str,
    method_b: str,
    titles: dict[str, str],
) -> list[dict]:
    placeholders = ",".join("?" for _ in project_ids)
    rows = conn.execute(
        f"""
        SELECT cp.global_project_id, cp.project_type, cp.title,
               a.primary_class_code, a.confidence, a.reason,
               b.primary_class_code, b.confidence, b.reason
        FROM combined_projects cp
        LEFT JOIN project_classifications a ON a.project_id = cp.global_project_id AND a.method = ?
        LEFT JOIN project_classifications b ON b.project_id = cp.global_project_id AND b.method = ?
        WHERE cp.global_project_id IN ({placeholders})
        ORDER BY cp.global_project_id
        """,
        [method_a, method_b] + project_ids,
    ).fetchall()

    out = []
    for (pid, ptype, title, a_code, a_conf, a_reason, b_code, b_conf, b_reason) in rows:
        if a_code is not None and b_code is not None:
            agreement = "True" if a_code == b_code else "False"
        else:
            agreement = ""
        out.append({
            "project_id": pid,
            "project_type": ptype,
            "title": title,
            "gpt4o_primary_code": a_code or "",
            "gpt4o_primary_title": titles.get(a_code, "") if a_code else "",
            "gpt4o_confidence": a_conf if a_conf is not None else "",
            "gpt4o_reason": a_reason or "",
            "gpt41_primary_code": b_code or "",
            "gpt41_primary_title": titles.get(b_code, "") if b_code else "",
            "gpt41_confidence": b_conf if b_conf is not None else "",
            "gpt41_reason": b_reason or "",
            "exact_code_agreement": agreement,
            "manual_correct_model": "",
            "manual_notes": "",
        })
    return out


def _write_comparison_report(rows: list[dict], output_path: str) -> None:
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "project_id", "project_type", "title",
        "gpt4o_primary_code", "gpt4o_primary_title", "gpt4o_confidence", "gpt4o_reason",
        "gpt41_primary_code", "gpt41_primary_title", "gpt41_confidence", "gpt41_reason",
        "exact_code_agreement", "manual_correct_model", "manual_notes",
    ]
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"Comparison report written to {output_path}")


def _compute_summary(rows: list[dict]) -> dict:
    sample_size = len(rows)
    both = [r for r in rows if r["gpt4o_primary_code"] and r["gpt41_primary_code"]]
    both_count = len(both)
    agreements = [r for r in both if r["gpt4o_primary_code"] == r["gpt41_primary_code"]]
    agreement_count = len(agreements)
    agreement_percent = round((agreement_count / both_count * 100), 2) if both_count else 0.0
    disagreements = [r for r in both if r["gpt4o_primary_code"] != r["gpt41_primary_code"]]

    a_confidences = [r["gpt4o_confidence"] for r in rows if isinstance(r["gpt4o_confidence"], (int, float))]
    b_confidences = [r["gpt41_confidence"] for r in rows if isinstance(r["gpt41_confidence"], (int, float))]
    avg_a = round(sum(a_confidences) / len(a_confidences), 4) if a_confidences else ""
    avg_b = round(sum(b_confidences) / len(b_confidences), 4) if b_confidences else ""

    pair_counts = Counter((r["gpt4o_primary_code"], r["gpt41_primary_code"]) for r in disagreements)
    top_pairs = pair_counts.most_common(10)

    return {
        "sample_size": sample_size,
        "both_models_available": both_count,
        "exact_code_agreement_count": agreement_count,
        "exact_code_agreement_percent": agreement_percent,
        "average_confidence_gpt4o": avg_a,
        "average_confidence_gpt41": avg_b,
        "disagreement_count": len(disagreements),
        "top_disagreement_pairs": top_pairs,
    }


def _write_summary_report(summary: dict) -> None:
    Path("reports").mkdir(parents=True, exist_ok=True)
    with open(SUMMARY_REPORT, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["metric", "value"])
        for key in (
            "sample_size", "both_models_available",
            "exact_code_agreement_count", "exact_code_agreement_percent",
            "average_confidence_gpt4o", "average_confidence_gpt41",
            "disagreement_count",
        ):
            w.writerow([key, summary[key]])
        w.writerow([])
        w.writerow(["rank", "gpt4o_code", "gpt41_code", "count"])
        for i, ((a_code, b_code), count) in enumerate(summary["top_disagreement_pairs"], start=1):
            w.writerow([i, a_code, b_code, count])
    print(f"Summary written to {SUMMARY_REPORT}")


def _print_summary(summary: dict) -> None:
    print(f"Both models available: {summary['both_models_available']}/{summary['sample_size']}")
    print(f"Exact agreement: {summary['exact_code_agreement_count']}/{summary['both_models_available']} "
          f"({summary['exact_code_agreement_percent']}%)")
    print(f"Disagreements: {summary['disagreement_count']}")
    print(f"Average confidence — gpt4o: {summary['average_confidence_gpt4o']}, "
          f"gpt41: {summary['average_confidence_gpt41']}")
    print("Top disagreement pairs (gpt4o -> gpt41: count):")
    if not summary["top_disagreement_pairs"]:
        print("  none")
    for (a_code, b_code), count in summary["top_disagreement_pairs"]:
        print(f"  {a_code} -> {b_code}: {count}")


def _print_first_rows(rows: list[dict], n: int = 20) -> None:
    print(f"First {min(n, len(rows))} comparison rows:")
    header = f"{'project_id':<10} {'type':<12} {'gpt4o':<6} {'gpt41':<6} {'agree':<6}"
    print(header)
    print("-" * len(header))
    for row in rows[:n]:
        print(
            f"{row['project_id']:<10} {row['project_type']:<12} "
            f"{row['gpt4o_primary_code']:<6} {row['gpt41_primary_code']:<6} {row['exact_code_agreement']:<6}"
        )


# ---------------------------------------------------------------------------
# SQL-safe checks
# ---------------------------------------------------------------------------

def _run_sql_checks(
    conn: sqlite3.Connection,
    project_ids: list[int],
    method_a: str,
    method_b: str,
    sample_size: int,
) -> list[dict]:
    checks = []

    def add(name: str, passed: bool, detail: str = "") -> None:
        checks.append({"check": name, "status": "PASS" if passed else "FAIL", "detail": detail})

    placeholders = ",".join("?" for _ in project_ids)

    sampled_a = conn.execute(
        f"SELECT COUNT(*) FROM project_classifications "
        f"WHERE method = ? AND project_id IN ({placeholders})",
        [method_a] + project_ids,
    ).fetchone()[0]
    add(
        f"exactly {sample_size} {method_a} rows were sampled",
        sampled_a == len(project_ids) == sample_size,
        f"{sampled_a} rows found for {len(project_ids)} sampled project_ids (requested {sample_size})",
    )

    sampled_b = conn.execute(
        f"SELECT COUNT(*) FROM project_classifications "
        f"WHERE method = ? AND project_id IN ({placeholders})",
        [method_b] + project_ids,
    ).fetchone()[0]
    add(
        f"same project IDs received {method_b} results",
        sampled_b == len(project_ids),
        f"{sampled_b}/{len(project_ids)} sampled project_ids have a {method_b} row "
        f"(remainder may be model_error — see errors report)",
    )

    dupes = conn.execute(
        f"SELECT COUNT(*) FROM ("
        f"  SELECT project_id, method FROM project_classifications "
        f"  WHERE project_id IN ({placeholders}) AND method IN (?, ?) "
        f"  GROUP BY project_id, method HAVING COUNT(*) > 1"
        f")",
        project_ids + [method_a, method_b],
    ).fetchone()[0]
    add("no duplicate (project_id, method) rows", dupes == 0, f"{dupes} duplicate pairs")

    invalid_codes = conn.execute(
        f"SELECT COUNT(*) FROM project_classifications "
        f"WHERE project_id IN ({placeholders}) AND method IN (?, ?) "
        f"AND primary_class_code IS NOT NULL "
        f"AND primary_class_code NOT IN (SELECT code FROM isic_divisions)",
        project_ids + [method_a, method_b],
    ).fetchone()[0]
    add("no invalid ISIC codes among sampled rows", invalid_codes == 0, f"{invalid_codes} invalid codes")

    orphan_count = sum(
        1 for pid in project_ids
        if conn.execute("SELECT 1 FROM combined_projects WHERE global_project_id = ?", (pid,)).fetchone() is None
    )
    add("no orphan project IDs", orphan_count == 0, f"{orphan_count} sampled project_ids missing from combined_projects")

    return checks


def _print_checks(checks: list[dict]) -> bool:
    print("SQL-safe checks:")
    all_pass = True
    for c in checks:
        print(f"  [{c['status']}] {c['check']} ({c['detail']})")
        if c["status"] != "PASS":
            all_pass = False
    return all_pass


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Compare two OpenAI ISIC classification methods on the same sample.")
    parser.add_argument("--db", default=DB_DEFAULT)
    parser.add_argument("--method-a", default=METHOD_A_DEFAULT)
    parser.add_argument("--method-b", default=METHOD_B_DEFAULT)
    parser.add_argument("--sample-size", type=int, default=SAMPLE_SIZE_DEFAULT)
    parser.add_argument("--output", default=OUTPUT_DEFAULT)
    parser.add_argument("--seed", type=int, default=SEED_DEFAULT)
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA journal_mode = WAL")

    divisions, titles, valid_codes = _load_divisions(conn)

    print("=" * 64)
    print("Step 1: Selecting comparison sample")
    print("=" * 64)
    pool = _load_eligible_pool(conn, args.method_a, args.method_b)
    _print_pool_size(pool, args.method_a, args.method_b)

    sample_ids_path = Path(SAMPLE_IDS_REPORT)
    if sample_ids_path.exists():
        sample = _load_existing_sample(sample_ids_path)
        print(f"Reusing existing sample from {SAMPLE_IDS_REPORT} — the sample is fixed once drawn so that a "
              f"re-run (e.g. after an interruption) finishes the same 100 rather than drawing new ones.")
        print("  Delete that file first if you want a fresh draw with a new seed/sample-size.")
    else:
        sample = _select_stratified_sample(pool, args.sample_size, args.seed)
        _write_sample_ids_report(sample, titles)
    _print_sample_breakdown(sample, args.sample_size)

    if not sample:
        print("No eligible projects to sample — nothing to compare.", file=sys.stderr)
        conn.close()
        sys.exit(1)

    project_ids = [row["project_id"] for row in sample]

    import os
    api_key = os.environ.get("OPENAI_API_KEY", "")
    provider_b, model_b = args.method_b.split(":", 1) if ":" in args.method_b else ("openai", args.method_b)
    if provider_b != "openai":
        print(f"ERROR: --method-b '{args.method_b}' is not an openai:<model> method; "
              f"this comparison runner only supports the openai provider.", file=sys.stderr)
        conn.close()
        sys.exit(1)

    if not api_key:
        print()
        print("=" * 64)
        print("OPENAI_API_KEY is not set — sample prepared, classification not run.")
        print("=" * 64)
        print("Run this exact command once the key is available to finish the comparison:")
        print(
            f"  python phase_2/compare_models.py --db {args.db} "
            f"--method-a {args.method_a} --method-b {args.method_b} "
            f"--sample-size {args.sample_size} --output {args.output} --seed {args.seed}"
        )
        conn.close()
        return

    print()
    print("=" * 64)
    print(f"Step 2: Classifying sample with {args.method_b}")
    print("=" * 64)
    counters = asyncio.run(
        _classify_sample(conn, project_ids, divisions, valid_codes, model_b, args.method_b, api_key)
    )
    print(f"Classification counters: {counters}")

    print()
    print("=" * 64)
    print("Step 3: Building comparison report")
    print("=" * 64)
    rows = _build_comparison_rows(conn, project_ids, args.method_a, args.method_b, titles)
    _write_comparison_report(rows, args.output)

    print()
    print("=" * 64)
    print("Step 4: Summary")
    print("=" * 64)
    summary = _compute_summary(rows)
    _write_summary_report(summary)
    _print_summary(summary)

    print()
    _print_first_rows(rows, 20)

    print()
    print("=" * 64)
    print("Step 5: SQL-safe checks")
    print("=" * 64)
    checks = _run_sql_checks(conn, project_ids, args.method_a, args.method_b, len(sample))
    all_pass = _print_checks(checks)

    conn.close()

    if not all_pass:
        sys.exit(1)


if __name__ == "__main__":
    main()
