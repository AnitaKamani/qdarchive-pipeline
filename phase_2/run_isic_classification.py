"""
Run model-based ISIC Rev. 5 classification on PROJECT inputs.

Reads classification_inputs (target_type='PROJECT'), classifies each via the
chosen provider, and writes results to project_classifications.

Usage:
    python phase_2/run_isic_classification.py [options]

Options:
    --db            PATH          default: 23727550-sq26-combined.db
    --provider      PROV          local-dry-run | openai  (default: local-dry-run)
    --model         MODEL         OpenAI model  (default: gpt-4o-mini)
    --target-type   TYPE          PROJECT (only supported value for now)
    --limit         N             process at most N rows
    --offset        N             skip first N rows
    --overwrite                   re-classify already-classified projects
    --sleep         SECS          pause between API calls (default: 0)
    --max-input-chars N           truncate input_text to N chars (default: 6000)
"""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
import time
from pathlib import Path

_here = Path(__file__).parent
if str(_here) not in sys.path:
    sys.path.insert(0, str(_here))

from model_isic_classifier import classify, Result

DB_DEFAULT = "23727550-sq26-combined.db"
SUMMARY_REPORT = "reports/isic_classification_summary.csv"
ERRORS_REPORT = "reports/isic_classification_errors.csv"
PROGRESS_INTERVAL = 50


def _load_divisions(conn: sqlite3.Connection) -> tuple[list[dict], set[str]]:
    rows = conn.execute("SELECT code, title FROM isic_divisions ORDER BY code").fetchall()
    divisions = [{"code": r[0], "title": r[1]} for r in rows]
    valid_codes = {d["code"] for d in divisions}
    return divisions, valid_codes


def _load_inputs(
    conn: sqlite3.Connection,
    target_type: str,
    limit: int | None,
    offset: int | None,
) -> list[dict]:
    sql = (
        "SELECT ci.id, ci.target_id, ci.project_id, ci.input_text "
        "FROM classification_inputs ci "
        "WHERE ci.target_type = ?"
    )
    params: list = [target_type]

    if offset:
        sql += " LIMIT -1 OFFSET ?"
        params.append(offset)
    if limit is not None:
        if offset:
            sql = sql.replace("LIMIT -1", f"LIMIT {limit}")
        else:
            sql += f" LIMIT {limit}"

    rows = conn.execute(sql, params).fetchall()
    return [{"id": r[0], "target_id": r[1], "project_id": r[2], "input_text": r[3]} for r in rows]


def _already_classified(conn: sqlite3.Connection, target_type: str, method: str) -> set[int]:
    if target_type == "PROJECT":
        rows = conn.execute(
            "SELECT project_id FROM project_classifications WHERE method = ?", (method,)
        ).fetchall()
        return {r[0] for r in rows}
    return set()


def _upsert_project_classification(
    conn: sqlite3.Connection,
    project_id: int,
    result: Result,
    method: str,
) -> None:
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


def _write_summary(conn: sqlite3.Connection, method: str) -> None:
    Path("reports").mkdir(parents=True, exist_ok=True)
    rows = conn.execute(
        "SELECT pc.primary_class_code, id.title, COUNT(*) AS cnt "
        "FROM project_classifications pc "
        "LEFT JOIN isic_divisions id ON id.code = pc.primary_class_code "
        "WHERE pc.method = ? "
        "GROUP BY pc.primary_class_code ORDER BY cnt DESC",
        (method,),
    ).fetchall()
    with open(SUMMARY_REPORT, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["method", "primary_class_code", "class_title", "count"])
        for r in rows:
            w.writerow([method, r[0], r[1] or "", r[2]])


def run(
    db_path: str,
    provider: str,
    model: str,
    target_type: str,
    limit: int | None,
    offset: int | None,
    overwrite: bool,
    sleep_secs: float,
    max_input_chars: int,
    api_key: str | None = None,
) -> dict[str, int]:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode = WAL")

    print("Loading ISIC divisions...", flush=True)
    divisions, valid_codes = _load_divisions(conn)
    print(f"  {len(valid_codes)} divisions loaded.", flush=True)

    print(f"Loading {target_type} classification inputs...", flush=True)
    inputs = _load_inputs(conn, target_type, limit, offset)
    print(f"  {len(inputs):,} inputs loaded.", flush=True)

    method = provider
    already_done: set[int] = set()
    if not overwrite:
        already_done = _already_classified(conn, target_type, method)
        print(f"  {len(already_done):,} already classified (will skip).", flush=True)

    counters = {"processed": 0, "inserted": 0, "skipped_existing": 0, "errors": 0}
    error_rows: list[dict] = []

    for i, row in enumerate(inputs):
        project_id = row["project_id"] or row["target_id"]

        if project_id in already_done:
            counters["skipped_existing"] += 1
            continue

        text = row["input_text"][:max_input_chars]

        result = classify(
            provider=provider,
            input_text=text,
            valid_codes=valid_codes,
            divisions=divisions,
            model=model,
            api_key=api_key,
            max_input_chars=max_input_chars,
        )

        is_error = result.get("primary_class_code") is None
        if is_error:
            counters["errors"] += 1
            error_rows.append({
                "input_id": row["id"],
                "project_id": project_id,
                "reason": result.get("reason", ""),
            })
            result_to_store = {**result, "primary_class_code": None}
            _upsert_project_classification(conn, project_id, result_to_store, "model_error")
        else:
            _upsert_project_classification(conn, project_id, result, method)
            counters["inserted"] += 1

        counters["processed"] += 1

        if counters["processed"] % PROGRESS_INTERVAL == 0:
            conn.commit()
            print(
                f"  [{counters['processed']:,}/{len(inputs):,}] "
                f"inserted={counters['inserted']:,} errors={counters['errors']:,}",
                flush=True,
            )

        if sleep_secs > 0:
            time.sleep(sleep_secs)

    conn.commit()

    _write_summary(conn, method)

    if error_rows:
        Path("reports").mkdir(parents=True, exist_ok=True)
        with open(ERRORS_REPORT, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["input_id", "project_id", "reason"])
            w.writeheader()
            w.writerows(error_rows)
        print(f"  Errors written to {ERRORS_REPORT}", flush=True)

    conn.close()
    return counters


def main() -> None:
    parser = argparse.ArgumentParser(description="Run ISIC classification on PROJECT inputs.")
    parser.add_argument("--db", default=DB_DEFAULT)
    parser.add_argument("--provider", choices=["local-dry-run", "openai"], default="local-dry-run")
    parser.add_argument("--model", default="gpt-4o-mini")
    parser.add_argument("--target-type", choices=["PROJECT"], default="PROJECT")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--offset", type=int, default=None)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--sleep", type=float, default=0.0)
    parser.add_argument("--max-input-chars", type=int, default=6000)
    args = parser.parse_args()

    import os
    api_key = os.environ.get("OPENAI_API_KEY") if args.provider == "openai" else None

    print("=" * 64)
    print("ISIC Classification Run")
    print("=" * 64)
    print(f"  provider  : {args.provider}")
    if args.provider == "openai":
        print(f"  model     : {args.model}")
    print(f"  db        : {args.db}")
    print(f"  limit     : {args.limit or 'all'}")
    print(f"  overwrite : {args.overwrite}")
    print()

    counters = run(
        db_path=args.db,
        provider=args.provider,
        model=args.model,
        target_type=args.target_type,
        limit=args.limit,
        offset=args.offset,
        overwrite=args.overwrite,
        sleep_secs=args.sleep,
        max_input_chars=args.max_input_chars,
        api_key=api_key,
    )

    print()
    print("=" * 64)
    print("Summary")
    print("=" * 64)
    for k, v in counters.items():
        print(f"  {k:<20}: {v:,}")
    print(f"  report              : {SUMMARY_REPORT}")
    print("=" * 64)

    if counters["errors"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
