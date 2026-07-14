"""
Run model-based ISIC Rev. 5 classification on PROJECT inputs.

Reads classification_inputs (target_type='PROJECT'), classifies each via the
chosen provider, and writes results to project_classifications.

The openai provider runs requests concurrently (bounded by --concurrency)
using asyncio and the OpenAI async client; local-dry-run runs sequentially
since it makes no network calls. Database writes always happen from a single
coroutine in the main thread, never from concurrent tasks.

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
    --sleep         SECS          pause between API calls (local-dry-run / sequential only)
    --max-input-chars N           truncate input_text to N chars (default: 6000)
    --concurrency   N             concurrent in-flight OpenAI requests (default: 5)
    --max-retries   N             max retries per request on transient errors (default: 5)

    --adaptive-concurrency        adjust concurrency between windows based on recent
                                   retry/error rates (openai provider only)
    --min-concurrency N           adaptive mode floor (default: 1)
    --max-concurrency N           adaptive mode ceiling (default: 8)
    --adjustment-window N         requests per adaptation window (default: 100)
    --increase-threshold F        retry_rate at/below which concurrency may increase (default: 0.01)
    --decrease-threshold F        retry_rate at/above which concurrency decreases (default: 0.05)
    --increase-step N             concurrency added per increase (default: 1)
    --decrease-factor F           multiplier applied on decrease, floored (default: 0.5)
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

_here = Path(__file__).parent
if str(_here) not in sys.path:
    sys.path.insert(0, str(_here))

from tqdm import tqdm

from adaptive_concurrency import AdaptiveConcurrencyController
from model_isic_classifier import classify, classify_openai_async, Result

DB_DEFAULT = "23727550-sq26-combined.db"
SUMMARY_REPORT = "reports/isic_classification_summary.csv"
ERRORS_REPORT = "reports/isic_classification_errors.csv"
CONCURRENCY_HISTORY_REPORT = "reports/isic_concurrency_history.csv"
PROGRESS_INTERVAL = 50
DEFAULT_CONCURRENCY = 5
DEFAULT_MAX_RETRIES = 5
DEFAULT_MIN_CONCURRENCY = 1
DEFAULT_MAX_CONCURRENCY = 8
DEFAULT_ADJUSTMENT_WINDOW = 100
DEFAULT_INCREASE_THRESHOLD = 0.01
DEFAULT_DECREASE_THRESHOLD = 0.05
DEFAULT_INCREASE_STEP = 1
DEFAULT_DECREASE_FACTOR = 0.5


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
    exclude_method: str | None = None,
) -> list[dict]:
    params: list = [target_type]
    sql = (
        "SELECT ci.id, ci.target_id, ci.project_id, ci.input_text "
        "FROM classification_inputs ci "
        "WHERE ci.target_type = ?"
    )
    if exclude_method is not None:
        sql += (
            " AND NOT EXISTS ("
            "SELECT 1 FROM project_classifications pc "
            "WHERE pc.project_id = COALESCE(ci.project_id, ci.target_id) "
            "AND pc.method = ?)"
        )
        params.append(exclude_method)
    sql += " ORDER BY ci.id"
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


def _write_errors_report(error_rows: list[dict]) -> None:
    Path("reports").mkdir(parents=True, exist_ok=True)
    with open(ERRORS_REPORT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["input_id", "project_id", "reason", "raw_model_output"])
        w.writeheader()
        w.writerows(error_rows)
    print(f"  Errors written to {ERRORS_REPORT}", flush=True)


_CONCURRENCY_HISTORY_HEADER = [
    "run_id", "timestamp", "window_number", "window_size",
    "previous_concurrency", "new_concurrency", "completed",
    "inserted", "errors", "requests_retried", "retry_events",
    "retry_rate", "error_rate", "adjustment_reason",
]


def _append_concurrency_history(
    run_id: str,
    decision,
    window_size: int,
    inserted: int,
) -> None:
    """Append one row per adaptation window. Appends across runs (tagged by
    run_id) rather than overwriting, so prior run history is preserved."""
    Path("reports").mkdir(parents=True, exist_ok=True)
    report_path = Path(CONCURRENCY_HISTORY_REPORT)
    is_new = not report_path.exists()
    with open(report_path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if is_new:
            w.writerow(_CONCURRENCY_HISTORY_HEADER)
        w.writerow([
            run_id,
            datetime.now(timezone.utc).isoformat(timespec="seconds"),
            decision.window_number,
            window_size,
            decision.previous_concurrency,
            decision.new_concurrency,
            decision.completed,
            inserted,
            decision.errors,
            decision.requests_retried,
            decision.retry_events,
            f"{decision.retry_rate:.4f}",
            f"{decision.error_rate:.4f}",
            decision.reason,
        ])


def _run_sequential(
    conn: sqlite3.Connection,
    inputs: list[dict],
    provider: str,
    model: str,
    method: str,
    max_input_chars: int,
    api_key: str | None,
    sleep_secs: float,
    divisions: list[dict],
    valid_codes: set[str],
) -> dict[str, int]:
    counters = {"processed": 0, "inserted": 0, "errors": 0, "retries": 0}
    error_rows: list[dict] = []

    bar = tqdm(inputs, desc="Classifying", unit="proj", dynamic_ncols=True)
    for row in bar:
        project_id = row["project_id"] or row["target_id"]
        bar.set_postfix(inserted=counters["inserted"], errors=counters["errors"], pid=project_id, refresh=False)

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
            if result.get("fatal"):
                bar.close()
                conn.commit()
                print(f"\nFATAL: {result.get('reason', '')}", file=sys.stderr)
                print("  Aborting — no rows will be written for this error.", file=sys.stderr)
                sys.exit(2)
            counters["errors"] += 1
            error_rows.append({
                "input_id": row["id"],
                "project_id": project_id,
                "reason": result.get("reason", ""),
                "raw_model_output": (result.get("raw_model_output") or "")[:1000],
            })
            result_to_store = {**result, "primary_class_code": None}
            _upsert_project_classification(conn, project_id, result_to_store, "model_error")
        else:
            _upsert_project_classification(conn, project_id, result, method)
            counters["inserted"] += 1

        counters["processed"] += 1
        bar.set_postfix(inserted=counters["inserted"], errors=counters["errors"], pid=project_id)

        if counters["processed"] % PROGRESS_INTERVAL == 0:
            conn.commit()

        if sleep_secs > 0:
            time.sleep(sleep_secs)

    conn.commit()
    bar.close()

    if error_rows:
        _write_errors_report(error_rows)

    return counters


def _run_openai_concurrent(
    conn: sqlite3.Connection,
    inputs: list[dict],
    divisions: list[dict],
    valid_codes: set[str],
    model: str,
    method: str,
    max_input_chars: int,
    api_key: str | None,
    concurrency: int,
    max_retries: int,
) -> dict[str, int]:
    try:
        import openai  # type: ignore
    except ImportError:
        print("ERROR: openai package not installed; run: pip install openai", file=sys.stderr)
        sys.exit(1)

    counters = {"processed": 0, "inserted": 0, "errors": 0, "retries": 0}
    error_rows: list[dict] = []
    abort: dict[str, str | None] = {"reason": None}

    async def _drive() -> None:
        # max_retries=0 on the client: our own retry loop in classify_openai_async
        # owns backoff/jitter, so the SDK's built-in retries would double up otherwise.
        client = openai.AsyncOpenAI(api_key=api_key, max_retries=0)
        sem = asyncio.Semaphore(concurrency)
        queue: asyncio.Queue = asyncio.Queue()

        async def worker(row: dict) -> None:
            if abort["reason"] is not None:
                await queue.put((row, None))
                return
            async with sem:
                if abort["reason"] is not None:
                    await queue.put((row, None))
                    return
                text = row["input_text"][:max_input_chars]
                result = await classify_openai_async(
                    text,
                    valid_codes,
                    divisions,
                    client=client,
                    model=model,
                    max_input_chars=max_input_chars,
                    max_retries=max_retries,
                )
                await queue.put((row, result))

        worker_tasks = [asyncio.create_task(worker(row)) for row in inputs]

        bar = tqdm(total=len(inputs), desc="Classifying", unit="proj", dynamic_ncols=True)
        try:
            completed = 0
            while completed < len(inputs):
                row, result = await queue.get()
                completed += 1

                if result is None:
                    # Skipped: an abort was already in progress when this task started.
                    bar.update(1)
                    continue

                if result.get("fatal"):
                    if abort["reason"] is None:
                        abort["reason"] = result.get("reason", "")
                    bar.update(1)
                    continue

                project_id = row["project_id"] or row["target_id"]
                is_error = result.get("primary_class_code") is None
                if is_error:
                    counters["errors"] += 1
                    error_rows.append({
                        "input_id": row["id"],
                        "project_id": project_id,
                        "reason": result.get("reason", ""),
                        "raw_model_output": (result.get("raw_model_output") or "")[:1000],
                    })
                    result_to_store = {**result, "primary_class_code": None}
                    _upsert_project_classification(conn, project_id, result_to_store, "model_error")
                else:
                    _upsert_project_classification(conn, project_id, result, method)
                    counters["inserted"] += 1

                counters["processed"] += 1
                counters["retries"] += result.get("retries", 0)
                bar.set_postfix(
                    inserted=counters["inserted"],
                    errors=counters["errors"],
                    retries=counters["retries"],
                    refresh=False,
                )
                bar.update(1)

                if counters["processed"] % PROGRESS_INTERVAL == 0:
                    conn.commit()
        finally:
            conn.commit()
            bar.close()
            await asyncio.gather(*worker_tasks, return_exceptions=True)
            await client.close()

        if abort["reason"] is not None:
            print(f"\nFATAL: {abort['reason']}", file=sys.stderr)
            print("  Aborting — remaining rows were not processed.", file=sys.stderr)
            sys.exit(2)

    try:
        asyncio.run(_drive())
    except KeyboardInterrupt:
        conn.commit()
        print(
            "\nInterrupted — already-committed rows are safe. Re-run the same command to resume.",
            file=sys.stderr,
        )
        sys.exit(130)

    if error_rows:
        _write_errors_report(error_rows)

    return counters


def _run_openai_adaptive(
    conn: sqlite3.Connection,
    inputs: list[dict],
    divisions: list[dict],
    valid_codes: set[str],
    model: str,
    method: str,
    max_input_chars: int,
    api_key: str | None,
    start_concurrency: int,
    max_retries: int,
    min_concurrency: int,
    max_concurrency: int,
    adjustment_window: int,
    increase_threshold: float,
    decrease_threshold: float,
    increase_step: int,
    decrease_factor: float,
) -> dict[str, int]:
    """Same engine as _run_openai_concurrent, but processes inputs in fixed-size
    windows and re-derives concurrency between windows from recent retry/error
    rates via AdaptiveConcurrencyController. A single coroutine still owns all
    SQLite writes; concurrency only ever changes at a window boundary (no live
    semaphore mutation), so there is nothing to reconcile mid-window.
    """
    try:
        import openai  # type: ignore
    except ImportError:
        print("ERROR: openai package not installed; run: pip install openai", file=sys.stderr)
        sys.exit(1)

    controller = AdaptiveConcurrencyController(
        start_concurrency=start_concurrency,
        min_concurrency=min_concurrency,
        max_concurrency=max_concurrency,
        increase_threshold=increase_threshold,
        decrease_threshold=decrease_threshold,
        increase_step=increase_step,
        decrease_factor=decrease_factor,
    )
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    counters = {"processed": 0, "inserted": 0, "errors": 0, "retries": 0, "requests_retried": 0}
    error_rows: list[dict] = []
    abort: dict[str, str | None] = {"reason": None}

    async def _drive() -> None:
        client = openai.AsyncOpenAI(api_key=api_key, max_retries=0)
        bar = tqdm(total=len(inputs), desc="Classifying", unit="proj", dynamic_ncols=True)
        idx = 0

        try:
            while idx < len(inputs) and abort["reason"] is None:
                window_inputs = inputs[idx: idx + adjustment_window]
                current_concurrency = controller.concurrency
                sem = asyncio.Semaphore(current_concurrency)
                queue: asyncio.Queue = asyncio.Queue()

                async def worker(row: dict) -> None:
                    if abort["reason"] is not None:
                        await queue.put((row, None))
                        return
                    async with sem:
                        if abort["reason"] is not None:
                            await queue.put((row, None))
                            return
                        text = row["input_text"][:max_input_chars]
                        result = await classify_openai_async(
                            text,
                            valid_codes,
                            divisions,
                            client=client,
                            model=model,
                            max_input_chars=max_input_chars,
                            max_retries=max_retries,
                        )
                        await queue.put((row, result))

                worker_tasks = [asyncio.create_task(worker(row)) for row in window_inputs]

                drained = 0
                window_completed = 0
                window_inserted = 0
                window_errors = 0
                window_requests_retried = 0
                window_retry_events = 0

                while drained < len(window_inputs):
                    row, result = await queue.get()
                    drained += 1

                    if result is None:
                        # Skipped: an abort was already in progress when this task started.
                        bar.update(1)
                        continue

                    if result.get("fatal"):
                        if abort["reason"] is None:
                            abort["reason"] = result.get("reason", "")
                        bar.update(1)
                        continue

                    project_id = row["project_id"] or row["target_id"]
                    is_error = result.get("primary_class_code") is None
                    retries = result.get("retries", 0)

                    if is_error:
                        counters["errors"] += 1
                        window_errors += 1
                        error_rows.append({
                            "input_id": row["id"],
                            "project_id": project_id,
                            "reason": result.get("reason", ""),
                            "raw_model_output": (result.get("raw_model_output") or "")[:1000],
                        })
                        result_to_store = {**result, "primary_class_code": None}
                        _upsert_project_classification(conn, project_id, result_to_store, "model_error")
                    else:
                        _upsert_project_classification(conn, project_id, result, method)
                        counters["inserted"] += 1
                        window_inserted += 1

                    if retries > 0:
                        counters["requests_retried"] += 1
                        window_requests_retried += 1
                    counters["retries"] += retries
                    window_retry_events += retries
                    window_completed += 1

                    counters["processed"] += 1
                    bar.set_postfix(
                        inserted=counters["inserted"],
                        errors=counters["errors"],
                        retry_events=counters["retries"],
                        conc=current_concurrency,
                        retry_rate=f"{(window_requests_retried / window_completed):.2f}",
                        err_rate=f"{(window_errors / window_completed):.2f}",
                        refresh=False,
                    )
                    bar.update(1)

                    if counters["processed"] % PROGRESS_INTERVAL == 0:
                        conn.commit()

                # Window boundary: commit regardless of the 50-row interval, then adapt.
                conn.commit()
                await asyncio.gather(*worker_tasks, return_exceptions=True)

                if abort["reason"] is None and window_completed > 0:
                    decision = controller.decide(
                        completed=window_completed,
                        requests_retried=window_requests_retried,
                        retry_events=window_retry_events,
                        errors=window_errors,
                    )
                    if decision.changed:
                        tqdm.write(
                            f"concurrency {decision.previous_concurrency} -> {decision.new_concurrency}: "
                            f"retry_rate={decision.retry_rate:.3f}, error_rate={decision.error_rate:.3f}"
                        )
                    _append_concurrency_history(run_id, decision, len(window_inputs), window_inserted)

                idx += len(window_inputs)
        finally:
            conn.commit()
            bar.close()
            await client.close()

        if abort["reason"] is not None:
            print(f"\nFATAL: {abort['reason']}", file=sys.stderr)
            print("  Aborting — remaining rows were not processed.", file=sys.stderr)
            sys.exit(2)

    try:
        asyncio.run(_drive())
    except KeyboardInterrupt:
        conn.commit()
        print(
            "\nInterrupted — already-committed rows are safe. Re-run the same command to resume.",
            file=sys.stderr,
        )
        sys.exit(130)

    if error_rows:
        _write_errors_report(error_rows)

    return counters


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
    concurrency: int = DEFAULT_CONCURRENCY,
    max_retries: int = DEFAULT_MAX_RETRIES,
    adaptive_concurrency: bool = False,
    min_concurrency: int = DEFAULT_MIN_CONCURRENCY,
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
    adjustment_window: int = DEFAULT_ADJUSTMENT_WINDOW,
    increase_threshold: float = DEFAULT_INCREASE_THRESHOLD,
    decrease_threshold: float = DEFAULT_DECREASE_THRESHOLD,
    increase_step: int = DEFAULT_INCREASE_STEP,
    decrease_factor: float = DEFAULT_DECREASE_FACTOR,
) -> dict[str, int]:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode = WAL")

    print("Loading ISIC divisions...", flush=True)
    divisions, valid_codes = _load_divisions(conn)
    print(f"  {len(valid_codes)} divisions loaded.", flush=True)

    method = f"{provider}:{model}" if provider == "openai" else provider

    print(f"Loading {target_type} classification inputs...", flush=True)
    exclude = None if overwrite else method
    inputs = _load_inputs(conn, target_type, limit, offset, exclude_method=exclude)
    print(f"  {len(inputs):,} inputs loaded (unclassified for method '{method}').", flush=True)

    if provider == "openai" and adaptive_concurrency:
        counters = _run_openai_adaptive(
            conn, inputs, divisions, valid_codes, model, method,
            max_input_chars, api_key, concurrency, max_retries,
            min_concurrency, max_concurrency, adjustment_window,
            increase_threshold, decrease_threshold, increase_step, decrease_factor,
        )
    elif provider == "openai":
        counters = _run_openai_concurrent(
            conn, inputs, divisions, valid_codes, model, method,
            max_input_chars, api_key, concurrency, max_retries,
        )
    else:
        counters = _run_sequential(
            conn, inputs, provider, model, method, max_input_chars, api_key, sleep_secs,
            divisions, valid_codes,
        )

    _write_summary(conn, method)
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
    parser.add_argument(
        "--concurrency", type=int, default=DEFAULT_CONCURRENCY,
        help="concurrent in-flight OpenAI requests (default: 5)",
    )
    parser.add_argument(
        "--max-retries", type=int, default=DEFAULT_MAX_RETRIES,
        help="max retries per request on transient errors: 429/500/502/503/504/timeouts (default: 5)",
    )
    parser.add_argument(
        "--adaptive-concurrency", action="store_true",
        help="adjust concurrency between windows based on recent retry/error rates (openai provider only)",
    )
    parser.add_argument(
        "--min-concurrency", type=int, default=DEFAULT_MIN_CONCURRENCY,
        help=f"adaptive mode floor (default: {DEFAULT_MIN_CONCURRENCY})",
    )
    parser.add_argument(
        "--max-concurrency", type=int, default=DEFAULT_MAX_CONCURRENCY,
        help=f"adaptive mode ceiling (default: {DEFAULT_MAX_CONCURRENCY})",
    )
    parser.add_argument(
        "--adjustment-window", type=int, default=DEFAULT_ADJUSTMENT_WINDOW,
        help=f"requests per adaptation window (default: {DEFAULT_ADJUSTMENT_WINDOW})",
    )
    parser.add_argument(
        "--increase-threshold", type=float, default=DEFAULT_INCREASE_THRESHOLD,
        help=f"retry_rate at/below which concurrency may increase (default: {DEFAULT_INCREASE_THRESHOLD})",
    )
    parser.add_argument(
        "--decrease-threshold", type=float, default=DEFAULT_DECREASE_THRESHOLD,
        help=f"retry_rate at/above which concurrency decreases (default: {DEFAULT_DECREASE_THRESHOLD})",
    )
    parser.add_argument(
        "--increase-step", type=int, default=DEFAULT_INCREASE_STEP,
        help=f"concurrency added per increase (default: {DEFAULT_INCREASE_STEP})",
    )
    parser.add_argument(
        "--decrease-factor", type=float, default=DEFAULT_DECREASE_FACTOR,
        help=f"multiplier applied on decrease, floored (default: {DEFAULT_DECREASE_FACTOR})",
    )
    args = parser.parse_args()

    if args.adaptive_concurrency and args.min_concurrency > args.max_concurrency:
        print("ERROR: --min-concurrency cannot exceed --max-concurrency.", file=sys.stderr)
        sys.exit(1)

    import os
    api_key = None
    if args.provider == "openai":
        api_key = os.environ.get("OPENAI_API_KEY", "")
        if not api_key:
            print("ERROR: OPENAI_API_KEY environment variable is not set.", file=sys.stderr)
            print("  Set it with:  export OPENAI_API_KEY='sk-...'", file=sys.stderr)
            sys.exit(1)

    print("=" * 64)
    print("ISIC Classification Run")
    print("=" * 64)
    print(f"  provider    : {args.provider}")
    if args.provider == "openai":
        print(f"  model       : {args.model}")
        print(f"  concurrency : {args.concurrency}")
        print(f"  max_retries : {args.max_retries}")
        if args.adaptive_concurrency:
            print(f"  adaptive    : on (min={args.min_concurrency}, max={args.max_concurrency}, "
                  f"window={args.adjustment_window})")
    print(f"  db          : {args.db}")
    print(f"  limit       : {args.limit or 'all'}")
    print(f"  overwrite   : {args.overwrite}")
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
        concurrency=args.concurrency,
        max_retries=args.max_retries,
        adaptive_concurrency=args.adaptive_concurrency,
        min_concurrency=args.min_concurrency,
        max_concurrency=args.max_concurrency,
        adjustment_window=args.adjustment_window,
        increase_threshold=args.increase_threshold,
        decrease_threshold=args.decrease_threshold,
        increase_step=args.increase_step,
        decrease_factor=args.decrease_factor,
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
