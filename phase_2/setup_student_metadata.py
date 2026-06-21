"""
Phase 2 one-command student metadata setup.

Usage:
    python phase_2/setup_student_metadata.py [--manifest PATH] [--out-dir PATH]
                                              [--combined-db PATH]

Steps:
    1. Download student metadata databases (download_student_dbs).
    2. Inspect each database for schema validity (inspect_student_dbs).
    3. Merge all valid databases into a combined DB (merge_student_dbs).
    4. Print a PASS/FAIL summary.

Stops only if step 2 yields zero valid databases.
"""

import sys
from pathlib import Path

_here = Path(__file__).parent
if str(_here) not in sys.path:
    sys.path.insert(0, str(_here))

from download_student_dbs import (
    download_all,
    MANIFEST_DEFAULT,
    OUT_DIR_DEFAULT,
    REPORT_DEFAULT as DL_REPORT,
)
from inspect_student_dbs import (
    inspect_all,
    IN_DIR_DEFAULT,
    REPORT_DEFAULT as INSPECT_REPORT,
)
from merge_student_dbs import (
    merge_all,
    COMBINED_DB_DEFAULT,
    MERGE_REPORT_DEFAULT,
    DUP_REPORT_DEFAULT,
)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Phase 2 student metadata full setup.")
    parser.add_argument("--manifest", default=MANIFEST_DEFAULT)
    parser.add_argument("--out-dir", default=OUT_DIR_DEFAULT)
    parser.add_argument("--combined-db", default=COMBINED_DB_DEFAULT)
    args = parser.parse_args()

    results: dict[str, str] = {}

    _banner("Phase 2 Student Metadata Setup")

    # ── Step 1: Download ─────────────────────────────────────────────────────
    _section("1/3", "Downloading student databases")
    try:
        n_dl, n_loc, n_fail = download_all(args.manifest, args.out_dir, DL_REPORT)
        print(f"\n  downloaded={n_dl}  local={n_loc}  failed={n_fail}")
        results["download"] = "PASS" if (n_dl + n_loc) > 0 else "FAIL"
    except SystemExit:
        results["download"] = "FAIL"
        _summary(results)
        sys.exit(1)
    except Exception as exc:
        print(f"  ERROR: {exc}", file=sys.stderr)
        results["download"] = "FAIL"
        _summary(results)
        sys.exit(1)

    # ── Step 2: Inspect ───────────────────────────────────────────────────────
    _section("2/3", "Inspecting downloaded databases")
    try:
        n_valid, n_invalid, _ = inspect_all(args.out_dir, INSPECT_REPORT)
        print(f"\n  valid={n_valid}  invalid={n_invalid}")
        results["inspect"] = "PASS" if n_valid > 0 else "FAIL"
    except Exception as exc:
        print(f"  ERROR: {exc}", file=sys.stderr)
        results["inspect"] = "FAIL"
        _summary(results)
        sys.exit(1)

    if results["inspect"] == "FAIL":
        print("ERROR: no valid databases — cannot merge.", file=sys.stderr)
        _summary(results)
        sys.exit(1)

    # ── Step 3: Merge ─────────────────────────────────────────────────────────
    _section("3/3", "Merging into combined database")
    try:
        totals = merge_all(
            args.out_dir, args.combined_db, MERGE_REPORT_DEFAULT, DUP_REPORT_DEFAULT
        )
        print()
        for tbl, cnt in totals.items():
            print(f"  {tbl:<30} : {cnt:,}")
        results["merge"] = "PASS" if totals.get("combined_projects", 0) > 0 else "FAIL"
    except Exception as exc:
        print(f"  ERROR: {exc}", file=sys.stderr)
        results["merge"] = "FAIL"

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
