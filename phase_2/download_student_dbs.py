"""
Download student metadata SQLite databases listed in the manifest CSV.

Usage:
    python phase_2/download_student_dbs.py [--manifest PATH] [--out-dir PATH] [--report PATH]

URL normalization applied automatically:
    raw.githubusercontent.com/.../blob/BRANCH/...  →  remove /blob/
    github.com/.../blob/BRANCH/...                 →  raw.githubusercontent.com/...

LOCAL: entries are copied from the local filesystem instead of downloaded.
Downloaded files are validated against the SQLite magic bytes.
"""

import csv
import re
import shutil
import sys
from pathlib import Path

import requests

MANIFEST_DEFAULT = "data/manifests/student_submissions.csv"
OUT_DIR_DEFAULT = "data/student_metadata"
REPORT_DEFAULT = "reports/student_db_download_report.csv"

SQLITE_MAGIC = b"SQLite format 3"
TIMEOUT = 30


def normalize_url(url: str) -> str:
    # raw.githubusercontent.com URLs that still contain /blob/
    url = re.sub(
        r"(raw\.githubusercontent\.com/[^/]+/[^/]+)/blob/",
        r"\1/",
        url,
    )
    # github.com blob URL → raw content
    url = re.sub(
        r"https://github\.com/([^/]+/[^/]+)/blob/",
        r"https://raw.githubusercontent.com/\1/",
        url,
    )
    return url


def is_sqlite(data: bytes) -> bool:
    return data[: len(SQLITE_MAGIC)] == SQLITE_MAGIC


def download_all(
    manifest_path: str = MANIFEST_DEFAULT,
    out_dir: str = OUT_DIR_DEFAULT,
    report_path: str = REPORT_DEFAULT,
) -> tuple[int, int, int]:
    """Download all entries in the manifest. Returns (n_downloaded, n_local, n_failed)."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    Path(report_path).parent.mkdir(parents=True, exist_ok=True)

    with open(manifest_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    report_rows: list[dict] = []
    n_downloaded = n_local = n_failed = 0

    for row in rows:
        student_id = row["student_id"].strip()
        url = row["metadata_url"].strip()
        dest = out / f"{student_id}.db"

        if url.startswith("LOCAL:"):
            src = Path(url[6:].strip())
            if src.exists():
                shutil.copy2(src, dest)
                size = dest.stat().st_size
                n_local += 1
                status, error = "local", ""
                print(f"  [local]  {student_id}: {src} ({size:,} bytes)")
            else:
                size = 0
                n_failed += 1
                status = "failed"
                error = f"local file not found: {src}"
                print(f"  [FAIL]   {student_id}: {error}")

            report_rows.append(dict(
                student_id=student_id,
                original_url=url,
                normalized_url=str(src),
                local_path=str(dest) if status == "local" else "",
                status=status,
                error=error,
                file_size_bytes=size,
            ))
            continue

        normalized = normalize_url(url)
        status, error, size = "failed", "", 0

        try:
            resp = requests.get(normalized, timeout=TIMEOUT)
            resp.raise_for_status()
            data = resp.content
            if not is_sqlite(data):
                header = data[:32]
                error = f"not a SQLite file ({len(data)} bytes, header={header!r})"
                print(f"  [FAIL]   {student_id}: {error}")
            else:
                dest.write_bytes(data)
                size = len(data)
                status = "downloaded"
                n_downloaded += 1
                print(f"  [OK]     {student_id}: {size:,} bytes")
        except requests.HTTPError as exc:
            error = f"HTTP {exc.response.status_code}"
            print(f"  [FAIL]   {student_id}: {error}")
        except requests.RequestException as exc:
            error = str(exc)[:120]
            print(f"  [FAIL]   {student_id}: {error}")

        if status == "failed":
            n_failed += 1

        report_rows.append(dict(
            student_id=student_id,
            original_url=url,
            normalized_url=normalized,
            local_path=str(dest) if status == "downloaded" else "",
            status=status,
            error=error,
            file_size_bytes=size,
        ))

    _write_csv(
        report_path,
        ["student_id", "original_url", "normalized_url", "local_path",
         "status", "error", "file_size_bytes"],
        report_rows,
    )
    return n_downloaded, n_local, n_failed


def _write_csv(path: str, fields: list[str], rows: list[dict]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Download student metadata SQLite databases.")
    parser.add_argument("--manifest", default=MANIFEST_DEFAULT)
    parser.add_argument("--out-dir", default=OUT_DIR_DEFAULT)
    parser.add_argument("--report", default=REPORT_DEFAULT)
    args = parser.parse_args()

    print("Downloading student databases...")
    n_dl, n_loc, n_fail = download_all(args.manifest, args.out_dir, args.report)

    print(f"\nDownload summary:")
    print(f"  Downloaded   : {n_dl}")
    print(f"  Local (copy) : {n_loc}")
    print(f"  Failed       : {n_fail}")
    print(f"  Report       : {args.report}")

    if n_dl + n_loc == 0:
        print("ERROR: no usable databases.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
