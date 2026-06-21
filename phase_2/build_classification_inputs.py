"""
Build classification_inputs rows for model-based ISIC classification.

For each qualifying project (or file), assembles a clean input_text from
title, description, keywords, file names, and metadata fields, then
inserts into classification_inputs using INSERT OR REPLACE.

Usage:
    python phase_2/build_classification_inputs.py [--db PATH]
        [--target-type {PROJECT,FILE,BOTH}]
        [--limit N]
        [--include-project-types QDA_PROJECT,QD_PROJECT]
"""

import re
import sqlite3
from collections import defaultdict
from pathlib import Path

DB_DEFAULT = "23727550-sq26-combined.db"
MAX_INPUT_LEN = 6000
MIN_INPUT_LEN = 30
KW_LIMIT_PROJ = 50
FILE_LIMIT_PROJ = 50
KW_LIMIT_FILE = 30


def _clean(text: str | None) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _dedup(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out = []
    for x in items:
        lx = x.lower()
        if lx not in seen:
            seen.add(lx)
            out.append(x)
    return out


def _project_text(proj: dict, keywords: list[str], file_names: list[str]) -> str:
    parts = []
    if proj.get("title"):
        parts.append(f"Title: {_clean(proj['title'])}")
    if proj.get("description"):
        parts.append(f"Description: {_clean(proj['description'])}")
    if keywords:
        parts.append(f"Keywords: {', '.join(keywords)}")
    if proj.get("repository_id") is not None:
        parts.append(f"Repository: {proj['repository_id']}")
    if proj.get("repository_url"):
        parts.append(f"Repository URL: {_clean(proj['repository_url'])}")
    if proj.get("project_url"):
        parts.append(f"Project URL: {_clean(proj['project_url'])}")
    if proj.get("language"):
        parts.append(f"Language: {_clean(proj['language'])}")
    if proj.get("doi"):
        parts.append(f"DOI: {_clean(proj['doi'])}")
    if file_names:
        parts.append(f"Files: {', '.join(file_names)}")
    return (" | ".join(parts))[:MAX_INPUT_LEN]


def _file_text(file: dict, proj: dict, keywords: list[str]) -> str:
    parts = []
    if file.get("file_name"):
        parts.append(f"File: {_clean(file['file_name'])}")
    if file.get("file_type"):
        parts.append(f"Type: {_clean(file['file_type'])}")
    if proj.get("title"):
        parts.append(f"Project: {_clean(proj['title'])}")
    if proj.get("description"):
        parts.append(f"Description: {_clean(proj['description'])}")
    if keywords:
        parts.append(f"Keywords: {', '.join(keywords)}")
    if proj.get("project_url"):
        parts.append(f"Project URL: {_clean(proj['project_url'])}")
    return (" | ".join(parts))[:MAX_INPUT_LEN]


def build_inputs(
    db_path: str,
    target_type: str = "PROJECT",
    limit: int | None = None,
    include_types: list[str] | None = None,
) -> dict[str, int]:
    """Build classification_inputs rows. Returns {target_type: count}."""
    if include_types is None:
        include_types = ["QDA_PROJECT", "QD_PROJECT"]

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.row_factory = sqlite3.Row

    type_in = ", ".join(f"'{t}'" for t in include_types)
    counts = {"PROJECT": 0, "FILE": 0}

    do_proj = target_type in ("PROJECT", "BOTH")
    do_file = target_type in ("FILE", "BOTH")

    if do_proj:
        print("  Loading keywords...", flush=True)
        kw_by_project: defaultdict[int, list[str]] = defaultdict(list)
        for r in conn.execute(
            "SELECT global_project_id, keyword FROM combined_keywords WHERE keyword IS NOT NULL"
        ):
            kw_by_project[r[0]].append(r[1])

        print("  Loading file names...", flush=True)
        fname_by_project: defaultdict[int, list[str]] = defaultdict(list)
        for r in conn.execute(
            "SELECT global_project_id, COALESCE(file_name, file_type) "
            "FROM combined_files WHERE file_name IS NOT NULL OR file_type IS NOT NULL"
        ):
            if r[1]:
                fname_by_project[r[0]].append(r[1])

        proj_sql = (
            f"SELECT global_project_id, source_student_id, title, description, "
            f"repository_id, repository_url, project_url, language, doi "
            f"FROM combined_projects WHERE project_type IN ({type_in})"
        )
        if limit:
            proj_sql += f" LIMIT {limit}"

        print("  Building PROJECT inputs...", flush=True)
        batch: list[tuple] = []
        for p_row in conn.execute(proj_sql):
            proj = dict(p_row)
            gid = proj["global_project_id"]

            kws = _dedup(kw_by_project[gid])[:KW_LIMIT_PROJ]
            fnames = _dedup(fname_by_project[gid])[:FILE_LIMIT_PROJ]

            text = _project_text(proj, kws, fnames)
            if len(text) < MIN_INPUT_LEN:
                continue

            batch.append(("PROJECT", gid, gid, text))
            counts["PROJECT"] += 1

        conn.executemany(
            "INSERT OR REPLACE INTO classification_inputs "
            "(target_type, target_id, project_id, input_text) VALUES (?, ?, ?, ?)",
            batch,
        )
        conn.commit()
        print(f"  Wrote {counts['PROJECT']:,} PROJECT inputs.", flush=True)

    if do_file:
        print("  Loading project info for FILE inputs...", flush=True)
        proj_info: dict[int, dict] = {}
        for r in conn.execute(
            f"SELECT global_project_id, title, description, project_url "
            f"FROM combined_projects WHERE project_type IN ({type_in})"
        ):
            proj_info[r[0]] = {"title": r[1], "description": r[2], "project_url": r[3]}

        kw_by_proj2: defaultdict[int, list[str]] = defaultdict(list)
        for r in conn.execute(
            "SELECT global_project_id, keyword FROM combined_keywords WHERE keyword IS NOT NULL"
        ):
            if r[0] in proj_info:
                kw_by_proj2[r[0]].append(r[1])

        file_sql = (
            "SELECT cf.global_file_id, cf.global_project_id, cf.file_name, cf.file_type "
            "FROM combined_files cf "
            f"WHERE cf.global_project_id IN "
            f"(SELECT global_project_id FROM combined_projects WHERE project_type IN ({type_in}))"
        )
        if limit:
            file_sql += f" LIMIT {limit}"

        print("  Building FILE inputs...", flush=True)
        batch = []
        for f_row in conn.execute(file_sql):
            fid, pid = f_row[0], f_row[1]
            proj = proj_info.get(pid, {})
            kws = kw_by_proj2[pid][:KW_LIMIT_FILE]
            file_dict = {"file_name": f_row[2], "file_type": f_row[3]}
            text = _file_text(file_dict, proj, kws)
            if len(text) < MIN_INPUT_LEN:
                continue

            batch.append(("FILE", fid, pid, text))
            counts["FILE"] += 1

            if len(batch) >= 10_000:
                conn.executemany(
                    "INSERT OR REPLACE INTO classification_inputs "
                    "(target_type, target_id, project_id, input_text) VALUES (?, ?, ?, ?)",
                    batch,
                )
                conn.commit()
                batch = []

        if batch:
            conn.executemany(
                "INSERT OR REPLACE INTO classification_inputs "
                "(target_type, target_id, project_id, input_text) VALUES (?, ?, ?, ?)",
                batch,
            )
            conn.commit()
        print(f"  Wrote {counts['FILE']:,} FILE inputs.", flush=True)

    conn.close()
    return counts


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(
        description="Build classification_inputs for ISIC model classification."
    )
    parser.add_argument("--db", default=DB_DEFAULT)
    parser.add_argument("--target-type", choices=["PROJECT", "FILE", "BOTH"], default="PROJECT")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--include-project-types", default="QDA_PROJECT,QD_PROJECT")
    args = parser.parse_args()

    include_types = [t.strip() for t in args.include_project_types.split(",")]
    print(f"Building {args.target_type} inputs for types: {include_types}")
    counts = build_inputs(args.db, args.target_type, args.limit, include_types)

    print("\nResults:")
    for k, v in counts.items():
        if v > 0:
            print(f"  {k}: {v:,} inputs written")


if __name__ == "__main__":
    main()
