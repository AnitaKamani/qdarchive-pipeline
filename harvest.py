import sys
import db
import config
from harvesters import OAIHarvester, DataverseHarvester

HARVESTER_MAP = {
    "oai":       OAIHarvester,
    "dataverse": DataverseHarvester,
}

REPO_INDEX = {r["repo_id"]: r for r in config.REPOS}


def ask_yn(prompt: str, default: bool = False) -> bool:
    hint = "[Y/n]" if default else "[y/N]"
    raw  = input(f"{prompt} {hint}: ").strip().lower()
    if raw == "":
        return default
    return raw in ("y", "yes")


def ask_limit(prompt: str) -> int | None:
    raw = input(f"{prompt} [number / 0 or Enter = no limit]: ").strip()
    if raw == "" or raw == "0":
        return None
    try:
        v = int(raw)
        return v if v > 0 else None
    except ValueError:
        print("  Invalid input, using no limit.")
        return None


if __name__ == "__main__":
    # ── repo selection ────────────────────────────────────────────────────────
    available = ", ".join(str(r["repo_id"]) for r in config.REPOS)

    if len(sys.argv) > 1:
        raw_ids = sys.argv[1:]
    else:
        raw_input = input(f"Repos to harvest (available: {available}, Enter = all): ").strip()
        raw_ids   = raw_input.replace(",", " ").split()

    if not raw_ids:
        repos = list(config.REPOS)
    else:
        try:
            selected_ids = [int(x) for x in raw_ids]
        except ValueError:
            print("Invalid repo IDs.")
            sys.exit(1)
        repos = [REPO_INDEX[rid] for rid in selected_ids if rid in REPO_INDEX]
        if not repos:
            print(f"No matching repos found. Available: {available}")
            sys.exit(1)

    print(f"\nSelected: {[r['repo_id'] for r in repos]}")

    # ── interactive prompts ───────────────────────────────────────────────────
    do_truncate  = ask_yn("1. Truncate database first?", default=False)
    limit        = ask_limit("2. Max results per repo?")
    do_download  = ask_yn("3. Download files?", default=False)
    max_file_mb  = None
    if do_download:
        raw_mb = input("   Max file size in MB? [number / 0 or Enter = no limit]: ").strip()
        if raw_mb and raw_mb != "0":
            try:
                max_file_mb = int(float(raw_mb) * 1024 * 1024)
            except ValueError:
                print("   Invalid input, using no limit.")

    print()

    # ── run ───────────────────────────────────────────────────────────────────
    db.init_db()
    if do_truncate:
        db.truncate_db()

    for repo in repos:
        cls       = HARVESTER_MAP[repo["type"]]
        harvester = cls(**{k: v for k, v in repo.items() if k != "type"})
        ids       = harvester.run(config.KEYWORDS, config.QDA_EXTENSIONS, limit)
        if do_download:
            harvester.download_projects(ids, max_file_bytes=max_file_mb)
