import sys
import time
from collections import deque
from datetime import datetime

import db
import config
from harvesters import OAIHarvester, DataverseHarvester, RateLimitError, CircuitOpenError

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
    do_download  = ask_yn("3. Download files?", default=True)
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
    start_time = time.time()
    start_dt   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Began: {start_dt}\n")

    if do_truncate:
        db.truncate_db()
    else:
        db.init_db()

    queue         = deque(repos)
    failures      = {}   # repo_id -> retry count
    backoff_until = {}   # repo_id -> timestamp when it may be retried

    while queue:
        repo = queue.popleft()
        rid  = repo["repo_id"]

        # Check cooldown
        wait_until = backoff_until.get(rid, 0)
        if time.time() < wait_until:
            queue.append(repo)
            # If every remaining repo is in cooldown, sleep until the soonest one is ready
            all_cooling = all(time.time() < backoff_until.get(r["repo_id"], 0) for r in queue)
            if all_cooling:
                soonest = min(backoff_until.get(r["repo_id"], 0) for r in queue)
                wait    = max(0, soonest - time.time())
                print(f"\nAll repos in cooldown — waiting {wait:.0f}s...")
                time.sleep(wait)
            continue

        try:
            cls       = HARVESTER_MAP[repo["type"]]
            harvester = cls(**{k: v for k, v in repo.items() if k != "type"})
            harvester.run(config.KEYWORDS, config.QDA_EXTENSIONS, limit,
                          max_file_bytes=max_file_mb, download=do_download)
            # success — clear failure state
            failures.pop(rid, None)
            backoff_until.pop(rid, None)

        except (RateLimitError, CircuitOpenError) as e:
            count = failures.get(rid, 0) + 1
            failures[rid] = count
            if count >= config.MAX_REPO_RETRIES:
                print(f"\n  Repo {rid} failed {count} times — giving up.")
            else:
                backoff = config.REPO_RETRY_BACKOFF * (2 ** (count - 1))
                backoff_until[rid] = time.time() + backoff
                print(f"\n  Repo {rid} throttled (attempt {count}/{config.MAX_REPO_RETRIES})"
                      f" — retrying in {backoff}s. Moving to next repo.")
                queue.append(repo)

        except Exception as e:
            print(f"\n  Repo {rid} error: {e}")

    end_dt  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    elapsed = int(time.time() - start_time)
    days, rem       = divmod(elapsed, 86400)
    hours, rem      = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    parts = []
    if days:    parts.append(f"{days}d")
    if hours:   parts.append(f"{hours}h")
    if minutes: parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")
    print(f"\nBegan:    {start_dt}")
    print(f"Ended:    {end_dt}")
    print(f"Duration: {' '.join(parts)}")
