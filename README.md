# qdarchive-pipeline

Pipeline for acquiring and structuring open qualitative research data for QDArchive. Crawls public repositories using OAI-PMH and Dataverse APIs, harvests project metadata into a normalized SQLite database, and optionally downloads associated files.

## Repositories

| ID | Repository | Type |
|----|-----------|------|
| 16 | opendata.uni-halle.de | OAI-PMH |
| 5  | ssh.datastations.nl (DANS) | Dataverse |

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # then fill in your tokens
```

## Configuration

Edit [config.py](config.py) to add keywords, file extensions, or new repositories.

**API tokens** go in `.env` (never commit this file):

```
DANS_API_TOKEN=your-token-here
```

Get your DANS token: log in at ssh.datastations.nl → account menu → API Token.

## Usage

```bash
python harvest.py
```

You will be prompted for:
1. Which repos to harvest (Enter = all)
2. Truncate database first?
3. Max results per repo (Enter = no limit)
4. Download files? (if yes: max file size in MB)

## Database

SQLite file: `qdarchive.db` — 6 tables, 1 view:

| Table | Description |
|-------|-------------|
| `projects` | Harvested dataset metadata |
| `files` | File listings with URL, size, and download status |
| `keywords` | Subject keywords per project |
| `person_role` | Authors, uploaders, contributors |
| `licenses` | License info per project |
| `harvest_state` | Incremental harvest checkpoint per repository |

| View | Description |
|------|-------------|
| `v_files` | Files joined with project URL, repo ID, and size in MB |

File download statuses: `SUCCEEDED`, `NOT_ATTEMPTED`, `FAILED_LOGIN_REQUIRED`, `FAILED_TOO_LARGE`, `FAILED_SERVER_UNRESPONSIVE`

> **Truncate** — choosing "Truncate database" at startup deletes the `.db` file entirely and recreates it from scratch, including all tables and views.

## Downloads

Files are saved to:
```
downloads/<repo_folder>/<project_folder>/<filename>
```
