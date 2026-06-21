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
5. Output mode: `bar` (default) shows a tqdm progress bar; `detail` shows per-item verbose output

## Database

SQLite file: `<student_id>-<seeding_db>.db` (configured in `config.py`) — 5 tables, 1 view:

| Table | Description |
|-------|-------------|
| `projects` | Harvested dataset metadata |
| `files` | File listings with URL, size, and download status |
| `keywords` | Subject keywords per project |
| `person_role` | Authors, uploaders, contributors |
| `licenses` | License info per project |

| View | Description |
|------|-------------|
| `v_files` | Files joined with project URL, repo ID, size in MB, and download date |

File download statuses: `SUCCEEDED`, `NOT_ATTEMPTED`, `FAILED_LOGIN_REQUIRED`, `FAILED_TOO_LARGE`, `FAILED_SERVER_UNRESPONSIVE`

> **Truncate** — choosing "Truncate database" at startup deletes the `.db` file entirely and recreates it from scratch, including all tables and views.

## Downloads

Files are saved locally to:

```
downloads/<repo_folder>/<project_folder>/<filename>
```

The downloaded files referenced in [23727550-sq26.db](https://github.com/AnitaKamani/qdarchive-pipeline/blob/main/23727550-sq26.db) have been uploaded to [Google Drive](https://drive.google.com/drive/folders/1o9fbdV-gSAqRUw8gA0AnbUHjLxd9Vg0U) under the same folder structure.

## Phase 2: Classification

Phase 2 merges student databases, classifies project types by file extension, builds model-ready input text, and assigns ISIC Rev. 5 division codes to each project.

### Student metadata merge

```bash
python phase_2/setup_student_metadata.py --in-dir data/student_metadata --combined-db 23727550-sq26-combined.db
```

### Classification preparation (Milestone 3)

Classifies projects by file extension rules and builds `classification_inputs`:

```bash
python phase_2/prepare_classification.py --db 23727550-sq26-combined.db
python phase_2/check_classification_preparation.py --db 23727550-sq26-combined.db
```

### ISIC classification (Milestone 4)

**Dry-run test (no API key required):**

```bash
python phase_2/run_isic_classification.py --db 23727550-sq26-combined.db --provider local-dry-run --limit 20 --overwrite
python phase_2/check_isic_classification.py --db 23727550-sq26-combined.db
```

**OpenAI test (20 projects):**

```bash
export OPENAI_API_KEY="..."
python phase_2/run_isic_classification.py --db 23727550-sq26-combined.db --provider openai --model gpt-4o-mini --limit 20 --overwrite
```

**Full OpenAI project classification:**

```bash
python phase_2/run_isic_classification.py --db 23727550-sq26-combined.db --provider openai --model gpt-4o-mini
```

**Validate results:**

```bash
python phase_2/check_isic_classification.py --db 23727550-sq26-combined.db
```
