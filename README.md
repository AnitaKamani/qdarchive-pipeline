# qdarchive-pipeline

A two-phase pipeline for building the QDArchive research-data catalogue. Phase 1
harvests project metadata from open research-data repositories (OAI-PMH and
Dataverse APIs), normalizes it into a SQLite database, optionally downloads
associated files, and classifies each project as `QDA_PROJECT`, `QD_PROJECT`,
`OTHER_PROJECT`, or `NOT_A_PROJECT` by file-extension/keyword rules. Phase 2
merges per-student databases, classifies eligible projects into ISIC Rev. 5
divisions using OpenAI models, validates and evaluates the results, and
generates the final PDF/XLSX/CSV deliverables.

## Features

- OAI-PMH and Dataverse repository harvesting
- SQLite metadata normalization
- Rule-based project-type classification (Phase 1)
- ISIC Rev. 5 division classification via OpenAI (Phase 2)
- Asynchronous OpenAI inference with adaptive concurrency
- Automatic retry handling on transient API errors
- Cross-model resume (skips projects already classified by an accepted model)
- Evaluation reports and charts
- PDF report and XLSX table deliverables

## Repository Support

Repositories are registered in [config.py](config.py)'s `REPOS` list, not
hardcoded. Each entry specifies a `repo_id`, a connector `type` (`oai` or
`dataverse`), a base URL, and a local download folder; `harvest.py` dispatches
each entry to the matching connector in its `HARVESTER_MAP`. Any number of
repositories exposing an OAI-PMH or Dataverse API can be added this way
without touching the harvesting code itself.

## Installation

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # then add your API tokens
```

## Pipeline

**Phase 1** (per student)

```
Harvest → Normalize → Project-type classification → Per-student SQLite database
```

**Phase 2** (combined)

```
Merge student databases → Classification preparation → OpenAI ISIC classification
  → Validation → Evaluation → PDF / XLSX outputs
```

## Main Commands

### Harvest

```bash
python harvest.py
```

Interactive prompts: which repos to harvest (Enter = all), whether to
truncate the database first, max results per repo (Enter = no limit),
whether to download files (and max file size), and output mode
(`bar` = progress bar, `detail` = verbose per-item output).

### Merge student databases

```bash
python phase_2/setup_student_metadata.py --in-dir data/student_metadata --combined-db 23727550-sq26-combined.db
```

### Prepare classification

```bash
python phase_2/prepare_classification.py --db 23727550-sq26-combined.db
python phase_2/check_classification_preparation.py --db 23727550-sq26-combined.db
```

### Run ISIC classification

```bash
python phase_2/run_isic_classification.py \
  --db 23727550-sq26-combined.db \
  --provider openai \
  --model gpt-4.1-mini \
  --concurrency 3 \
  --adaptive-concurrency \
  --min-concurrency 2 \
  --max-concurrency 10 \
  --adjustment-window 100 \
  --resume-across-models
```

This command is resumable: rerunning it continues an interrupted run and
skips any project already classified successfully by an accepted model
(`--resume-across-models`), rather than reclassifying from scratch. A
`--provider local-dry-run` mode is also available for testing the pipeline
without an OpenAI API key.

### Validate

```bash
python phase_2/check_isic_classification.py --db 23727550-sq26-combined.db \
  --combined-methods openai:gpt-4.1-mini,openai:gpt-4o-mini
```

### Regenerate all outputs

```bash
./regenerate_outputs.sh
```

Regenerates every derived artifact from the current database state in one
command: evaluation reports, figures, both XLSX classification tables, the
PDF report, and all validation artifacts. It never reruns OpenAI
classification and never makes an API call — it only reads the database and
rewrites files under `reports/`. Pass `--dry-run` to preview the planned
stages without touching any file, or `--continue-on-error` to run every
stage and report all failures at the end instead of stopping at the first
one.

## Project Structure

```
config.py                # keywords, extensions, REPOS list, resilience settings
harvest.py                # Phase 1 entry point
harvesters.py             # OAI-PMH / Dataverse connectors
db.py                     # Phase 1 schema and writes
phase_2/                  # merge, classification, validation, evaluation, reporting
data/                     # manifests, reference data, per-student metadata
reports/                  # generated deliverables (not committed)
downloads/                # downloaded project files
docs/                     # architecture notes
regenerate_outputs.sh     # regenerate all Phase 2 outputs
```

## Outputs

- Per-student and combined SQLite databases
- PDF classification report (with table of contents, charts, and tables)
- XLSX classification tables (classified-only and full)
- Evaluation CSVs (coverage, confidence, model agreement, throughput)
- Figures (division distribution, confidence, coverage, model agreement)

## Technologies

- Python
- SQLite
- OpenAI API
- Matplotlib
- pypdf
- OpenPyXL
- tqdm
- Requests

## Notes

- The OpenAI API key is read from `.env` (never committed).
- Generated reports and exports under `reports/` are intentionally not
  committed; they are reproducible from the database.
- The SQLite database is the single source of truth for all deliverables.
- Report and export regeneration is deterministic given the same database
  state — running it twice without new classification results produces
  identical output.
