-- =============================================================================
-- Phase 2 Migration — QDArchive Classification Layer
--
-- SQLite does not support ALTER TABLE ... ADD COLUMN IF NOT EXISTS.
-- The projects.project_type column is added by apply_migration.py, which
-- checks PRAGMA table_info(projects) before executing ALTER TABLE.
--
-- All CREATE TABLE / CREATE INDEX statements use IF NOT EXISTS and are safe
-- to re-execute.  Use apply_migration.py (or setup_isic.py) as the runner;
-- do not pipe this file directly to the sqlite3 CLI on a fresh database.
--
-- Requires SQLite >= 3.8.0.
-- Does NOT modify or drop any Phase 1 tables or data.
-- =============================================================================

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

BEGIN;

-- ---------------------------------------------------------------------------
-- 1. projects.project_type is added by apply_migration.py via ALTER TABLE.
--    Shown here for documentation only:
--        ALTER TABLE projects ADD COLUMN project_type TEXT;
-- ---------------------------------------------------------------------------


-- ---------------------------------------------------------------------------
-- 2. ISIC reference table — lookup only, populated by import_isic_divisions.py
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS isic_divisions (
    code            TEXT    PRIMARY KEY,
    section_code    TEXT,
    division        INTEGER,
    title           TEXT    NOT NULL,
    description     TEXT
);


-- ---------------------------------------------------------------------------
-- 3. Classification input staging
--    Records the text fed to the classifier for each target (project or file).
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS classification_inputs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    target_type TEXT    NOT NULL,           -- 'project' | 'file'
    target_id   INTEGER NOT NULL,
    project_id  INTEGER,
    input_text  TEXT    NOT NULL,
    created_at  TEXT    DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (target_type, target_id)
);


-- ---------------------------------------------------------------------------
-- 4. Project-level classification results
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS project_classifications (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id           INTEGER NOT NULL,
    primary_class_code   TEXT,
    secondary_class_code TEXT,
    tags                 TEXT,
    confidence           REAL,
    method               TEXT,
    reason               TEXT,
    created_at           TEXT    DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (project_id, method),
    FOREIGN KEY (project_id)             REFERENCES projects(id),
    FOREIGN KEY (primary_class_code)     REFERENCES isic_divisions(code),
    FOREIGN KEY (secondary_class_code)   REFERENCES isic_divisions(code)
);


-- ---------------------------------------------------------------------------
-- 5. File-level classification results
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS file_classifications (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id              INTEGER NOT NULL,
    project_id           INTEGER NOT NULL,
    primary_class_code   TEXT,
    secondary_class_code TEXT,
    tags                 TEXT,
    confidence           REAL,
    method               TEXT,
    reason               TEXT,
    created_at           TEXT    DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (file_id, method),
    FOREIGN KEY (file_id)                REFERENCES files(id),
    FOREIGN KEY (project_id)             REFERENCES projects(id),
    FOREIGN KEY (primary_class_code)     REFERENCES isic_divisions(code),
    FOREIGN KEY (secondary_class_code)   REFERENCES isic_divisions(code)
);


-- ---------------------------------------------------------------------------
-- 6. Indexes
-- ---------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_files_project_id
    ON files (project_id);

CREATE INDEX IF NOT EXISTS idx_projects_project_type
    ON projects (project_type);

CREATE INDEX IF NOT EXISTS idx_classification_inputs_target_type
    ON classification_inputs (target_type);

CREATE INDEX IF NOT EXISTS idx_project_classifications_primary_class
    ON project_classifications (primary_class_code);

CREATE INDEX IF NOT EXISTS idx_file_classifications_primary_class
    ON file_classifications (primary_class_code);


COMMIT;
