-- =============================================================================
-- Phase 2 Migration — QDArchive Classification Layer
--
-- SQLite does not support ALTER TABLE ... ADD COLUMN IF NOT EXISTS.
-- The ADD COLUMN statement below is guarded by apply_migration.py, which
-- checks PRAGMA table_info(projects) before executing it.
--
-- All CREATE TABLE / CREATE INDEX statements use IF NOT EXISTS and are safe
-- to execute repeatedly.  Do NOT run this file directly via the sqlite3 CLI
-- unless you are certain project_type does not yet exist; use apply_migration.py
-- instead, which is fully idempotent.
--
-- Requires SQLite >= 3.8.0.
-- Does NOT modify or drop any Phase 1 tables or data.
-- =============================================================================

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

BEGIN;

-- ---------------------------------------------------------------------------
-- 1. Extend projects with a nullable classification column.
--    Guarded externally by apply_migration.py; omit here to stay valid SQL.
--    Equivalent statement (run only when column is absent):
--        ALTER TABLE projects ADD COLUMN project_type TEXT;
-- ---------------------------------------------------------------------------


-- ---------------------------------------------------------------------------
-- 2. ISIC reference table — lookup only, populated separately
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

-- Phase 1 tables (adding useful lookup indexes without touching existing data)
CREATE INDEX IF NOT EXISTS idx_projects_project_type
    ON projects (project_type);

CREATE INDEX IF NOT EXISTS idx_files_project_id
    ON files (project_id);

-- Phase 2 classification tables
CREATE INDEX IF NOT EXISTS idx_project_classifications_primary_code
    ON project_classifications (primary_class_code);

CREATE INDEX IF NOT EXISTS idx_file_classifications_primary_code
    ON file_classifications (primary_class_code);

CREATE INDEX IF NOT EXISTS idx_classification_inputs_target_type
    ON classification_inputs (target_type);


COMMIT;
