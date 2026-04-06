import shutil
import sqlite3
from contextlib import contextmanager
from pathlib import Path

DB_PATH = "qdarchive.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS projects (
    id                          INTEGER PRIMARY KEY,
    query_string                TEXT,
    repository_id               INTEGER NOT NULL,
    repository_url              TEXT    NOT NULL,
    project_url                 TEXT    NOT NULL,
    version                     TEXT,
    title                       TEXT    NOT NULL,
    description                 TEXT    NOT NULL,
    language                    TEXT,
    doi                         TEXT,
    upload_date                 TEXT,
    download_date               TEXT    NOT NULL,
    download_repository_folder  TEXT    NOT NULL,
    download_project_folder     TEXT    NOT NULL,
    download_version_folder     TEXT,
    download_method             TEXT    NOT NULL CHECK(download_method IN ('SCRAPING','API-CALL'))
);

CREATE TABLE IF NOT EXISTS files (
    id          INTEGER PRIMARY KEY,
    project_id  INTEGER NOT NULL REFERENCES projects(id),
    file_name   TEXT    NOT NULL,
    file_type   TEXT    NOT NULL,
    file_url    TEXT,
    file_size   INTEGER,
    zip_path    TEXT,
    status      TEXT    NOT NULL CHECK(status IN ('SUCCEEDED','FAILED_SERVER_UNRESPONSIVE','FAILED_LOGIN_REQUIRED','FAILED_TOO_LARGE','NOT_ATTEMPTED'))
);

CREATE TABLE IF NOT EXISTS harvest_state (
    repo_id           INTEGER PRIMARY KEY,
    last_harvested_at TEXT,
    resumption_token  TEXT
);

CREATE TABLE IF NOT EXISTS keywords (
    id          INTEGER PRIMARY KEY,
    project_id  INTEGER NOT NULL REFERENCES projects(id),
    keyword     TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS person_role (
    id          INTEGER PRIMARY KEY,
    project_id  INTEGER NOT NULL REFERENCES projects(id),
    name        TEXT    NOT NULL,
    role        TEXT    NOT NULL CHECK(role IN ('UPLOADER','AUTHOR','OWNER','OTHER','UNKNOWN'))
);

CREATE TABLE IF NOT EXISTS licenses (
    id          INTEGER PRIMARY KEY,
    project_id  INTEGER NOT NULL REFERENCES projects(id),
    license     TEXT    NOT NULL
);
"""


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    with get_conn() as conn:
        conn.executescript(SCHEMA)
        _migrate_files_table(conn)


def _migrate_files_table(conn):
    """Recreate files table if schema is outdated (new columns or CHECK values)."""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(files)")}
    needs_recreate = "file_url" not in cols or "file_size" not in cols or "zip_path" not in cols

    if not needs_recreate:
        # Check if NOT_ATTEMPTED is already accepted
        try:
            conn.execute("SAVEPOINT chk")
            conn.execute("INSERT INTO files (project_id,file_name,file_type,status) "
                         "VALUES (1,'_chk','_chk','NOT_ATTEMPTED')")
            conn.execute("DELETE FROM files WHERE file_name='_chk'")
            conn.execute("RELEASE chk")
            return  # constraint already updated
        except Exception:
            conn.execute("ROLLBACK TO chk")
            conn.execute("RELEASE chk")
            needs_recreate = True

    if needs_recreate:
        conn.executescript("""
            CREATE TABLE files_new (
                id          INTEGER PRIMARY KEY,
                project_id  INTEGER NOT NULL REFERENCES projects(id),
                file_name   TEXT    NOT NULL,
                file_type   TEXT    NOT NULL,
                file_url    TEXT,
                file_size   INTEGER,
                zip_path    TEXT,
                status      TEXT    NOT NULL CHECK(status IN (
                    'SUCCEEDED','FAILED_SERVER_UNRESPONSIVE',
                    'FAILED_LOGIN_REQUIRED','FAILED_TOO_LARGE','NOT_ATTEMPTED'))
            );
            INSERT INTO files_new (id, project_id, file_name, file_type, file_url, file_size, status)
                SELECT id, project_id, file_name, file_type, file_url, file_size, status FROM files;
            DROP TABLE files;
            ALTER TABLE files_new RENAME TO files;
        """)
        print("DB migrated: files table updated.")


def truncate_db():
    with get_conn() as conn:
        conn.executescript("""
            DELETE FROM licenses;
            DELETE FROM person_role;
            DELETE FROM keywords;
            DELETE FROM files;
            DELETE FROM projects;
            DELETE FROM harvest_state;
        """)
    downloads = Path("downloads")
    if downloads.exists():
        shutil.rmtree(downloads)
    print("DB and downloads/ truncated.")


# ── insert helpers ────────────────────────────────────────────────────────────

def get_harvest_state(conn, repo_id: int) -> dict:
    row = conn.execute(
        "SELECT last_harvested_at, resumption_token FROM harvest_state WHERE repo_id=?", (repo_id,)
    ).fetchone()
    return dict(row) if row else {"last_harvested_at": None, "resumption_token": None}


def save_harvest_state(conn, repo_id: int, last_harvested_at: str | None = None,
                       resumption_token: str | None = None):
    conn.execute("""
        INSERT INTO harvest_state (repo_id, last_harvested_at, resumption_token)
        VALUES (?, ?, ?)
        ON CONFLICT(repo_id) DO UPDATE SET
            last_harvested_at = COALESCE(excluded.last_harvested_at, last_harvested_at),
            resumption_token  = excluded.resumption_token
    """, (repo_id, last_harvested_at, resumption_token))


def project_exists(conn, repository_id: int, project_url: str) -> int | None:
    """Return existing project id if already harvested, else None."""
    row = conn.execute(
        "SELECT id FROM projects WHERE repository_id=? AND project_url=?",
        (repository_id, project_url),
    ).fetchone()
    return row["id"] if row else None


def project_needs_files(conn, project_id: int) -> bool:
    """True if project has no SUCCEEDED file rows."""
    row = conn.execute(
        "SELECT 1 FROM files WHERE project_id=? AND status='SUCCEEDED' LIMIT 1", (project_id,)
    ).fetchone()
    return row is None


def clear_project_files(conn, project_id: int):
    """Delete all file rows for a project so they can be re-fetched."""
    conn.execute("DELETE FROM files WHERE project_id=?", (project_id,))


def insert_project(conn, data: dict) -> int:
    cur = conn.execute("""
        INSERT INTO projects (
            query_string, repository_id, repository_url, project_url,
            version, title, description, language, doi, upload_date,
            download_date, download_repository_folder, download_project_folder,
            download_version_folder, download_method
        ) VALUES (
            :query_string, :repository_id, :repository_url, :project_url,
            :version, :title, :description, :language, :doi, :upload_date,
            :download_date, :download_repository_folder, :download_project_folder,
            :download_version_folder, :download_method
        )
    """, data)
    return cur.lastrowid


def insert_file(conn, project_id: int, file_name: str, file_type: str, status: str,
                file_url: str | None = None, file_size: int | None = None,
                zip_path: str | None = None):
    conn.execute(
        "INSERT INTO files (project_id, file_name, file_type, file_url, file_size, zip_path, status) VALUES (?,?,?,?,?,?,?)",
        (project_id, file_name, file_type, file_url, file_size, zip_path, status),
    )


def insert_keyword(conn, project_id: int, keyword: str):
    conn.execute(
        "INSERT INTO keywords (project_id, keyword) VALUES (?,?)",
        (project_id, keyword),
    )


def insert_person(conn, project_id: int, name: str, role: str):
    conn.execute(
        "INSERT INTO person_role (project_id, name, role) VALUES (?,?,?)",
        (project_id, name, role),
    )


def insert_license(conn, project_id: int, license_str: str):
    conn.execute(
        "INSERT INTO licenses (project_id, license) VALUES (?,?)",
        (project_id, license_str),
    )
