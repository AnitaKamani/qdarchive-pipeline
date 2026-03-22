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
    status      TEXT    NOT NULL CHECK(status IN ('SUCCEEDED','FAILED_SERVER_UNRESPONSIVE','FAILED_LOGIN_REQUIRED','FAILED_TOO_LARGE'))
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
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
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


def truncate_db():
    with get_conn() as conn:
        conn.executescript("""
            DELETE FROM licenses;
            DELETE FROM person_role;
            DELETE FROM keywords;
            DELETE FROM files;
            DELETE FROM projects;
        """)
    downloads = Path("downloads")
    if downloads.exists():
        shutil.rmtree(downloads)
    print("DB and downloads/ truncated.")


# ── insert helpers ────────────────────────────────────────────────────────────

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


def insert_file(conn, project_id: int, file_name: str, file_type: str, status: str):
    conn.execute(
        "INSERT INTO files (project_id, file_name, file_type, status) VALUES (?,?,?,?)",
        (project_id, file_name, file_type, status),
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
