import re
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

CET = ZoneInfo("Europe/Berlin")

import requests
import db

DOWNLOAD_ROOT  = "downloads"
MAX_FILE_BYTES = 500 * 1024 * 1024   # 500 MB hard cap

# ── base ──────────────────────────────────────────────────────────────────────

class BaseHarvester:
    def __init__(self, repo_id: int, repo_url: str, repo_folder: str):
        self.repo_id     = repo_id
        self.repo_url    = repo_url
        self.repo_folder = repo_folder
        self.session     = requests.Session()

    def run(self, keywords, extensions, limit) -> list[int]:
        raise NotImplementedError

    def download_projects(self, project_ids: list[int]):
        raise NotImplementedError

    # ── shared helpers ────────────────────────────────────────────────────────

    def _match(self, text: str, keywords, extensions) -> str | None:
        text = text.lower()
        for kw  in keywords:
            if kw  in text: return kw
        for ext in extensions:
            if ext in text: return ext
        return None

    def _now(self) -> str:
        return datetime.now(CET).isoformat()

    def _project_folder(self, project_url: str) -> str:
        return project_url.rstrip("/").split("/")[-1] if project_url else "unknown"

    def _dest_dir(self, project_folder: str) -> Path:
        return Path(DOWNLOAD_ROOT) / self.repo_folder / project_folder

    def _save_file(self, conn, project_id: int, url: str, file_name: str, dest_dir: Path,
                   max_bytes: int | None = None, known_size: int | None = None, download: bool = True):
        cap       = max_bytes if max_bytes is not None else MAX_FILE_BYTES
        file_type = Path(file_name).suffix.lstrip(".").lower() or "unknown"

        if not download:
            size = known_size
            if size is None:
                try:
                    r = self.session.head(url, timeout=(10, 15), allow_redirects=True)
                    cl = r.headers.get("content-length")
                    if cl:
                        size = int(cl)
                except Exception:
                    pass
            db.insert_file(conn, project_id, file_name, file_type, "NOT_ATTEMPTED", url, size)
            kb = f"{size // 1024} KB" if size else "size unknown"
            print(f"    - {file_name} [NOT_ATTEMPTED] ({kb})")
            return

        status    = "FAILED_SERVER_UNRESPONSIVE"
        file_size = None

        # Pre-check known size from metadata before making any request
        if cap and known_size and known_size > cap:
            status = "FAILED_TOO_LARGE"
            db.insert_file(conn, project_id, file_name, file_type, status, url, known_size)
            print(f"    ✗ {file_name} [FAILED_TOO_LARGE] (metadata: {known_size // 1024} KB)")
            return

        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / file_name
        try:
            # (connect_timeout, read_timeout) — fail fast on hangs
            resp = self.session.get(url, stream=True, timeout=(10, 60))
            if resp.status_code in (401, 403):
                status = "FAILED_LOGIN_REQUIRED"
                file_size = known_size
                if file_size is None:
                    print('shit\n\n\n\n\n')
                    try:
                        r = self.session.head(url, timeout=(10, 15), allow_redirects=True)
                        cl = r.headers.get("content-length")
                        if cl:
                            file_size = int(cl)
                    except Exception:
                        pass
            else:
                resp.raise_for_status()
                # Check Content-Length header before streaming
                content_length = int(resp.headers.get("content-length", 0))
                if cap and content_length and content_length > cap:
                    status    = "FAILED_TOO_LARGE"
                    file_size = content_length
                else:
                    size = 0
                    with open(dest, "wb") as f:
                        for chunk in resp.iter_content(8192):
                            size += len(chunk)
                            if cap and size > cap:
                                dest.unlink(missing_ok=True)
                                status    = "FAILED_TOO_LARGE"
                                file_size = size
                                break
                            f.write(chunk)
                        else:
                            status    = "SUCCEEDED"
                            file_size = size
        except Exception:
            status = "FAILED_SERVER_UNRESPONSIVE"

        db.insert_file(conn, project_id, file_name, file_type, status, url, file_size)
        icon = "✓" if status == "SUCCEEDED" else "✗"
        print(f"    {icon} {file_name} [{status}]")


# ── OAI-PMH ───────────────────────────────────────────────────────────────────

OAI_NS = {
    "oai":    "http://www.openarchives.org/OAI/2.0/",
    "dc":     "http://purl.org/dc/elements/1.1/",
    "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
}


class OAIHarvester(BaseHarvester):
    def __init__(self, oai_url: str, **kwargs):
        super().__init__(**kwargs)
        self.oai_url = oai_url

    def run(self, keywords, extensions, limit) -> list[int]:
        params = {"verb": "ListRecords", "metadataPrefix": "oai_dc"}
        page = 0
        ids  = []
        print(f"\n[OAI] {self.repo_url}")

        while True:
            if limit and len(ids) >= limit:
                print(f"  Result limit reached ({limit}).")
                break
            page += 1

            resp = self.session.get(self.oai_url, params=params, timeout=30)
            resp.raise_for_status()
            root = ET.fromstring(resp.text)

            with db.get_conn() as conn:
                for record in root.findall(".//oai:record", OAI_NS):
                    if limit and len(ids) >= limit:
                        break
                    header = record.find("oai:header", OAI_NS)
                    if header.get("status") == "deleted":
                        continue
                    text = self._record_text(record)
                    kw   = self._match(text, keywords, extensions)
                    if kw:
                        pid = self._process(conn, record, kw)
                        ids.append(pid)
                        title = (self._fields(record, "title") or ["?"])[0][:70]
                        print(f"  + [{kw}] {title}")

            token_el = root.find(".//oai:resumptionToken", OAI_NS)
            print(f"  Page {page} | {len(ids)} matches")
            if token_el is not None and token_el.text:
                params = {"verb": "ListRecords", "resumptionToken": token_el.text}
            else:
                break

        print(f"  Done. {len(ids)} projects from {self.repo_url}")
        return ids

    def download_projects(self, project_ids: list[int], max_file_bytes: int | None = None, download: bool = True):
        print(f"\n[OAI Files] {len(project_ids)} projects from {self.repo_url}")
        base = self.repo_url.rstrip("/")

        for pid in project_ids:
            with db.get_conn() as conn:
                row = conn.execute(
                    "SELECT project_url, download_project_folder FROM projects WHERE id=?", (pid,)
                ).fetchone()
            if not row or not row["project_url"]:
                continue

            try:
                resp = self.session.get(row["project_url"], timeout=30)
                resp.raise_for_status()
                paths = list(dict.fromkeys(
                    re.findall(r'href="(/bitstream/[^"?]+)"', resp.text)
                ))

                dest_dir = self._dest_dir(row["download_project_folder"])
                print(f"  Project {pid} — {len(paths)} file(s)")
                with db.get_conn() as conn:
                    for path in paths:
                        name   = path.split("/")[-1]
                        dl_url = f"{base}{path}"
                        self._save_file(conn, pid, dl_url, name, dest_dir, max_file_bytes, download=download)

            except Exception as e:
                print(f"  ! Project {pid} error: {e}")

    # ── OAI helpers ───────────────────────────────────────────────────────────

    def _fields(self, record, tag: str) -> list[str]:
        return [el.text for el in record.findall(f".//dc:{tag}", OAI_NS) if el.text]

    def _record_text(self, record) -> str:
        return " ".join(
            t for field in ["title", "description", "subject", "type", "format"]
            for t in self._fields(record, field)
        )

    def _project_url(self, record) -> str:
        host = self.repo_url.split("//")[1]
        for el in record.findall(".//dc:identifier", OAI_NS):
            if el.text and host in el.text and "handle" in el.text:
                return el.text.replace("//handle", "/handle")
        return ""

    def _doi(self, record) -> str | None:
        for el in record.findall(".//dc:identifier", OAI_NS):
            t = el.text or ""
            if "doi.org" in t:
                return t
            if t.startswith("doi:") or t.startswith("doi: doi:"):
                raw = t.split("doi:")[-1].strip()
                return f"https://doi.org/{raw}"
        return None

    def _process(self, conn, record, query_string: str) -> int:
        url = self._project_url(record)
        project_id = db.insert_project(conn, {
            "query_string":               query_string,
            "repository_id":              self.repo_id,
            "repository_url":             self.repo_url,
            "project_url":                url,
            "version":                    None,
            "title":                      (self._fields(record, "title")       or [""])[0],
            "description":                (self._fields(record, "description") or [""])[0],
            "language":                   (self._fields(record, "language")    or [None])[0],
            "doi":                        self._doi(record),
            "upload_date":                (self._fields(record, "date")        or [None])[0],
            "download_date":              self._now(),
            "download_repository_folder": self.repo_folder,
            "download_project_folder":    self._project_folder(url),
            "download_version_folder":    None,
            "download_method":            "API-CALL",
        })
        for kw   in self._fields(record, "subject"):     db.insert_keyword(conn, project_id, kw)
        for name in self._fields(record, "creator"):     db.insert_person(conn, project_id, name, "AUTHOR")
        for name in self._fields(record, "contributor"): db.insert_person(conn, project_id, name, "OTHER")
        for lic  in self._fields(record, "rights"):      db.insert_license(conn, project_id, lic)
        return project_id


# ── Dataverse ─────────────────────────────────────────────────────────────────

class DataverseHarvester(BaseHarvester):
    def __init__(self, api_url: str, **kwargs):
        super().__init__(**kwargs)
        self.api_url  = api_url
        self.api_base = api_url.split("/api/")[0]   # e.g. https://ssh.datastations.nl

    def run(self, keywords, extensions, limit) -> list[int]:
        seen      = set()
        ids       = []
        all_terms = list(keywords) + list(extensions)
        print(f"\n[Dataverse] {self.repo_url}")

        for term in all_terms:
            if limit and len(ids) >= limit:
                break
            start = 0
            while True:
                if limit and len(ids) >= limit:
                    break

                resp = self.session.get(self.api_url, params={
                    "q": term, "type": "dataset", "per_page": 100, "start": start,
                }, timeout=30)
                resp.raise_for_status()
                data  = resp.json().get("data", {})
                items = data.get("items", [])
                if not items:
                    break

                with db.get_conn() as conn:
                    for item in items:
                        if limit and len(ids) >= limit:
                            break
                        url = item.get("url", "")
                        if url in seen:
                            continue
                        seen.add(url)
                        pid = self._process(conn, item, term)
                        ids.append(pid)
                        print(f"  + [{term}] {item.get('name', '?')[:70]}")

                start += len(items)
                if start >= data.get("total_count", 0):
                    break

        print(f"  Done. {len(ids)} projects from {self.repo_url}")
        return ids

    def download_projects(self, project_ids: list[int], max_file_bytes: int | None = None, download: bool = True):
        print(f"\n[Dataverse Files] {len(project_ids)} projects from {self.repo_url}")

        for pid in project_ids:
            with db.get_conn() as conn:
                row = conn.execute(
                    "SELECT doi, download_project_folder FROM projects WHERE id=?", (pid,)
                ).fetchone()
            if not row or not row["doi"]:
                continue

            persistent_id = row["doi"].replace("https://doi.org/", "doi:")
            try:
                resp = self.session.get(
                    f"{self.api_base}/api/datasets/:persistentId/",
                    params={"persistentId": persistent_id}, timeout=30,
                )
                resp.raise_for_status()
                files = resp.json().get("data", {}).get("latestVersion", {}).get("files", [])

                dest_dir = self._dest_dir(row["download_project_folder"])
                print(f"  Project {pid} — {len(files)} file(s)")
                with db.get_conn() as conn:
                    for entry in files:
                        df      = entry.get("dataFile", {})
                        file_id = df.get("id")
                        name    = df.get("filename", f"file_{file_id}")
                        dl_url  = f"{self.api_base}/api/access/datafile/{file_id}"
                        self._save_file(conn, pid, dl_url, name, dest_dir, max_file_bytes,
                                        known_size=df.get("filesize"), download=download)

            except Exception as e:
                print(f"  ! Project {pid} error: {e}")

    def _process(self, conn, item: dict, query_string: str) -> int:
        url = item.get("url", "")
        doi = item.get("global_id", "")
        if doi and not doi.startswith("http"):
            doi = f"https://doi.org/{doi.removeprefix('doi:').strip()}"

        project_id = db.insert_project(conn, {
            "query_string":               query_string,
            "repository_id":              self.repo_id,
            "repository_url":             self.repo_url,
            "project_url":                url,
            "version":                    None,
            "title":                      item.get("name", ""),
            "description":                item.get("description", ""),
            "language":                   None,
            "doi":                        doi or None,
            "upload_date":                item.get("published_at", None),
            "download_date":              self._now(),
            "download_repository_folder": self.repo_folder,
            "download_project_folder":    self._project_folder(url),
            "download_version_folder":    None,
            "download_method":            "API-CALL",
        })
        for subj in item.get("subjects", []):
            db.insert_keyword(conn, project_id, subj)
        for auth in item.get("authors", []):
            name = auth.get("name", "") if isinstance(auth, dict) else str(auth)
            if name: db.insert_person(conn, project_id, name, "AUTHOR")
        return project_id
