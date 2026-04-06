import io
import random
import re
import tarfile
import tempfile
import time
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import py7zr
import rarfile
import requests
import db
import config


class RateLimitError(Exception):
    """Raised when a repo signals rate limiting (429) or repeated connection failures."""

CET = ZoneInfo("Europe/Berlin")

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

    def _get(self, url: str, **kwargs) -> requests.Response:
        """session.get() with jitter and 429 → RateLimitError."""
        time.sleep(random.uniform(*config.REQUEST_JITTER))
        resp = self.session.get(url, **kwargs)
        if resp.status_code == 429:
            raise RateLimitError(f"429 Too Many Requests: {url}")
        return resp

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
                   max_bytes: int | None = None, known_size: int | None = None,
                   download: bool = True, query_string: str = ""):
        cap       = max_bytes if max_bytes is not None else MAX_FILE_BYTES
        suffix    = Path(file_name).suffix.lower()
        file_type = suffix.lstrip(".") or "unknown"

        # Route archives to dedicated handler
        if suffix in config.COMPRESS_EXTENSIONS:
            self._handle_archive(conn, project_id, url, file_name, dest_dir,
                                 cap, known_size, download, query_string)
            return

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

        if cap and known_size and known_size > cap:
            status = "FAILED_TOO_LARGE"
            db.insert_file(conn, project_id, file_name, file_type, status, url, known_size)
            print(f"    ✗ {file_name} [FAILED_TOO_LARGE] (metadata: {known_size // 1024} KB)")
            return

        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / file_name
        try:
            resp = self.session.get(url, stream=True, timeout=(10, 60))
            if resp.status_code in (401, 403):
                status = "FAILED_LOGIN_REQUIRED"
                file_size = known_size
                if file_size is None:
                    try:
                        r = self.session.head(url, timeout=(10, 15), allow_redirects=True)
                        cl = r.headers.get("content-length")
                        if cl:
                            file_size = int(cl)
                    except Exception:
                        pass
            else:
                resp.raise_for_status()
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

    def _handle_archive(self, conn, project_id: int, url: str, file_name: str,
                        dest_dir: Path, cap: int, known_size: int | None,
                        download: bool, query_string: str):
        """Handle archive files: peek inside, decide whether to download, record inner files."""
        suffix     = Path(file_name).suffix.lower()
        multiplier = config.ZIP_OVERSIZED_MULTIPLIER
        extensions = set(config.QDA_EXTENSIONS)
        by_keyword = query_string.lower() not in {e.lstrip(".") for e in config.QDA_EXTENSIONS} \
                     and query_string not in {e.lstrip(".") for e in config.QDA_EXTENSIONS}

        # Get archive size via HEAD if not known
        arc_size = known_size
        if arc_size is None:
            try:
                r = self.session.head(url, timeout=(10, 15), allow_redirects=True)
                cl = r.headers.get("content-length")
                if cl:
                    arc_size = int(cl)
            except Exception:
                pass

        if not download:
            # Just peek and record inner files as NOT_ATTEMPTED
            inner = self._peek_archive(url, suffix, arc_size, cap, multiplier)
            if inner is None:
                # couldn't peek — record the archive itself
                db.insert_file(conn, project_id, file_name, suffix.lstrip(".") or "archive",
                               "NOT_ATTEMPTED", url, arc_size)
                print(f"    - {file_name} [NOT_ATTEMPTED] (archive, could not peek)")
                return
            relevant = [f for f in inner if Path(f['name']).suffix.lower() in extensions]
            to_record = relevant if not by_keyword else inner
            if not to_record and not by_keyword:
                print(f"    - {file_name} skipped (archive, no relevant files inside)")
                return
            for f in to_record:
                ft = Path(f['name']).suffix.lstrip(".").lower() or "unknown"
                db.insert_file(conn, project_id, f['name'], ft, "NOT_ATTEMPTED",
                               url, f['size'], zip_path=file_name)
                print(f"    - {f['name']} [NOT_ATTEMPTED] in {file_name}")
            return

        # --- downloading ---
        # Size gate: zip_size <= file_count * cap AND no inner file > cap * multiplier
        inner = self._peek_archive(url, suffix, arc_size, cap, multiplier)

        if inner is not None:
            relevant = [f for f in inner if Path(f['name']).suffix.lower() in extensions]
            if not by_keyword and not relevant:
                print(f"    - {file_name} skipped (no relevant files inside)")
                return
            file_count = len(inner)
            # aggregate size check
            if cap and arc_size and arc_size > file_count * cap:
                ft = suffix.lstrip(".") or "archive"
                db.insert_file(conn, project_id, file_name, ft, "FAILED_TOO_LARGE", url, arc_size)
                print(f"    ✗ {file_name} [FAILED_TOO_LARGE] archive too large")
                return
            # individual file check
            oversized = [f for f in inner if cap and f['size'] and f['size'] > cap * multiplier]
            if oversized:
                ft = suffix.lstrip(".") or "archive"
                db.insert_file(conn, project_id, file_name, ft, "FAILED_TOO_LARGE", url, arc_size)
                print(f"    ✗ {file_name} [FAILED_TOO_LARGE] contains oversized file(s)")
                return
            to_record = relevant if not by_keyword else inner
        else:
            # Can't peek — fall back to raw size check with multiplier
            if cap and arc_size and arc_size > cap * multiplier:
                ft = suffix.lstrip(".") or "archive"
                db.insert_file(conn, project_id, file_name, ft, "FAILED_TOO_LARGE", url, arc_size)
                print(f"    ✗ {file_name} [FAILED_TOO_LARGE]")
                return
            to_record = None  # record all after download

        # Download the archive
        dest_dir.mkdir(parents=True, exist_ok=True)
        arc_dest = dest_dir / file_name
        status = "FAILED_SERVER_UNRESPONSIVE"
        try:
            resp = self.session.get(url, stream=True, timeout=(10, 60))
            if resp.status_code in (401, 403):
                status = "FAILED_LOGIN_REQUIRED"
                ft = suffix.lstrip(".") or "archive"
                db.insert_file(conn, project_id, file_name, ft, status, url, arc_size)
                print(f"    ✗ {file_name} [{status}]")
                return
            resp.raise_for_status()
            size = 0
            with open(arc_dest, "wb") as f:
                for chunk in resp.iter_content(8192):
                    size += len(chunk)
                    f.write(chunk)
            status = "SUCCEEDED"
        except Exception:
            ft = suffix.lstrip(".") or "archive"
            db.insert_file(conn, project_id, file_name, ft, status, url, arc_size)
            print(f"    ✗ {file_name} [{status}]")
            return

        # If we couldn't peek before, read inner listing now from downloaded file
        if to_record is None:
            inner = self._list_archive(arc_dest, suffix)
            if inner is not None:
                relevant = [f for f in inner if Path(f['name']).suffix.lower() in extensions]
                to_record = relevant if not by_keyword else inner
                if not by_keyword and not to_record:
                    arc_dest.unlink(missing_ok=True)
                    print(f"    - {file_name} deleted (no relevant files inside)")
                    return
            else:
                to_record = [{"name": file_name, "size": size}]

        zip_path_str = str(arc_dest)
        for f in to_record:
            ft = Path(f['name']).suffix.lstrip(".").lower() or "unknown"
            db.insert_file(conn, project_id, f['name'], ft, "SUCCEEDED",
                           url, f['size'], zip_path=zip_path_str)
        print(f"    ✓ {file_name} [SUCCEEDED] — {len(to_record)} inner file(s) recorded")

    def _peek_archive(self, url: str, suffix: str, arc_size: int | None,
                      cap: int, multiplier: int) -> list[dict] | None:
        """Try to read archive listing without full download. Returns list of {name, size} or None."""
        try:
            if suffix == ".zip":
                return self._peek_zip(url, arc_size)
            # For other types, only peek if compressed size <= cap * multiplier
            if cap and arc_size and arc_size > cap * multiplier:
                return None
            # Download to temp and list
            resp = self.session.get(url, timeout=(10, 120))
            resp.raise_for_status()
            with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
                tmp.write(resp.content)
                tmp_path = Path(tmp.name)
            try:
                return self._list_archive(tmp_path, suffix)
            finally:
                tmp_path.unlink(missing_ok=True)
        except Exception:
            return None

    def _peek_zip(self, url: str, arc_size: int | None) -> list[dict] | None:
        """Read ZIP central directory via HTTP Range request — no full download."""
        try:
            # Fetch last 65KB which contains the ZIP end-of-central-directory
            fetch_bytes = min(65536, arc_size) if arc_size else 65536
            r = self.session.get(url, headers={"Range": f"bytes=-{fetch_bytes}"}, timeout=(10, 30))
            if r.status_code not in (206, 200):
                return None
            data = r.content
            # Find end-of-central-directory signature
            eocd = data.rfind(b'PK\x05\x06')
            if eocd == -1:
                return None
            # Need the central directory — may require a second request for larger ZIPs
            cd_size   = int.from_bytes(data[eocd+12:eocd+16], 'little')
            cd_offset = int.from_bytes(data[eocd+16:eocd+20], 'little')
            r2 = self.session.get(url, headers={"Range": f"bytes={cd_offset}-{cd_offset+cd_size-1}"},
                                  timeout=(10, 30))
            if r2.status_code not in (206, 200):
                return None
            buf = io.BytesIO(r2.content + data[eocd:])
            with zipfile.ZipFile(buf) as zf:
                return [{"name": i.filename, "size": i.file_size}
                        for i in zf.infolist() if not i.is_dir()]
        except Exception:
            return None

    def _list_archive(self, path: Path, suffix: str) -> list[dict] | None:
        """List contents of a local archive file."""
        try:
            if suffix == ".zip":
                with zipfile.ZipFile(path) as zf:
                    return [{"name": i.filename, "size": i.file_size}
                            for i in zf.infolist() if not i.is_dir()]
            if suffix in (".tar", ".gz", ".tgz", ".bz2", ".tar.gz"):
                with tarfile.open(path) as tf:
                    return [{"name": m.name, "size": m.size} for m in tf.getmembers() if m.isfile()]
            if suffix == ".rar":
                with rarfile.RarFile(path) as rf:
                    return [{"name": i.filename, "size": i.file_size}
                            for i in rf.infolist() if not i.is_dir()]
            if suffix == ".7z":
                with py7zr.SevenZipFile(path, mode='r') as sz:
                    return [{"name": f.filename, "size": f.uncompressed or 0}
                            for f in sz.list() if not f.is_directory]
        except Exception:
            return None


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

    def run(self, keywords, extensions, limit,
            max_file_bytes: int | None = None, download: bool = False) -> list[int]:
        page     = 0
        ids      = []
        run_start = self._now()
        print(f"\n[OAI] {self.repo_url}")

        # Load checkpoint
        with db.get_conn() as conn:
            state = db.get_harvest_state(conn, self.repo_id)
        saved_token = state["resumption_token"]
        from_date   = state["last_harvested_at"]

        # Save run_start now — even if interrupted, next run can use it as `from`
        with db.get_conn() as conn:
            db.save_harvest_state(conn, self.repo_id, last_harvested_at=run_start)

        params = self._build_params(saved_token, from_date)

        while True:
            if limit and len(ids) >= limit:
                print(f"  Result limit reached ({limit}).")
                break
            page += 1

            resp = self._get(self.oai_url, params=params, timeout=30)
            resp.raise_for_status()
            root = ET.fromstring(resp.text)

            # Check for OAI-PMH level errors (e.g. badResumptionToken)
            error_el = root.find(".//oai:error", OAI_NS)
            if error_el is not None:
                code = error_el.get("code", "")
                print(f"  OAI error: {code} — {error_el.text}")
                if code == "badResumptionToken":
                    print("  Token expired — falling back to incremental/full harvest")
                    with db.get_conn() as conn:
                        db.save_harvest_state(conn, self.repo_id, resumption_token=None)
                    params = self._build_params(None, from_date)
                    page   = 0
                    continue
                break

            for record in root.findall(".//oai:record", OAI_NS):
                if limit and len(ids) >= limit:
                    break
                header = record.find("oai:header", OAI_NS)
                if header.get("status") == "deleted":
                    continue
                text = self._record_text(record)
                kw   = self._match(text, keywords, extensions)
                if kw:
                    with db.get_conn() as conn:
                        pid = self._process(conn, record, kw)
                    if pid is None:
                        continue
                    ids.append(pid)
                    title = (self._fields(record, "title") or ["?"])[0][:70]
                    print(f"  + [{kw}] {title}")
                    self._process_files(pid, max_file_bytes, download)

            token_el = root.find(".//oai:resumptionToken", OAI_NS)
            print(f"  Page {page} | {len(ids)} matches")

            if token_el is not None and token_el.text:
                token = token_el.text
                with db.get_conn() as conn:
                    db.save_harvest_state(conn, self.repo_id, resumption_token=token)
                params = {"verb": "ListRecords", "resumptionToken": token}
            else:
                # Harvest complete — clear token (last_harvested_at already saved above)
                with db.get_conn() as conn:
                    db.save_harvest_state(conn, self.repo_id, resumption_token=None)
                break

        print(f"  Done. {len(ids)} projects from {self.repo_url}")
        return ids

    def _process_files(self, pid: int, max_file_bytes: int | None = None, download: bool = False):
        base = self.repo_url.rstrip("/")
        with db.get_conn() as conn:
            row = conn.execute(
                "SELECT project_url, download_project_folder, query_string FROM projects WHERE id=?", (pid,)
            ).fetchone()
        if not row or not row["project_url"]:
            return
        try:
            resp = self._get(row["project_url"], timeout=30)
            resp.raise_for_status()
            paths = list(dict.fromkeys(
                re.findall(r'href="(/bitstream/[^"?]+)"', resp.text)
            ))
            dest_dir     = self._dest_dir(row["download_project_folder"])
            query_string = row["query_string"] or ""
            print(f"    {len(paths)} file(s) for project {pid}")
            with db.get_conn() as conn:
                for path in paths:
                    name   = path.split("/")[-1]
                    dl_url = f"{base}{path}"
                    self._save_file(conn, pid, dl_url, name, dest_dir, max_file_bytes,
                                    download=download, query_string=query_string)
        except Exception as e:
            print(f"  ! Project {pid} files error: {e}")

    def download_projects(self, project_ids: list[int], max_file_bytes: int | None = None, download: bool = True):
        print(f"\n[OAI Files] {len(project_ids)} projects from {self.repo_url}")
        for pid in project_ids:
            self._process_files(pid, max_file_bytes, download)

    def _build_params(self, token: str | None, from_date: str | None) -> dict:
        if token:
            print(f"  Resuming from checkpoint token")
            return {"verb": "ListRecords", "resumptionToken": token}
        if from_date:
            from_short = from_date[:10]
            print(f"  Incremental harvest from {from_short}")
            return {"verb": "ListRecords", "metadataPrefix": "oai_dc", "from": from_short}
        print("  Full harvest (first run)")
        return {"verb": "ListRecords", "metadataPrefix": "oai_dc"}

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

    def _process(self, conn, record, query_string: str) -> int | None:
        url = self._project_url(record)
        existing = db.project_exists(conn, self.repo_id, url)
        if existing:
            print(f"  ~ skipped (already in DB): {url}")
            return None
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
    def __init__(self, api_url: str, api_token: str | None = None, **kwargs):
        super().__init__(**kwargs)
        self.api_url  = api_url
        self.api_base = api_url.split("/api/")[0]
        if api_token:
            self.session.headers.update({"X-Dataverse-key": api_token})

    def run(self, keywords, extensions, limit,
            max_file_bytes: int | None = None, download: bool = False) -> list[int]:
        seen      = set()
        ids       = []
        all_terms = list(keywords) + list(extensions)
        run_start = self._now()
        print(f"\n[Dataverse] {self.repo_url}")

        for term in all_terms:
            if limit and len(ids) >= limit:
                break
            start = 0
            while True:
                if limit and len(ids) >= limit:
                    break

                resp = self._get(self.api_url, params={
                    "q": term, "type": "dataset", "per_page": 100, "start": start,
                }, timeout=30)
                resp.raise_for_status()
                data  = resp.json().get("data", {})
                items = data.get("items", [])
                if not items:
                    break

                for item in items:
                    if limit and len(ids) >= limit:
                        break
                    url = item.get("url", "")
                    if url in seen:
                        continue
                    seen.add(url)
                    with db.get_conn() as conn:
                        pid = self._process(conn, item, term)
                    if pid is None:
                        continue
                    ids.append(pid)
                    print(f"  + [{term}] {item.get('name', '?')[:70]}")
                    self._process_files(pid, max_file_bytes, download)

                start += len(items)
                if start >= data.get("total_count", 0):
                    break

        with db.get_conn() as conn:
            db.save_harvest_state(conn, self.repo_id, last_harvested_at=run_start)
        print(f"  Done. {len(ids)} projects from {self.repo_url}")
        return ids

    def _process_files(self, pid: int, max_file_bytes: int | None = None, download: bool = False):
        with db.get_conn() as conn:
            row = conn.execute(
                "SELECT doi, download_project_folder, query_string FROM projects WHERE id=?", (pid,)
            ).fetchone()
        if not row or not row["doi"]:
            return
        persistent_id = row["doi"].replace("https://doi.org/", "doi:")
        try:
            resp = self._get(
                f"{self.api_base}/api/datasets/:persistentId/",
                params={"persistentId": persistent_id}, timeout=30,
            )
            resp.raise_for_status()
            files        = resp.json().get("data", {}).get("latestVersion", {}).get("files", [])
            dest_dir     = self._dest_dir(row["download_project_folder"])
            query_string = row["query_string"] or ""
            has_token    = bool(self.session.headers.get("X-Dataverse-key"))
            print(f"    {len(files)} file(s) for project {pid}")
            with db.get_conn() as conn:
                for entry in files:
                    df         = entry.get("dataFile", {})
                    file_id    = df.get("id")
                    name       = df.get("filename", f"file_{file_id}")
                    dl_url     = f"{self.api_base}/api/access/datafile/{file_id}"
                    restricted = entry.get("restricted", False) or df.get("restricted", False)
                    if restricted and not has_token:
                        file_type = Path(name).suffix.lstrip(".").lower() or "unknown"
                        db.insert_file(conn, pid, name, file_type, "FAILED_LOGIN_REQUIRED",
                                       dl_url, df.get("filesize"))
                        kb = f"{df['filesize'] // 1024} KB" if df.get("filesize") else "size unknown"
                        print(f"    ✗ {name} [FAILED_LOGIN_REQUIRED] restricted, no token ({kb})")
                        continue
                    self._save_file(conn, pid, dl_url, name, dest_dir, max_file_bytes,
                                    known_size=df.get("filesize"), download=download,
                                    query_string=query_string)
        except Exception as e:
            print(f"  ! Project {pid} files error: {e}")

    def download_projects(self, project_ids: list[int], max_file_bytes: int | None = None, download: bool = True):
        print(f"\n[Dataverse Files] {len(project_ids)} projects from {self.repo_url}")
        for pid in project_ids:
            self._process_files(pid, max_file_bytes, download)

    def _process(self, conn, item: dict, query_string: str) -> int | None:
        url = item.get("url", "")
        existing = db.project_exists(conn, self.repo_id, url)
        if existing:
            print(f"  ~ skipped (already in DB): {url}")
            return None
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
