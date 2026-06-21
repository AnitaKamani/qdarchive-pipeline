# QDArchive Database Schema Overview

**Database:** `23727550-sq26.db`  
**Generated:** 2026-06-21 16:00:22  
**Tables:** 10  
**Views:** 1

---

## Tables

### `classification_inputs`

**Row count:** 0

| # | Column | Type | PK | Nullable | Default |
|---|--------|------|----|----------|---------|
| 0 | `id` | `INTEGER` | PK1 | nullable | — |
| 1 | `target_type` | `TEXT` |  | NOT NULL | — |
| 2 | `target_id` | `INTEGER` |  | NOT NULL | — |
| 3 | `project_id` | `INTEGER` |  | nullable | — |
| 4 | `input_text` | `TEXT` |  | NOT NULL | — |
| 5 | `created_at` | `TEXT` |  | nullable | `CURRENT_TIMESTAMP` |

---

### `file_classifications`

**Row count:** 0

| # | Column | Type | PK | Nullable | Default |
|---|--------|------|----|----------|---------|
| 0 | `id` | `INTEGER` | PK1 | nullable | — |
| 1 | `file_id` | `INTEGER` |  | NOT NULL | — |
| 2 | `project_id` | `INTEGER` |  | NOT NULL | — |
| 3 | `primary_class_code` | `TEXT` |  | nullable | — |
| 4 | `secondary_class_code` | `TEXT` |  | nullable | — |
| 5 | `tags` | `TEXT` |  | nullable | — |
| 6 | `confidence` | `REAL` |  | nullable | — |
| 7 | `method` | `TEXT` |  | nullable | — |
| 8 | `reason` | `TEXT` |  | nullable | — |
| 9 | `created_at` | `TEXT` |  | nullable | `CURRENT_TIMESTAMP` |

**Foreign keys:**

| Column | References | On Update | On Delete |
|--------|------------|-----------|-----------|
| `secondary_class_code` | `isic_divisions(code)` | NO ACTION | NO ACTION |
| `primary_class_code` | `isic_divisions(code)` | NO ACTION | NO ACTION |
| `project_id` | `projects(id)` | NO ACTION | NO ACTION |
| `file_id` | `files(id)` | NO ACTION | NO ACTION |

---

### `files`

**Row count:** 143,696

| # | Column | Type | PK | Nullable | Default |
|---|--------|------|----|----------|---------|
| 0 | `id` | `INTEGER` | PK1 | nullable | — |
| 1 | `project_id` | `INTEGER` |  | NOT NULL | — |
| 2 | `file_name` | `TEXT` |  | NOT NULL | — |
| 3 | `file_type` | `TEXT` |  | NOT NULL | — |
| 4 | `file_url` | `TEXT` |  | nullable | — |
| 5 | `file_size` | `INTEGER` |  | nullable | — |
| 6 | `zip_path` | `TEXT` |  | nullable | — |
| 7 | `status` | `TEXT` |  | NOT NULL | — |

**Foreign keys:**

| Column | References | On Update | On Delete |
|--------|------------|-----------|-----------|
| `project_id` | `projects(id)` | NO ACTION | NO ACTION |

**CHECK constraints:**

- `status IN (
                    'SUCCEEDED','FAILED_SERVER_UNRESPONSIVE',
                    'FAILED_LOGIN_REQUIRED','FAILED_TOO_LARGE','NOT_ATTEMPTED'`

---

### `isic_divisions`

**Row count:** 87

| # | Column | Type | PK | Nullable | Default |
|---|--------|------|----|----------|---------|
| 0 | `code` | `TEXT` | PK1 | nullable | — |
| 1 | `section_code` | `TEXT` |  | nullable | — |
| 2 | `division` | `INTEGER` |  | nullable | — |
| 3 | `title` | `TEXT` |  | NOT NULL | — |
| 4 | `description` | `TEXT` |  | nullable | — |

---

### `keywords`

**Row count:** 3,832

| # | Column | Type | PK | Nullable | Default |
|---|--------|------|----|----------|---------|
| 0 | `id` | `INTEGER` | PK1 | nullable | — |
| 1 | `project_id` | `INTEGER` |  | NOT NULL | — |
| 2 | `keyword` | `TEXT` |  | NOT NULL | — |

**Foreign keys:**

| Column | References | On Update | On Delete |
|--------|------------|-----------|-----------|
| `project_id` | `projects(id)` | NO ACTION | NO ACTION |

---

### `licenses`

**Row count:** 62

| # | Column | Type | PK | Nullable | Default |
|---|--------|------|----|----------|---------|
| 0 | `id` | `INTEGER` | PK1 | nullable | — |
| 1 | `project_id` | `INTEGER` |  | NOT NULL | — |
| 2 | `license` | `TEXT` |  | NOT NULL | — |

**Foreign keys:**

| Column | References | On Update | On Delete |
|--------|------------|-----------|-----------|
| `project_id` | `projects(id)` | NO ACTION | NO ACTION |

---

### `person_role`

**Row count:** 4,111

| # | Column | Type | PK | Nullable | Default |
|---|--------|------|----|----------|---------|
| 0 | `id` | `INTEGER` | PK1 | nullable | — |
| 1 | `project_id` | `INTEGER` |  | NOT NULL | — |
| 2 | `name` | `TEXT` |  | NOT NULL | — |
| 3 | `role` | `TEXT` |  | NOT NULL | — |

**Foreign keys:**

| Column | References | On Update | On Delete |
|--------|------------|-----------|-----------|
| `project_id` | `projects(id)` | NO ACTION | NO ACTION |

**CHECK constraints:**

- `role IN ('UPLOADER','AUTHOR','OWNER','OTHER','UNKNOWN'`

---

### `project_classifications`

**Row count:** 0

| # | Column | Type | PK | Nullable | Default |
|---|--------|------|----|----------|---------|
| 0 | `id` | `INTEGER` | PK1 | nullable | — |
| 1 | `project_id` | `INTEGER` |  | NOT NULL | — |
| 2 | `primary_class_code` | `TEXT` |  | nullable | — |
| 3 | `secondary_class_code` | `TEXT` |  | nullable | — |
| 4 | `tags` | `TEXT` |  | nullable | — |
| 5 | `confidence` | `REAL` |  | nullable | — |
| 6 | `method` | `TEXT` |  | nullable | — |
| 7 | `reason` | `TEXT` |  | nullable | — |
| 8 | `created_at` | `TEXT` |  | nullable | `CURRENT_TIMESTAMP` |

**Foreign keys:**

| Column | References | On Update | On Delete |
|--------|------------|-----------|-----------|
| `secondary_class_code` | `isic_divisions(code)` | NO ACTION | NO ACTION |
| `primary_class_code` | `isic_divisions(code)` | NO ACTION | NO ACTION |
| `project_id` | `projects(id)` | NO ACTION | NO ACTION |

---

### `projects`

**Row count:** 2,684

| # | Column | Type | PK | Nullable | Default |
|---|--------|------|----|----------|---------|
| 0 | `id` | `INTEGER` | PK1 | nullable | — |
| 1 | `query_string` | `TEXT` |  | nullable | — |
| 2 | `repository_id` | `INTEGER` |  | NOT NULL | — |
| 3 | `repository_url` | `TEXT` |  | NOT NULL | — |
| 4 | `project_url` | `TEXT` |  | NOT NULL | — |
| 5 | `version` | `TEXT` |  | nullable | — |
| 6 | `title` | `TEXT` |  | NOT NULL | — |
| 7 | `description` | `TEXT` |  | NOT NULL | — |
| 8 | `language` | `TEXT` |  | nullable | — |
| 9 | `doi` | `TEXT` |  | nullable | — |
| 10 | `upload_date` | `TEXT` |  | nullable | — |
| 11 | `download_date` | `TEXT` |  | NOT NULL | — |
| 12 | `download_repository_folder` | `TEXT` |  | NOT NULL | — |
| 13 | `download_project_folder` | `TEXT` |  | NOT NULL | — |
| 14 | `download_version_folder` | `TEXT` |  | nullable | — |
| 15 | `download_method` | `TEXT` |  | NOT NULL | — |
| 16 | `project_type` | `TEXT` |  | nullable | — |

**CHECK constraints:**

- `download_method IN ('SCRAPING','API-CALL'`

---

### `sqlite_sequence`

**Row count:** 0

| # | Column | Type | PK | Nullable | Default |
|---|--------|------|----|----------|---------|
| 0 | `name` | `—` |  | nullable | — |
| 1 | `seq` | `—` |  | nullable | — |

---

## Views

### `v_files`

```sql
CREATE VIEW v_files AS
        SELECT
            f.id                                AS file_id,
            f.project_id,
            p.repository_id                     AS repo_id,
            p.project_url,
            f.file_url,
            f.file_name,
            ROUND(f.file_size / 1048576.0, 2)  AS file_size_mb,
            f.status,
            p.download_date
        FROM files f
        JOIN projects p ON p.id = f.project_id
```
