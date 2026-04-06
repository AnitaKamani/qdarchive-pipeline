import os
from dotenv import load_dotenv
load_dotenv()

# ── match terms ───────────────────────────────────────────────────────────────

KEYWORDS = [
    # QDA formats / standards
    "qdpx", "mqda", "mqdai", "refi-qda", "refi qda",
    # QDA software names
    "maxqda", "atlas.ti", "atlasti", "nvivo", "dedoose",
    "quirkos", "transana", "f4analyse", "f4analyze",
    # Research methods
    "interview study", "qualitative data", "qualitative research",
]

QDA_EXTENSIONS = [
    # MaxQDA
    ".mx22", ".mx20", ".mx18", ".mx12", ".mex", ".mxmsg", ".mxdb",
    # ATLAS.ti
    ".atlproj", ".atlcb",
    # NVivo
    ".nvp", ".nvpx", ".nvcx",
    # REFI-QDA universal exchange format
    ".qdpx",
    # Others
    ".f4a", ".qrk",
]

# ── repositories ──────────────────────────────────────────────────────────────
# Add a new repo here; "type" must match a key in harvest.py HARVESTER_MAP.

REPOS = [
    {
        "type":        "oai",
        "repo_id":     16,
        "repo_url":    "https://opendata.uni-halle.de",
        "repo_folder": "uni-halle",
        "oai_url":     "https://opendata.uni-halle.de/oai/request",
    },
    {
        "type":        "dataverse",
        "repo_id":     5,
        "repo_url":    "https://dans.knaw.nl",
        "repo_folder": "dans",
        "api_url":     "https://ssh.datastations.nl/api/search",
        "api_token":   os.getenv("DANS_API_TOKEN"),
    },
]

# ── limits ────────────────────────────────────────────────────────────────────

PAGE_LIMIT = 20   # pages per repo; set to None for a full harvest
