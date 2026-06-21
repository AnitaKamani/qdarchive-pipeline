"""
ISIC Rev. 5 classification providers for project records.

Providers:
  local-dry-run   Keyword heuristic, no API call — pipeline testing only.
  openai          GPT model via OPENAI_API_KEY; strict JSON output.

Both return a Result dict:
  {
    "primary_class_code":   str,
    "secondary_class_code": str | None,
    "tags":                 list[str],
    "confidence":           float,
    "reason":               str,
  }
"""

from __future__ import annotations

import json
import os
import re
from typing import Any

# ---------------------------------------------------------------------------
# Shared types
# ---------------------------------------------------------------------------

Result = dict[str, Any]

_EMPTY_RESULT: Result = {
    "primary_class_code": None,
    "secondary_class_code": None,
    "tags": [],
    "confidence": 0.0,
    "reason": "",
}


def _make_error(reason: str) -> Result:
    return {**_EMPTY_RESULT, "reason": reason}


# ---------------------------------------------------------------------------
# local-dry-run provider
# ---------------------------------------------------------------------------

_KEYWORD_RULES: list[tuple[list[str], str]] = [
    (["education", "school", "student", "teacher", "university", "learning", "curriculum", "classroom"], "Q85"),
    (["health", "medical", "patient", "hospital", "disease", "clinical", "nursing", "therapy"], "R86"),
    (["social work", "welfare", "poverty", "inequality", "social service", "community service"], "R88"),
    (["community", "family", "household", "neighbourhood", "civil society"], "T94"),
    (["agriculture", "farm", "crop", "livestock", "soil", "irrigation", "harvest", "food security"], "A01"),
    (["crime", "police", "justice", "court", "prison", "offend", "criminal", "law enforcement"], "N69"),
    (["government", "public administration", "policy", "regulation", "compulsory", "defence"], "P84"),
    (["business", "company", "management", "enterprise", "managerial", "consultancy"], "N70"),
    (["environment", "climate", "emission", "carbon", "pollution", "biodiversity", "ecology"], "E39"),
    (["water", "ocean", "river", "flood", "drought", "sanitation", "wastewater"], "E36"),
    (["transport", "commute", "mobility", "vehicle", "road", "traffic", "pipeline"], "H49"),
    (["media", "journalism", "news", "broadcast", "communication", "publishing"], "J60"),
    (["research", "science", "experiment", "laboratory", "innovation", "development"], "N72"),
    (["technology", "software", "digital", "internet", "data", "computing", "information"], "N74"),
    (["labour", "work", "employment", "occupation", "wage", "worker", "workforce"], "O78"),
    (["gender", "women", "feminist", "sexuality", "identity", "inequality"], "R88"),
    (["migration", "refugee", "asylum", "immigrant", "diaspora", "stateless"], "P84"),
    (["culture", "art", "museum", "heritage", "music", "creative", "performing"], "S90"),
    (["library", "archive", "heritage", "cultural institution"], "S91"),
    (["mental", "psychology", "behaviour", "cognition", "emotion", "wellbeing", "psychiatric"], "R86"),
    (["child", "youth", "adolescent", "infant", "parenting", "childcare"], "R88"),
    (["urban", "city", "housing", "spatial", "planning", "architecture", "engineering"], "N71"),
    (["energy", "electricity", "renewable", "solar", "wind", "fossil fuel", "gas"], "D35"),
    (["sport", "recreation", "leisure", "physical activity", "fitness"], "S93"),
    (["gambling", "betting", "lottery"], "S92"),
    (["legal", "accounting", "audit", "tax", "notary"], "N69"),
    (["advertising", "market research", "public relations", "marketing"], "N73"),
    (["veterinary", "animal health", "livestock disease"], "N75"),
    (["food", "beverage", "nutrition", "diet", "eating"], "C10"),
]

_DEFAULT_CODE = "N72"


def classify_local_dry_run(input_text: str, valid_codes: set[str]) -> Result:
    lower = input_text.lower()
    best_code = _DEFAULT_CODE
    best_score = 0
    matched_tags: list[str] = []

    for keywords, code in _KEYWORD_RULES:
        if code not in valid_codes:
            continue
        hits = [kw for kw in keywords if kw in lower]
        if len(hits) > best_score:
            best_score = len(hits)
            best_code = code
            matched_tags = hits

    if best_code not in valid_codes:
        best_code = next(iter(valid_codes)) if valid_codes else _DEFAULT_CODE

    confidence = min(0.3 + 0.07 * best_score, 0.75)
    reason = f"keyword match: {', '.join(matched_tags[:5])}" if matched_tags else "default fallback"

    return {
        "primary_class_code": best_code,
        "secondary_class_code": None,
        "tags": matched_tags[:10],
        "confidence": round(confidence, 2),
        "reason": reason,
    }


# ---------------------------------------------------------------------------
# openai provider
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = (
    "You are an expert research classifier. Given a description of a qualitative research project, "
    "classify it into the most appropriate ISIC Rev. 5 division. "
    "You must respond only with valid JSON matching the schema below — no markdown, no explanation outside the JSON."
)

_OUTPUT_SCHEMA = """\
{
  "primary_class_code": "<ISIC division code, e.g. P85>",
  "secondary_class_code": "<ISIC division code or null>",
  "tags": ["<keyword1>", "<keyword2>"],
  "confidence": <float 0.0–1.0>,
  "reason": "<one sentence>"
}"""


def _build_divisions_list(divisions: list[dict]) -> str:
    lines = [f"  {d['code']} — {d['title']}" for d in divisions]
    return "\n".join(lines)


def _build_user_prompt(input_text: str, divisions: list[dict], max_chars: int) -> str:
    text = input_text[:max_chars]
    div_list = _build_divisions_list(divisions)
    return (
        f"Project description:\n{text}\n\n"
        f"Allowed ISIC Rev. 5 divisions:\n{div_list}\n\n"
        f"Output schema:\n{_OUTPUT_SCHEMA}"
    )


def _validate_result(result: dict, valid_codes: set[str]) -> list[str]:
    errors = []
    code = result.get("primary_class_code")
    if not code or code not in valid_codes:
        errors.append(f"primary_class_code '{code}' not in isic_divisions")
    sec = result.get("secondary_class_code")
    if sec is not None and sec not in valid_codes:
        errors.append(f"secondary_class_code '{sec}' not in isic_divisions")
    conf = result.get("confidence")
    if conf is None or not isinstance(conf, (int, float)) or not (0.0 <= conf <= 1.0):
        errors.append(f"confidence '{conf}' out of range")
    if not isinstance(result.get("tags", []), list):
        errors.append("tags must be a list")
    return errors


def classify_openai(
    input_text: str,
    valid_codes: set[str],
    divisions: list[dict],
    model: str = "gpt-4o-mini",
    api_key: str | None = None,
    max_input_chars: int = 6000,
) -> Result:
    try:
        import openai  # type: ignore
    except ImportError:
        return _make_error("openai package not installed; run: pip install openai")

    key = api_key or os.environ.get("OPENAI_API_KEY", "")
    if not key:
        return _make_error("OPENAI_API_KEY not set")

    client = openai.OpenAI(api_key=key)
    user_prompt = _build_user_prompt(input_text, divisions, max_input_chars)

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.0,
            max_tokens=300,
            response_format={"type": "json_object"},
        )
    except Exception as exc:
        return _make_error(f"API error: {str(exc)[:200]}")

    raw = response.choices[0].message.content or ""
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        return _make_error(f"JSON parse error: {exc} | raw={raw[:100]}")

    errors = _validate_result(parsed, valid_codes)
    if errors:
        return _make_error(f"validation failed: {'; '.join(errors)}")

    tags = parsed.get("tags", [])
    if not isinstance(tags, list):
        tags = []

    return {
        "primary_class_code": parsed["primary_class_code"],
        "secondary_class_code": parsed.get("secondary_class_code"),
        "tags": [str(t) for t in tags[:20]],
        "confidence": float(parsed.get("confidence", 0.0)),
        "reason": str(parsed.get("reason", ""))[:500],
    }


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

def classify(
    provider: str,
    input_text: str,
    valid_codes: set[str],
    divisions: list[dict] | None = None,
    model: str = "gpt-4o-mini",
    api_key: str | None = None,
    max_input_chars: int = 6000,
) -> Result:
    """Dispatch to the chosen provider. Returns a Result dict."""
    if provider == "local-dry-run":
        return classify_local_dry_run(input_text, valid_codes)
    if provider == "openai":
        return classify_openai(
            input_text, valid_codes, divisions or [],
            model=model, api_key=api_key, max_input_chars=max_input_chars,
        )
    return _make_error(f"unknown provider '{provider}'")
