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

import asyncio
import json
import os
import random
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

_SYSTEM_PROMPT = """\
You are an expert research classifier. Given a description of a qualitative research project, \
classify it into the most appropriate ISIC Rev. 5 division from the provided list.
You must respond only with valid JSON matching the schema below — no markdown, no explanation outside the JSON.

Classification rules:
- Choose the ISIC division that best matches the main substantive domain of the project, not a superficial keyword.
- Do not choose a class only because a single keyword appears.
- Q85 Education: use when the project is about teaching, students, schools, universities, curriculum, \
learning, training, educational practice, or student feedback. Do NOT use Q85 for lab science merely \
because the data was collected at a university.
- P84 Public administration and defence: use ONLY for government administration, public policy administration, \
defence, or compulsory social security. Do NOT use P84 for university teaching, student datasets, or \
education-sector research.
- R86 Human health activities: use for medical, clinical, patient, disease, diagnosis, treatment, \
healthcare, hospital, therapy, or health-service projects.
- N72 Scientific research and development: use when the project is primarily academic or scientific research \
and no clearer economic sector dominates (e.g. lab science, biomedical research, general R&D).
- M68 Real estate activities: use ONLY for real estate, property markets, rental, land/property transactions, \
or real-estate services. Do NOT use M68 for general consumer purchases, data management plans, or unrelated topics.
- A01/A02/A03 Agriculture/forestry/fishing: use ONLY when agriculture, forestry, fishing, or aquaculture \
is the main sector — not merely because plants, animals, or ecology appear.
- If the reason you would give mentions a different class than the code you selected, correct the code before returning JSON.

Confidence calibration:
- 0.90–1.00: the class is unambiguous and obvious.
- 0.70–0.89: likely correct but some ambiguity exists.
- 0.50–0.69: uncertain between two plausible classes.
- Do not return 0.95 as a default; calibrate genuinely.

Examples:
- "Survey on student expectations for faculty and peer support in engineering education" → Q85, confidence ~0.95
- "Hospital patient interviews about chronic pain management" → R86, confidence ~0.93
- "General ecological research dataset on soil microbiology" → N72, confidence ~0.80
- "Government policy implementation and public administration reform" → P84, confidence ~0.90
- "Real estate market transactions and rental price data" → M68, confidence ~0.92
- "Why did I buy that? A study of regretted consumer appliance purchases" → G47, confidence ~0.75\
"""

_OUTPUT_SCHEMA = """\
{
  "primary_class_code": "<ISIC division code from the allowed list>",
  "secondary_class_code": "<ISIC division code or null>",
  "tags": ["<keyword1>", "<keyword2>"],
  "confidence": <float 0.0–1.0>,
  "reason": "<one sentence explaining the classification>"
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


def _is_fatal_api_error(msg: str) -> bool:
    low = msg.lower()
    return "401" in msg or "authentication" in low or "incorrect api key" in low or "invalid_api_key" in low


def _parse_model_response(raw: str, valid_codes: set[str]) -> Result:
    """Parse and validate a raw chat-completion string into a Result dict."""
    # Strip markdown code fences and extract JSON object if surrounded by extra text.
    cleaned = raw.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned.strip())
    m = re.search(r"\{.*\}", cleaned, re.DOTALL)
    if m:
        cleaned = m.group(0)

    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        return {**_make_error(f"JSON parse error: {exc}"), "raw_model_output": raw[:1000]}

    # Coerce secondary_class_code: "" or "null" -> None
    sec = parsed.get("secondary_class_code")
    if sec == "" or sec == "null":
        parsed["secondary_class_code"] = None

    # Coerce confidence: string -> float
    conf = parsed.get("confidence")
    if isinstance(conf, str):
        try:
            parsed["confidence"] = float(conf)
        except (ValueError, TypeError):
            parsed["confidence"] = 0.0

    # Coerce tags: string -> list
    tags_raw = parsed.get("tags", [])
    if isinstance(tags_raw, str):
        parsed["tags"] = [t.strip() for t in tags_raw.split(",") if t.strip()]

    errors = _validate_result(parsed, valid_codes)
    if errors:
        return {**_make_error(f"validation failed: {'; '.join(errors)}"), "raw_model_output": raw[:1000]}

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


def _chat_messages(input_text: str, divisions: list[dict], max_input_chars: int) -> list[dict]:
    return [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {"role": "user", "content": _build_user_prompt(input_text, divisions, max_input_chars)},
    ]


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
        return {**_make_error("openai package not installed; run: pip install openai"), "fatal": True}

    key = api_key or os.environ.get("OPENAI_API_KEY", "")
    if not key:
        return {**_make_error("OPENAI_API_KEY not set"), "fatal": True}

    client = openai.OpenAI(api_key=key)

    try:
        response = client.chat.completions.create(
            model=model,
            messages=_chat_messages(input_text, divisions, max_input_chars),
            temperature=0.0,
            max_tokens=300,
            response_format={"type": "json_object"},
        )
    except Exception as exc:
        msg = str(exc)
        fatal = _is_fatal_api_error(msg)
        return {**_make_error(f"API error: {msg[:400]}"), "raw_model_output": "", "fatal": fatal}

    raw = response.choices[0].message.content or ""
    return _parse_model_response(raw, valid_codes)


# ---------------------------------------------------------------------------
# openai provider — async, with retry/backoff for transient errors
# ---------------------------------------------------------------------------

# Transient errors worth retrying: rate limits, 5xx server errors (the SDK
# raises InternalServerError for 500/502/503/504), timeouts, and
# connection-level failures. Authentication (401) and other client errors
# (4xx, bad request, invalid JSON, etc.) are not retried.
def _is_retryable_exception(exc: Exception) -> bool:
    import openai  # type: ignore
    return isinstance(exc, (
        openai.RateLimitError,
        openai.InternalServerError,
        openai.APIConnectionError,
        openai.APITimeoutError,
    ))


def _is_auth_exception(exc: Exception) -> bool:
    import openai  # type: ignore
    return isinstance(exc, openai.AuthenticationError)


async def classify_openai_async(
    input_text: str,
    valid_codes: set[str],
    divisions: list[dict],
    client: Any,
    model: str = "gpt-4o-mini",
    max_input_chars: int = 6000,
    max_retries: int = 5,
) -> Result:
    """Async equivalent of classify_openai, with exponential-backoff retry
    on transient errors. `client` must be an openai.AsyncOpenAI instance,
    created once and shared across concurrent calls for connection reuse.
    """
    messages = _chat_messages(input_text, divisions, max_input_chars)
    retries = 0

    while True:
        try:
            response = await client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=0.0,
                max_tokens=300,
                response_format={"type": "json_object"},
            )
        except Exception as exc:
            if _is_auth_exception(exc):
                return {
                    **_make_error(f"API error: {str(exc)[:400]}"),
                    "raw_model_output": "",
                    "fatal": True,
                    "retries": retries,
                }
            if _is_retryable_exception(exc) and retries < max_retries:
                delay = min(30.0, (2 ** retries)) + random.uniform(0, 1)
                retries += 1
                await asyncio.sleep(delay)
                continue
            msg = str(exc)
            return {
                **_make_error(f"API error after {retries} retries: {msg[:400]}"),
                "raw_model_output": "",
                "fatal": _is_fatal_api_error(msg),
                "retries": retries,
            }

        raw = response.choices[0].message.content or ""
        result = _parse_model_response(raw, valid_codes)
        result["retries"] = retries
        return result


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
