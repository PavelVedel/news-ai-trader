from __future__ import annotations
import json
from typing import Any, Iterable
from libs.utils.json_sanitize import extract_json_block
from libs.utils.logging_setup import get_logger
from apps.ai.inference.lmstudio_client import chat_completion
from libs.utils.json_sanitize import cheap_json_or_none, strip_fences
from apps.ai.inference.lmstudio_client import chat_completion

logger = get_logger("news.ai")

SYSTEM = (
  "You are a financial news analyzer. Output ONLY valid JSON (no code fences). "
  "{"
  "\"story\": {"
  "\"summary_short\": string (<=160 chars), "
  "\"topics\": array (<=3 items), "
  "\"sentiment\": number in [-1,1], "
  "\"actionability\": boolean, "
  "\"confidence\": number in [0,1], "
  "\"scope\": one of [\"company\",\"sector\",\"market\"]"
  "},"
  "\"per_symbol\": ["
  "{"
  "\"symbol\": string, "
  "\"relevance\": number in [0,1], "
  "\"sentiment\": number in [-1,1], "
  "\"direction\": one of [\"up\",\"down\",\"neutral\",\"volatility\"], "
  "\"horizon\": one of [\"minutes\",\"hours\",\"days\",\"weeks\"], "
  "\"actionability\": boolean, "
  "\"confidence\": number in [0,1], "
  "\"rationale\": string (<=100 chars)"
  "}"
  "]"
  "}"
  "Return at most 3 items in per_symbol, sorted by relevance desc. "
  "Symbols must be chosen ONLY from the provided list. "
  "Never use code fences."
)


def build_user_prompt(item: dict[str, Any]) -> str:
    def clip(x: str, n: int = 800) -> str:
        x = (x or "").strip()
        return x if len(x) <= n else x[:n] + "…"
    syms = ", ".join(item.get("symbols") or [])
    return (
        f"Source: {item.get('source','')}\n"
        f"Symbols: {syms}\n"
        f"Title: {clip(item.get('headline',''), 200)}\n"
        f"Text: {clip(item.get('summary',''), 1000)}\n"
        "Analyze and output ONLY JSON."
    )


def analyze_one(item: dict[str, Any]) -> dict[str, Any]:
    content = chat_completion(
        [{"role":"system","content":SYSTEM},{"role":"user","content":build_user_prompt(item)}],
        temperature=0.2, max_tokens=700  # 700–800 достаточно
    )
    data = cheap_json_or_none(content)
    if data is None:
        logger.warning("llm_json_parse_failed", extra={"payload":{
            "raw_head": content[:160], "raw_tail": content[-160:], "len": len(content)
        }})
        data = repair_json_with_llm(content, item.get("symbols") or [])

    # пост-обработка
    allowed = set(item.get("symbols") or [])
    per_symbol = data.get("per_symbol") or []
    per_symbol = [ps for ps in per_symbol if isinstance(ps, dict) and ps.get("symbol") in allowed]
    per_symbol.sort(key=lambda x: float(x.get("relevance", 0)), reverse=True)
    per_symbol = per_symbol[:3]

    return {
        "id": item.get("id"),
        "source": item.get("source"),
        "created_at": item.get("created_at"),
        "url": item.get("url"),
        "headline": item.get("headline"),
        "symbols": item.get("symbols") or [],
        "story": data.get("story", {"summary_short":"","topics":[],"sentiment":0,"actionability":False,"confidence":0,"scope":"company"}),
        "per_symbol": per_symbol,
    }


def analyze_batch(items: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]]:
    for it in items:
        yield analyze_one(it)

REPAIR_SYSTEM = (
    "You repair possibly truncated JSON. "
    "Input will be between <RAW> tags. "
    "Return ONE valid JSON object only, no code fences. "
    "Keep the same schema: {story:{...}, per_symbol:[...]}. "
    "If array items are incomplete, drop them. Max 3 items in per_symbol."
)

def repair_json_with_llm(raw: str, allowed_symbols: list[str]) -> dict:
    msg = (
        "Repair this into valid JSON.\n"
        f"Allowed symbols: {', '.join(allowed_symbols)}\n"
        "<RAW>\n" + strip_fences(raw) + "\n</RAW>\n"
        "Return only JSON."
    )
    content = chat_completion(
        [{"role":"system","content":REPAIR_SYSTEM},{"role":"user","content":msg}],
        temperature=0.0, max_tokens=500
    )
    fixed = cheap_json_or_none(content)
    if fixed is None:
        # как последний фоллбек — минимальный каркас
        return {"story":{"summary_short":"","topics":[],"sentiment":0,"actionability":False,"confidence":0,"scope":"company"},"per_symbol":[]}
    return fixed