"""
LM Studio Configuration (Qwen3-4B)

Model Context
- Context Length: 5000
- Model Max Supported: 262144

Hardware Offload
- GPU Offload: 36 / 36
- CPU Thread Pool Size: 12

Evaluation
- Evaluation Batch Size: 2048

RoPE (Rotary Positional Embeddings)
- RoPE Frequency Base: Auto
- RoPE Frequency Scale: Auto

Memory
- Offload KV Cache to GPU Memory: Enabled
- Keep Model in Memory: Enabled
- Try mmap(): Enabled

Randomness
- Seed: Not Set
- Random Seed Mode: Default

Optimizations
- Flash Attention: Enabled (Experimental)
- K Cache Quantization Type: Disabled (Experimental)
- V Cache Quantization Type: Disabled (Experimental)
"""

import json
from typing import Any, Iterable
from libs.utils.json_sanitize import cheap_json_or_none
from libs.utils.logging_setup import get_logger
from apps.ai.inference.lmstudio_client import chat_completion
from libs.database.connection import DatabaseConnection
from pprint import pprint

logger = get_logger("news.ai.analyzer2")

# System prompt for the Qwen model
SYSTEM = """
SYSTEM
You are a deterministic information-extraction model for financial news.
Task: convert one news article into a structured JSON object capturing entities (actors), roles, event type, quantitative facts, and the link between the article text and the provided symbol list. The extracted information will later be used for evaluating the potential market impact of the news. Therefore, every mentioned actor must be captured, even if its role seems secondary (e.g., platforms, sources, or communities), since their presence can still influence perception and price reaction. Accurate assessment of contextual clarity is critical.
Do NOT use any outside knowledge; rely solely on the article text and the supplied symbol list.
Do NOT guess tickers for entities not explicitly tied in `headline` or `summary`; instead, mark them as `needs_grounding: true`. Even if a ticker is present in `symbols_json`, treat it only as a CANDIDATE, not as confirmed evidence.
Return STRICT JSON only, UTF-8, no comments, no prose.

You must work with the following fields:
- `created_at_utc`: ISO8601 timestamp of news publishing  
- `headline`: news title text; part of article
- `summary`: short description of the news; part of article (may be empty)  
- `symbols_json`: list of symbols provided externally (possible candidates). NOT a part of article.

Definitions:
- `actor`: a real-world participant mentioned in the text (organization, person, fund, regulator, product line if central).
- `role` in {issuer, acquirer, target, supplier, customer, competitor, partner, rated, rater, regulator, plaintiff, defendant, executive, platform, source, discussion_hub, other}.
- `event_type` in {earnings, guidance, M&A, rating, buyback, dividend, litigation, product/tech, capacity, macro, insider/blocks, stake_change, FDA/clinical, supply_chain, other}.
- `quant`: normalized numeric fact or graded qualifier (e.g., pct_yoy, eps_surprise, deal_value_usd, share, guidance_delta, "record"/"all-time-high").
- `contextual_clarity` ∈ {low, medium, high}  
  - low: actor only named, no role or detail  
  - medium: actor named with explicit role or function (e.g., issuer, CEO, investor)  
  - high: actor named multiple times with consistent role and extra descriptive context
- `symbol_mentions_in_text` must be indicated only if `headline` or `summary` have symbol.

For each mention, capture its text span offsets if available; otherwise set null.

OUTPUT schema (single JSON object):
{
  "news_id": "<string>",
  "created_at_utc": "<ISO8601 or null>",
  "headline": "<string>",
  "symbols_input": ["<SYM1>", "..."],               // as provided externally
  "actors": [
    {
      "name": "<as in text>",
      "type": "org|person|fund|regulator|product|other",
      "role": "<from role set or 'other'>",
      "org_hint_from_text": "<string or null>",      // e.g., company name next to a person; from TEXT ONLY
      "mentions": [{"text":"<span>", "start": <int|null>, "end": <int|null>}],
      "needs_grounding": true,                        // always true unless the text itself states the EXCESS definition (who, role, what does, what owns, etc.)
      "contextual_clarity": "<string>"
    }
  ],
  "event": {
    "type": "<from event_type set>",
    "actions": ["<lemmatized verbs/phrases from text>"],
    "quants": [
      {
        "metric": "<eps_surprise|revenue_surprise|deal_value_usd|share|guidance_delta|...|qual>",
        "value": "<number|string>",                  // numbers as strings if not parseable
        "unit": "<unit or null>",
        "qualifier": "<e.g., record, unprecedented, minor, preliminary, etc. or null>"
      }
    ],
    "evidence_spans": [{"text":"<snippet>", "start":<int|null>, "end":<int|null>}]
  },
  "symbol_mentions_in_text": [
    {
      "symbol": "<SYM from symbols_json if explicitly mentioned in text; else null>",
      "surface": "<as appears in text or null>",
      "where": "title|lead|body|caption|other",
      "offset": {"start": <int|null>, "end": <int|null>}
    }
  ],
  "symbol_not_mentioned_in_text": ["<SYM1>", "..."]
  "unresolved_entities": [
    {
      "name": "<actor with unknown ticker/company>",
      "reason": "no_explicit_ticker_in_text|ambiguous_name|needs_company_resolution"
    }
  ]
}

Validation rules:
- Use only article text to populate fields; never infer CEO/company links, tickers, or competitor relations here.
- If the article lists a symbol (from symbols_input) but the text does not mention the underlying company name, leave the mapping for Stage B (next stage).
- Do NOT bind symbols from `symbols_json` to actors unless the symbol is explicitly present in `headline` or `summary`; in such cases set `needs_grounding: true` and place the symbol into `symbol_not_mentioned_in_text`.
- Ensure JSON is syntactically valid and conforms to the schema; unknowns → null; omit nothing.
- No guessing: capture only raw facts from the text. Do not equate symbols and names (e.g., `AAPL` ≠ `Apple`) unless explicitly written in the article.
"""


# def tabulate_print(data: dict[str, Any]) -> None:
#     from tabulate import tabulate
#     print(tabulate(data.items(), headers=["Key", "Value"], tablefmt="pretty"))


def build_user_prompt(item: dict[str, Any]) -> str:
    """Build a prompt for the LLM from a news item."""
    return json.dumps(dict(item), ensure_ascii=False)


def analyze_one(item: dict[str, Any]) -> dict[str, Any]:
    """Analyze a single news item using the LLM."""
    try:
        content = chat_completion(
            [
                {"role": "system", "content": SYSTEM},
                {"role": "user", "content": build_user_prompt(item)}
            ],
            temperature=0.00,
            max_tokens=5000  # Ensure we have enough tokens for the response
        )

        # Try to parse the JSON response
        data = cheap_json_or_none(content)
        if data is None:
            logger.warning("Failed to parse LLM response as JSON", extra={
                "news_id": item.get("id"),
                "content_preview": content[:200]
            })
            return None

        # Add original item data to the response
        data["news_id"] = item.get("news_id")

        return data

    except Exception as e:
        logger.error(f"Error analyzing news item: {str(e)}", extra={
                     "news_id": item.get("news_id")})
        return None


def analyze_batch(items: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]]:
    """Process a batch of news items."""
    for item in items:
        yield analyze_one(item)

# Функции process_all_news и process_one_news перенесены в apps/ai/perform_stage_a_news_analyzation.py


def main():
    db = DatabaseConnection("data/db/news.db")

    # Создаем таблицу для результатов анализа
    db.ensure_news_analysis_table()

    # Пример анализа одной новости
    one_news = dict(db.get_news_by_id(200))
    print("Исходная новость:")
    pprint(one_news)

    # Анализируем новость
    res = analyze_one(one_news)
    print("\nРезультат анализа:")
    pprint(res)

    # Сохраняем результат в базу данных
    if res:
        db.save_news_analysis(res)
        print(f"\nРезультат анализа сохранен в БД для новости {one_news['news_id']}")

        # Проверяем что сохранилось
        saved = db.get_news_analysis(one_news['news_id'])
        if saved:
            print("\nПроверка сохраненного результата:")
            print(f"ID: {saved['news_id']}")
            print(f"Headline: {saved['headline']}")
            print(f"Actors count: {len(saved['actors'])}")
            print(f"Event type: {saved['event'].get('type')}")


if __name__ == "__main__":
    main()
