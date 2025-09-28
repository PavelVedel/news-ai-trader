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
- Flash Attention: Disabled (Experimental)
- K Cache Quantization Type: Disabled (Experimental)
- V Cache Quantization Type: Disabled (Experimental)
"""
import json
from typing import Any, Iterable
from libs.utils.json_sanitize import smart_json_or_none
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
Do NOT guess tickers for entities not explicitly tied in `headline`, `summary`, `url`; instead, mark them as `needs_grounding: true`. Even if a ticker is present in `symbols_json`, treat it only as a CANDIDATE, not as confirmed evidence.
Return STRICT JSON only, UTF-8, no comments, no prose.

You must work with the following fields:
- `created_at_utc`: ISO8601 timestamp of news publishing  
- `headline`: news title text; part of article
- `summary`: short description of the news; part of article (may be empty)  
- `symbols_json`: list of symbols provided externally (possible candidates). NOT a part of article.
- `url`: can be used as alternative header.

Definitions:
- `actor`: a real-world participant mentioned in the text (organization, person, fund, regulator, product line if central).
- `role` in {issuer, acquirer, target, supplier, customer, competitor, partner, rated, rater, regulator, plaintiff, defendant, executive, platform, source, discussion_hub, other}.
- `event_type` in {earnings, guidance, M&A, rating, buyback, dividend, litigation, product/tech, capacity, macro, insider/blocks, stake_change, FDA/clinical, supply_chain, other}.
- `quant`: normalized numeric fact or graded qualifier (e.g., pct_yoy, eps_surprise, deal_value_usd, share, guidance_delta, "record"/"all-time-high").
- `contextual_clarity` ∈ {low, medium, high}  
  - low: actor only named, no role or detail  
  - medium: actor named with explicit role or function (e.g., issuer, CEO, investor)  
  - high: actor named multiple times with consistent role and extra descriptive context
- `symbol_mentions_in_text` must be indicated only if `headline`, `summary`, or `url` have symbol.

For each mention, capture its text span offsets if available; otherwise set null.

OUTPUT schema (single JSON object):
{
  "news_id": "<string>",
  "created_at_utc": "<ISO8601 or null>",
  "headline": "<string>",
  "symbols_input": ["<SYM1>", "..."], // as provided externally (`symbols_json`)
  "actors": [
    {
      "name": "<as in text>",
      "type": "org|person|fund|regulator|product|macro|other",
      "role": "<from role set or 'other'>",
      "org_hint_from_text": "<string or null>", // e.g., company name next to a person; from TEXT ONLY
      "mentions": [{"text":"<span>", "start": <int|null>, "end": <int|null>}],
      "needs_grounding": true, // always true unless the text itself states the EXCESS definition (who, role, what does, what owns, etc.)
      "contextual_clarity": "<string>"
    }
  ],
  "event": {
    "type": "<from event_type set>",
    "actions": ["<lemmatized verbs/phrases from text>"],
    "quants": [
      {
        "metric": "<eps_surprise|revenue_surprise|deal_value_usd|share|guidance_delta|interest_rate_change|...|qual>",
        "value": "<number|string>", // numbers as strings if not parseable
        "unit": "<unit or null>",
        "qualifier": "<e.g., record, unprecedented, minor, preliminary, cut, etc. or null>"
      }
    ],
    "evidence_spans": [{"text":"<snippet>", "start":<int|null>, "end":<int|null>}]
  },
  "symbol_mentions_in_text": [
    {
      "symbol": "<SYM from symbols_json if explicitly mentioned in text; else null>", // "INTC"
      "surface": "<as appears in text or null>", // "Intel"
      "where": "headline|summary|url|other", // "title"
      "offset": {"start": <int|null>, "end": <int|null>} // {"start": 30, "end": 35}
    }
  ],
  "symbol_not_mentioned_in_text": ["<SYM1>", "..."],
  "unresolved_entities": [
    {
      "name": "<actor with unknown ticker/company>", // "Federal Reserve"
      "reason": "no_explicit_ticker_in_text|ambiguous_name|needs_company_resolution"
    }
  ]
}

Validation rules:
- Use only INPUT text to populate fields; NEVER infer CEO/company links, tickers, or competitor relations here.
- NO guessing: capture only raw facts from the text. Do not equate symbols with names unless explicitly stated in the article (e.g., do not map if `symbols_json` contains 'AAPL' and the text only contains 'Apple Inc.').
- Do NOT use `symbols_json` to insert or bind symbols to actors unless the symbol itself is explicitly present in the `headline`, `summary`, or `url`. If present, set `needs_grounding: true` and handle unmatched symbols via `symbol_not_mentioned_in_text`.
- Ensure JSON is syntactically valid and conforms to the schema; unknowns → null; omit nothing.

- Each actor mention must correspond to a unique text span found in the headline, summary, or URL. 
- Do not duplicate offsets across different entities. If the same offset is reused for multiple names, treat it as an error and select only the entity that actually appears in that span.
- Prioritize `headline` > `summary` > `url` when extracting actors and symbols. Do not duplicate the same entity across multiple sources.

- When using the URL, treat it as a secondary text source after headline and summary. Only use the URL path (exclude query strings), split it on hyphens or slashes to identify entity tokens, and assign unique offsets within the URL string. Do not bulk-assign one offset to multiple entities.
- If multiple symbols are in `symbols_json` but absent from all three text sources, they must go into `symbol_not_mentioned_in_text`.
  
=== FEW-SHOT EXAMPLES ===
Below are examples of incorrect vs correct JSON outputs for similar news articles. Follow the GOOD examples strictly.

COUNTEREXAMPLE 1
Input:
news_id 148 source benzinga provider_id 47284986 created_at_utc 2025-08-22T15:08:51Z received_at_utc 2025-08-24T20:39:08.107165+00:00 headline 10 Stocks Rocketing After Powell's Dovish Shift summary Markets surged after Powell&#39;s Jackson Hole speech, as he flagged rising job risks and opened the door to possible rate cuts in September. symbols_json ["APP", "ARM", "COIN", "HOOD", "INTC", "MRVL", "MSTR", "NXPI", "SHOP", "TSLA"] url https://www.benzinga.com/news/25/08/47284986/powell-speech-jackson-hole-market-reactions-stocks-on-the-move-friday-wall-street hash_dedupe 3f040a3da3d89daf574bcaca2a7459cf

Good output:
{
  "news_id": "148",
  "actors": [
    {
      "name": "Powell",
      "type": "person",
      "role": "rater",
      "org_hint_from_text": null,
      "mentions": [{"text": "Powell", "start": 38, "end": 42}],
      "needs_grounding": true,
      "contextual_clarity": "high"
    }
  ],
  "event": {
    "type": "macro",
    "actions": ["flagged","opened","indicated","shifted"],
    "quants": [
      {"metric":"qual","value":"dovish","unit":null,"qualifier":"dovish"},
      {"metric":"qual","value":"possible rate cuts","unit":null,"qualifier":"September"}
    ],
    "evidence_spans":[{"text":"Powell's Jackson Hole speech, as he flagged rising job risks and opened the door to possible rate cuts in September.","start":43,"end":78}]
  },
  "symbol_mentions_in_text": [],
  "symbol_not_mentioned_in_text": ["APP","ARM","COIN","HOOD","INTC","MRVL","MSTR","NXPI","SHOP","TSLA"],
  "unresolved_entities": [
    {"name":"Powell","reason":"no_explicit_ticker_in_text"}
  ]
}

Bad output:
{
  "news_id": "148",
  "actors": [
    {"name": "Powell", "type": "person", "role": "rater", "mentions":[{"text":"Powell","start":38,"end":42}], "needs_grounding":false, "contextual_clarity":"high"}
  ],
  "event": {
    "type": "macro",
    "actions": ["flagged","opened","indicated","shift"],
    "quants": [
      {"metric":"qual","value":"dovish","unit":null,"qualifier":"dovish"},
      {"metric":"qual","value":"possible rate cuts","unit":null,"qualifier":"in September"}
    ],
    "evidence_spans":[{"text":"Powell's Jackson Hole speech, as he flagged rising job risks and opened the door to possible rate cuts in September.","start":43,"end":78}]
  },
  "symbol_mentions_in_text": [],
  "symbol_not_mentioned_in_text": ["APP","ARM","COIN","HOOD","INTC","MRVL","MSTR","NXPI","SHOP","TSLA"],
  "unresolved_entities": [
    {"name":"APP","reason":"no_explicit_ticker_in_text"},
    {"name":"ARM","reason":"no_explicit_ticker_in_text"},
    {"name":"COIN","reason":"no_explicit_ticker_in_text"}
  ]
}


COUNTEREXAMPLE 2
Input:
news_id 269107 source benzinga provider_id 47446142 created_at_utc 2025-09-02T12:04:32Z received_at_utc 2025-09-26T20:56:11.602761+00:00 headline vTv Therapeutics Enters $80M PIPE Financing With Institutional Investors And The T1D Fund summary symbols_json ["VTVT"] url https://www.benzinga.com/news/25/09/47446142/vtv-therapeutics-enters-80m-pipe-financing-with-institutional-investors-and-the-t1d-fund hash_dedupe 13e72556f051b85c72c633814448e06f

Good output:
{
  "news_id": "269107",
  "actors": [
    {
      "name":"vTv Therapeutics",
      "type":"org",
      "role":"issuer",
      "org_hint_from_text":null,
      "mentions":[{"text":"vTv Therapeutics","start":0,"end":17}],
      "needs_grounding":true,
      "contextual_clarity":"high"
    },
    {
      "name":"Institutional Investors",
      "type":"other",
      "role":"acquirer",
      "org_hint_from_text":null,
      "mentions":[{"text":"Institutional Investors","start":45,"end":67}],
      "needs_grounding":true,
      "contextual_clarity":"medium"
    },
    {
      "name":"The T1D Fund",
      "type":"org",
      "role":"acquirer",
      "org_hint_from_text":null,
      "mentions":[{"text":"The T1D Fund","start":69,"end":80}],
      "needs_grounding":true,
      "contextual_clarity":"medium"
    }
  ],
  "event": {
    "type": "M&A",
    "actions": ["enter","finance"],
    "quants": [{"metric":"deal_value_usd","value":"80","unit":"M USD","qualifier":null}],
    "evidence_spans":[{"text":"vTv Therapeutics Enters $80M PIPE Financing","start":0,"end":34}]
  },
  "symbol_mentions_in_text": [
    {"symbol":"VTVT","surface":"vTv Therapeutics","where":"headline","offset":{"start":0,"end":17}}
  ],
  "symbol_not_mentioned_in_text": [],
  "unresolved_entities": [
    {"name":"Institutional Investors","reason":"needs_company_resolution"},
    {"name":"The T1D Fund","reason":"needs_company_resolution"}
  ]
}

Bad output:
{
  "news_id": "269107",
  "actors": [
    {"name":"vTv Therapeutics","type":"org","role":"issuer","mentions":[{"text":"vTv Therapeutics","start":0,"end":11}],"needs_grounding":false,"contextual_clarity":"high"},
    {"name":"Institutional Investors","type":"other","role":"acquirer","mentions":[{"text":"Institutional Investors","start":45,"end":58}],"needs_grounding":true,"contextual_clarity":"medium"},
    {"name":"The T1D Fund","type":"org","role":"acquirer","mentions":[{"text":"The T1D Fund","start":60,"end":67}],"needs_grounding":true,"contextual_clarity":"medium"}
  ],
  "event": {
    "type": "M&A",
    "actions": ["enters","financing","pipes"],
    "quants": [{"metric":"deal_value_usd","value":"80M","unit":"USD","qualifier":null}],
    "evidence_spans":[{"text":"vTv Therapeutics Enters $80M PIPE Financing","start":0,"end":34}]
  },
  "symbol_mentions_in_text": [
    {"symbol":"VTVT","surface":"vTv Therapeutics","where":"title","offset":{"start":11,"end":19}}
  ],
  "symbol_not_mentioned_in_text": [],
  "unresolved_entities": []
}

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
            temperature=0.05,
            max_tokens=10000,  # Ensure we have enough tokens for the response
            timeout=2*60
        )

        # Try to parse the JSON response
        data = smart_json_or_none(content)
        if data is None:
            logger.warning("Failed to parse LLM response as JSON", extra={
                "news_id": item.get("news_id"),
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
    one_news = dict(db.get_news_by_id(264839))
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
    pass


if __name__ == "__main__":
    main()
