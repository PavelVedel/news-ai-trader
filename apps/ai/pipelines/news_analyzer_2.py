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
from pathlib import Path
from typing import Any, Iterable
from libs.utils.json_sanitize import smart_json_or_none
from libs.utils.logging_setup import get_logger
from apps.ai.inference.lmstudio_client import chat_completion
from libs.database.connection import DatabaseConnection
from pprint import pprint

logger = get_logger("news.ai.analyzer2")

def load_system_prompt() -> str:
    """
    Load system prompt from file.
    Works when script is run from project root or directly via cmd.
    """
    # Get project root: from apps/ai/pipelines/ go up 4 levels
    project_root = Path(__file__).parent.parent.parent.parent
    prompt_path = project_root / "data" / "prompts" / "system_news_analyzer.txt"
    
    try:
        with open(prompt_path, 'r', encoding='utf-8') as f:
            text = f.read().strip()
            if text:
              logger.info(f"System prompt file found at {prompt_path} with text: {text[:100]} ...")
            return text
    except FileNotFoundError:
        logger.error(f"System prompt file not found at {prompt_path}")
        raise
    except Exception as e:
        logger.error(f"Error loading system prompt: {e}")
        raise


# Load system prompt for the Qwen model
SYSTEM_NEWS_ANALYZER = load_system_prompt()


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
                {"role": "system", "content": SYSTEM_NEWS_ANALYZER},
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
    db.ensure_news_analysis_a_table()

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
        db.save_news_analysis_a(res)
        print(f"\nРезультат анализа сохранен в БД для новости {one_news['news_id']}")

        # Проверяем что сохранилось
        saved = db.get_news_analysis_a(one_news['news_id'])
        if saved:
            print("\nПроверка сохраненного результата:")
            print(f"ID: {saved['news_id']}")
            print(f"Headline: {saved['headline']}")
            print(f"Actors count: {len(saved['actors'])}")
            print(f"Event type: {saved['event'].get('type')}")
    pass


if __name__ == "__main__":
    main()
