from __future__ import annotations
import os, requests, json
from typing import Any

LMSTUDIO_URL   = os.getenv("LMSTUDIO_URL", "http://localhost:1234/v1")
LMSTUDIO_MODEL = os.getenv("LMSTUDIO_MODEL", "")
LMSTUDIO_API_KEY = os.getenv("LMSTUDIO_API_KEY", "lm-studio")

def chat_completion(messages: list[dict[str, str]], temperature: float = 0.2, max_tokens: int = 1000) -> str:
    url = f"{LMSTUDIO_URL}/chat/completions"
    payload = {"model": LMSTUDIO_MODEL, "messages": messages, "temperature": temperature, "max_tokens": max_tokens, "stream": False}
    # payload["response_format"] = {"type": "json_object"}
    headers = {"Authorization": f"Bearer {LMSTUDIO_API_KEY}"}
    r = requests.post(url, json=payload, headers=headers, timeout=60)
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"]
