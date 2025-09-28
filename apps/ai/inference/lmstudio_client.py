from __future__ import annotations
import os, requests, json
from typing import Any

LMSTUDIO_URL   = os.getenv("LMSTUDIO_URL", "http://localhost:1234/v1")
LMSTUDIO_MODEL = os.getenv("LMSTUDIO_MODEL", "")
LMSTUDIO_API_KEY = os.getenv("LMSTUDIO_API_KEY", "lm-studio")

def chat_completion(
    messages: list[dict[str, str]],
    temperature: float = 0.0,
    max_tokens: int = 4096,
    top_p: float = 1.0,
    top_k: int = 0,
    repetition_penalty: float | None = None,
    min_p: float = 0.0,
    stream: bool = False,
    timeout: int = 60,
) -> str:
    url = f"{LMSTUDIO_URL}/chat/completions"

    payload = {
        "model": LMSTUDIO_MODEL,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "top_p": top_p,
        "top_k": top_k,
        "min_p": min_p,
        "stream": stream,
    }

    # Добавляем repetition_penalty только если он задан
    if repetition_penalty is not None:
        payload["repetition_penalty"] = repetition_penalty

    headers = {"Authorization": f"Bearer {LMSTUDIO_API_KEY}"}
    try:
        r = requests.post(url, json=payload, headers=headers, timeout=timeout)
    except requests.exceptions.Timeout:
        raise Exception(f"Timeout: LMStudio request took too long to complete (more than {timeout} seconds)")
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"]

