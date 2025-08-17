import json, re

_FENCE = re.compile(r"^```(?:json)?\s*|\s*```$", re.IGNORECASE | re.MULTILINE)

def extract_json_block(text: str) -> str:
    """
    Убирает ```json ... ```, вырезает самый ранний корректно закрытый объект {…}.
    Если полного объекта нет — пытается обрезать до последней '}'.
    Если ничего не получилось — бросает ValueError.
    """
    t = _FENCE.sub("", text).strip()
    start = t.find("{")
    if start == -1:
        raise ValueError("no '{' in text")

    depth = 0
    last_good = None
    for i, ch in enumerate(t[start:], start=start):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                cand = t[start:i+1]
                try:
                    json.loads(cand)
                    return cand
                except Exception:
                    # запомним позицию и попробуем двигаться дальше
                    last_good = i

    # не нашли полностью сбалансированного — обрежем по последней '}' и попробуем назад
    j = t.rfind("}")
    while j != -1 and j > start:
        cand = t[start:j+1]
        try:
            json.loads(cand)
            return cand
        except Exception:
            j = t.rfind("}", start, j)
    raise ValueError("no valid json substring found")

def strip_fences(text: str) -> str:
    return _FENCE.sub("", text).strip()

def extract_balanced_json(text: str) -> str:
    """
    Находит первый сбалансированный {…}. Если верхний объект не закрыт,
    пытается обрезать до последней '}' и проверить валидность.
    Бросает ValueError, если ничего валидного нет.
    """
    t = strip_fences(text)
    start = t.find("{")
    if start < 0:
        raise ValueError("no '{' found")
    depth = 0
    for i, ch in enumerate(t[start:], start=start):
        if ch == "{": depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                cand = t[start:i+1]
                json.loads(cand)
                return cand
    # нет полного закрытия: попробуем обрезать по последней '}'
    j = t.rfind("}")
    while j > start:
        cand = t[start:j+1]
        try:
            json.loads(cand)
            return cand
        except Exception:
            j = t.rfind("}", start, j-1)
    raise ValueError("no valid json substring found")

def cheap_json_or_none(text: str):
    try:
        return json.loads(extract_balanced_json(text))
    except Exception:
        return None