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

def normalize_brackets(text: str) -> str:
    """
    Нормализует скобки в JSON, исправляя ] вместо } и [ вместо {.
    Использует стек для отслеживания открывающих скобок.
    
    Args:
        text: JSON строка с возможными ошибками скобок
        
    Returns:
        str: JSON строка с исправленными скобками
    """
    # Удаляем префикс "content\n" если он есть
    if text.startswith("content\n"):
        text = text[8:]
    
    # Если текст обернут в одинарные кавычки, удаляем их
    if text.startswith("'") and text.endswith("'"):
        try:
            import ast
            text = ast.literal_eval(text)
        except Exception:
            # Если не получилось, просто удаляем кавычки
            text = text[1:-1]
    
    result = []
    stack = []  # Стек для отслеживания открывающих скобок
    
    i = 0
    while i < len(text):
        char = text[i]
        
        if char in '{[':
            # Открывающая скобка - добавляем в стек
            stack.append(char)
            result.append(char)
        elif char in '}]':
            # Закрывающая скобка - проверяем соответствие
            if not stack:
                # Нет открывающих скобок - это ошибка, но продолжаем
                result.append(char)
            else:
                last_open = stack.pop()
                expected_close = '}' if last_open == '{' else ']'
                
                if char == expected_close:
                    # Правильная закрывающая скобка
                    result.append(char)
                else:
                    # Неправильная закрывающая скобка - исправляем
                    result.append(expected_close)
        else:
            # Обычный символ
            result.append(char)
        
        i += 1
    
    return ''.join(result)

def smart_json_or_none(text: str) -> dict | None:
    """
    Умная функция для парсинга JSON с автоматическим исправлением ошибок.
    
    Args:
        text: JSON строка с возможными ошибками
        
    Returns:
        dict | None: Распарсенный JSON или None при неудаче
    """
    try:
        # Сначала пробуем обычный парсинг
        return json.loads(extract_balanced_json(text))
    except Exception:
        try:
            # Если не получилось, исправляем скобки и пробуем снова
            normalized_text = normalize_brackets(text)
            return json.loads(extract_balanced_json(normalized_text))
        except Exception:
            return None

def cheap_json_or_none(text: str):
    try:
        balanced_json = extract_balanced_json(text)
        return json.loads(balanced_json)
    except Exception:
        return None