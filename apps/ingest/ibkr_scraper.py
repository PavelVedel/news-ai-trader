# file: ibkr_news_manual_assist.py
# Запуск: python ibkr_news_manual_assist.py
# Данные пишутся в ibkr_news_selected.csv в текущей папке.

import asyncio
import csv
import os
from pathlib import Path
from typing import Dict, Any
from playwright.async_api import async_playwright

NEWS_URL = "https://www.interactivebrokers.ie/portal/#/news3"
PROFILE_DIR = str(Path("./ibkr_profile").resolve())  # персистентный профиль Chromium
OUT_CSV = "ibkr_news_selected.csv"

# Набор возможных селекторов: страница — SPA, классы могут меняться.
# Оставлены несколько «устойчивых» вариантов; при необходимости подправьте после инспекции DevTools.
CARD_SEL = "[data-testid='news-card'], .news-card, .ibkr-news-card"
HEADLINE_SEL = "h3, [data-testid='headline'], [role='heading']"
SOURCE_SEL = "[data-testid='source'], .source, [aria-label*='source' i]"
TIME_SEL = "time, [data-testid='time']"
SENTIMENT_SEL = "[data-testid='sentiment'], .sentiment, [aria-label*='sentiment' i]"
LINK_SEL = "a[href]"

async def main():
    # Подготовка CSV с заголовком и множеством для дедупликации
    seen = set()
    exists = os.path.exists(OUT_CSV)
    f = open(OUT_CSV, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(f, fieldnames=["time", "source", "title", "url", "sentiment"])
    if not exists:
        writer.writeheader()

    async with async_playwright() as p:
        # Открываем persistent context — логин сохраняется между запусками
        browser = await p.chromium.launch_persistent_context(PROFILE_DIR, headless=False)
        page = await browser.new_page()
        await page.goto(NEWS_URL, wait_until="domcontentloaded")

        # Объявляем Python-функцию, которую будет вызывать JS в браузере при клике
        async def capture_news(payload: Dict[str, Any]):
            # Нормализация и запись, защита от дублей
            key = (payload.get("title","").strip(), payload.get("time","").strip())
            if key in seen:
                return
            seen.add(key)
            writer.writerow({
                "time": payload.get("time") or "",
                "source": payload.get("source") or "",
                "title": payload.get("title") or "",
                "url": payload.get("url") or "",
                "sentiment": payload.get("sentiment") or None
            })
            f.flush()
            print(f"Saved: {payload.get('title','')[:80]}")

        await page.expose_function("captureNews", capture_news)

        # Встраиваем делегирование кликов: любой клик внутри карточки — извлечь поля и отправить в Python
        inject_script = f"""
        (() => {{
          const CARD_SEL = `{CARD_SEL}`;
          const HEADLINE_SEL = `{HEADLINE_SEL}`;
          const SOURCE_SEL = `{SOURCE_SEL}`;
          const TIME_SEL = `{TIME_SEL}`;
          const SENTIMENT_SEL = `{SENTIMENT_SEL}`;
          const LINK_SEL = `{LINK_SEL}`;

          document.addEventListener("click", (ev) => {{
            const card = ev.target.closest(CARD_SEL);
            if (!card) return;

            const pickText = (sel) => {{
              const el = card.querySelector(sel);
              return el ? (el.textContent || "").trim() : null;
            }};
            const pickTime = () => {{
              const t = card.querySelector(TIME_SEL);
              return t ? (t.getAttribute("datetime") || t.textContent || "").trim() : null;
            }};
            const pickUrl = () => {{
              // Предпочитаем ссылку заголовка; если нет — первую ссылку внутри карточки
              const h = card.querySelector(HEADLINE_SEL);
              const a = h ? h.querySelector("a[href]") : null;
              const link = a || card.querySelector(LINK_SEL);
              return link ? link.href : null;
            }};

            const payload = {{
              title: pickText(HEADLINE_SEL),
              source: pickText(SOURCE_SEL),
              time: pickTime(),
              sentiment: pickText(SENTIMENT_SEL),  // может быть null
              url: pickUrl()
            }};
            // Фильтруем пустые клики (например, если кликнули по зоне без данных)
            if (payload.title) {{
              // Отправляем в Python
              window.captureNews(payload);
            }}
          }}, true);
        }})();
        """
        await page.add_init_script(inject_script)
        await page.evaluate(inject_script)

        print(
            "\nГотово. Открылось окно браузера.\n"
            "1) При необходимости выполните вход в IBKR и перейдите в раздел News (страница уже открыта).\n"
            "2) Примените фильтры/выборки.\n"
            "3) КЛИКАЙТЕ по карточкам/заголовкам, которые хотите сохранить.\n"
            f"Каждый клик добавляет строку в {OUT_CSV}. Для выхода закройте окно браузера."
        )

        # Ждём закрытия браузера пользователем
        await browser.wait_for_event("close", timeout=100_000)

    f.close()

if __name__ == "__main__":
    asyncio.run(main())
