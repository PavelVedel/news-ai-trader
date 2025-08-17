# news-ai-trader

Workflow:
`[Ingest] → (DB/queue) → [AI Processing] → (signals) → [Trading Execution]`

Предложено чатом:
news-ai-trader/                   # корневая папка проекта (название условное)
├── apps/                         # прикладные сервисы
│   ├── ingest/                   # агрегатор новостей и рыночных данных
│   │   ├── alpaca_client/        # коннектор к Alpaca (новости, WS)
│   │   ├── finnhub_client/       # коннектор к Finnhub (новости, WS)
│   │   ├── rss_feeds/            # RSS/EDGAR/Nasdaq коннекторы
│   │   ├── pipelines/            # сбор, нормализация, дедупликация
│   │   └── tests/
│   │
│   ├── ai/                       # обработка через AI/LLM
│   │   ├── preprocessing/        # чистка текста, токенизация, чанкинг
│   │   ├── inference/            # вызовы LLM (lm-studio, OpenAI API и т.п.)
│   │   ├── rag/                  # база векторных эмбеддингов (FAISS/Weaviate)
│   │   └── tests/
│   │
│   ├── trading/                  # модуль работы с брокерами
│   │   ├── ibkr_client/          # обёртка над IB Gateway / API
│   │   ├── execution/            # ордер-менеджмент, риск-менеджмент
│   │   ├── strategies/           # простые торговые стратегии на основе сигналов
│   │   └── tests/
│   │
│   └── ui/                       # при желании: веб-панель или CLI
│       ├── api/                  # REST/gRPC слой
│       └── dashboard/            # графики, мониторинг потоков
│
├── libs/                         # общие библиотеки
│   ├── models/                   # dataclasses/типизированные объекты (NewsItem, Signal, Order)
│   ├── utils/                    # вспомогательные функции (логирование, конфиг, retry)
│   ├── storage/                  # общий доступ к БД/кэшу (Postgres, Redis, Parquet)
│   └── config/                   # централизованная загрузка .env, YAML
│
├── infra/                        # инфраструктура
│   ├── docker/                   # Dockerfile для сервисов
│   ├── compose.yaml               # docker-compose для локального запуска
│   ├── k8s/                       # манифесты для деплоя в кластер
│   └── ci-cd/                     # GitHub Actions, тесты, линтеры
│
├── notebooks/                     # эксперименты, ресерч-модели
├── tests/                         # e2e/интеграционные тесты
├── LICENSE
├── README.md
└── pyproject.toml / requirements.txt
