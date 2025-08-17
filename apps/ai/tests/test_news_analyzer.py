import os, pytest, json
from apps.ai.pipelines.news_analyzer import analyze_one

LM_OK = bool(os.getenv("LMSTUDIO_URL"))

pytestmark = pytest.mark.skipif(not LM_OK, reason="Запусти LM Studio API (LMSTUDIO_URL)")

CASES = [
    {
        "id": 1,
        "headline": "NVIDIA announces record quarterly revenue and raises guidance",
        "summary": "The company reported better-than-expected figures and raised its outlook.",
        "symbols": ["NVDA"], "source": "dummy",
        "created_at": "2025-08-17T10:00:00Z", "url": "https://example.com",
    },
    {
        "id": 2,
        "headline": "NVIDIA Posts Record Low Quarterly Revenue",
        "summary": "Losses amid decline in card sales in China.",
        "symbols": ["NVDA"], "source": "dummy",
        "created_at": "2025-08-17T10:00:00Z", "url": "https://example.com",
    },
    # {
    #     "id": 3,
    #     "headline": "Apple Accidentally Exposes Top-Secret Hardware Plans Across Seven Product Lines",
    #     "summary": "Apple unintentionally revealed information about its future products, with the details discovered in the company&#39;s publicly available software code.",
    #     "symbols": ["AAPL"], "source": "dummy",
    #     "created_at": "2025-08-17T10:00:00Z", "url": "https://example.com",
    # },
    # {
    #     "id": 4,
    #     "headline": "David Tepper's Hedge Fund Bets On Intel, UnitedHealth; Cuts Position In Four Mag 7 Stocks",
    #     "summary": "David Tepper sold casino stocks and bought airline stocks in the second quarter. Here&#39;s a look at the changes made to the Appaloosa hedge fund.",
    #     "symbols": ["AAPL", "AMZN", "AVGO", "BABA", "BEKE", "BIDU", "CZR", "DAL", "FXI", "GOOG", "GT", "INTC", "IQV", "JD", "LHX", "LRCX", "LVS", "LYFT", "META", "MHK", "MSFT", "MU", "NRG", "NVDA", "ORCL", "PDD", "RTX", "SMH", "SPYX", "TSM", "UAL", "UBER", "UNH", "VST", "WHR", "WYNN", "XYZ"], "source": "dummy",
    #     "created_at": "2025-08-17T10:00:00Z", "url": "https://example.com",
    # },
]

@pytest.mark.parametrize("item", CASES, ids=lambda it: f"{it['id']}-{it['headline'][:24]}")
def test_analyze_one_smoke(item):
    out = analyze_one(item)
    assert {"summary_short","sentiment","actionability"} <= set(out)

def main():
    for item in CASES:
        print(f"\n=== Running case {item['id']} ===")
        out = analyze_one(item)
        print(json.dumps(out, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
