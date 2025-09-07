from fastmcp import FastMCP
from typing import Any, Literal
from pathlib import Path

from libs.database.connection import DatabaseConnection

mcp = FastMCP(name="LocalMarketInfos")

@mcp.tool(
    name="find_symbol_infos",           # Custom tool name for the LLM
    description=(
        "Fetch symbol info from the local DB. Use 'fields' to limit output and save tokens "
        "(e.g., ['symbol','long_name','sector','industry','website','officers_json',"
        "'last_updated','data_source']); if omitted, returns all."
    ),
    tags={"catalog", "search"},      # Optional tags for organization/filtering
)
def find_symbol_infos(symbol: str, filelds: list[Literal['symbol', 'long_name', 'short_name', 'display_name', 'website', 'ir_website', 'phone', 'address1', 'city', 'state', 'zip', 'country', 'sector', 'industry', 'full_time_employees', 'long_business_summary', 'exchange', 'currency', 'officers_json', 'raw_info_json', 'last_updated', 'data_source']] = None) -> dict[str, Any]:
    """Internal function description (ignored if description is provided above)."""
    # ['symbol', 'long_name', 'short_name', 'display_name', 'website', 'ir_website', 'phone', 'address1', 'city', 'state', 'zip', 'country', 'sector', 'industry', 'full_time_employees', 'long_business_summary', 'exchange', 'currency', 'officers_json', 'raw_info_json', 'last_updated', 'data_source']
    # Implementation...
    script_folder = Path(__file__).parent
    # Use a normalized absolute path to the database to avoid issues with '..' segments
    db_path = (script_folder / ".." / ".." / ".." / "data" / "db" / "news.db").resolve()
    db = DatabaseConnection(db_path)
    # Normalize symbol (strip spaces, uppercase, drop leading '$')
    clean_symbol = symbol.strip() #.upper().lstrip('$')
    infos = db.get_infos(clean_symbol)
    if infos is None:
        return {"error": "Symbol not found", "db_location": str(db_path), "mcp_folder": str(script_folder)}
    else:
        if filelds is None:
            return infos
        else:
            # maybe not all fields are present
            return {field: infos.get(field, None) for field in filelds}



@mcp.tool(
    name="find_raw_news",
    description=(
        "Return raw news for a given symbol. If start_date and end_date are provided (ISO8601), "
        "returns news within that date range for the symbol. You can pass 'limit' to cap results; "
        "omit or set limit<=0 to return all in the range. Otherwise, returns the latest 'limit' news "
        "for the symbol (defaults to 10 if not provided). Set only_headlines=True to return only "
        "news_id and headline for each item (use for brief overview)."
    ),
    tags={"catalog", "search"},
)
def find_raw_news(symbol: str, start_date: str = "", end_date: str = "", limit: int | None = None, only_headlines: bool = False) -> dict[str, Any]:
    """Fetch raw news from local DB for a symbol, optionally within a time range."""
    script_folder = Path(__file__).parent
    db_path = (script_folder / ".." / ".." / ".." / "data" / "db" / "news.db").resolve()
    db = DatabaseConnection(db_path)

    clean_symbol = symbol.strip()

    # Helper to convert sqlite rows to plain dicts
    def rows_to_dicts(rows: list) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for r in rows:
            try:
                out.append(dict(r))
            except Exception:
                # Fallback: keep as-is if conversion fails
                out.append(r)
        return out

    # If a time range is provided, get range then filter by symbol
    if start_date and end_date:
        # Request a large batch from DB, then filter by symbol; optionally cap by 'limit'
        rows = db.get_news_by_date_range(start_date=start_date, end_date=end_date, limit=1000000)
        filtered: list[Any] = []
        for row in rows:
            try:
                symbols = row["symbols_json"]
                import json as _json
                syms = _json.loads(symbols) if isinstance(symbols, str) else symbols
                if isinstance(syms, list) and clean_symbol in syms:
                    filtered.append(row)
            except Exception:
                # Skip malformed records
                continue
        # Apply limit after filtering (only if provided and > 0)
        if limit is not None and int(limit) > 0:
            selected = filtered[: int(limit)]
        else:
            selected = filtered
        items_full = rows_to_dicts(selected)
        items = (
            [{"news_id": it.get("news_id"), "headline": it.get("headline") } for it in items_full]
            if only_headlines else items_full
        )
        return {
            "symbol": clean_symbol,
            "count": len(items_full),
            "items": items,
            "db_location": str(db_path),
        }
    else:
        # Latest N news for the symbol (fallback to 10 if limit not provided)
        effective_limit = 10 if (limit is None or int(limit) <= 0) else max(1, int(limit))
        rows = db.get_news_by_symbol(symbol=clean_symbol, limit=effective_limit)
        items_full = rows_to_dicts(rows)
        items = (
            [{"news_id": it.get("news_id"), "headline": it.get("headline") } for it in items_full]
            if only_headlines else items_full
        )
        return {
            "symbol": clean_symbol,
            "count": len(items_full),
            "items": items,
            "db_location": str(db_path),
        }


@mcp.tool(
    name="find_raw_news_by_id",
    description=(
        "Return a single raw news item by its ID from the local DB."
    ),
    tags={"catalog", "search"},
)
def find_raw_news_by_id(news_id: int) -> dict[str, Any]:
    """Fetch one raw news record by ID."""
    script_folder = Path(__file__).parent
    db_path = (script_folder / ".." / ".." / ".." / "data" / "db" / "news.db").resolve()
    db = DatabaseConnection(db_path)

    try:
        nid = int(news_id)
    except Exception:
        return {"error": "Invalid news_id", "db_location": str(db_path)}

    row = db.get_news_by_id(nid)
    if not row:
        return {"error": "News not found", "news_id": nid, "db_location": str(db_path)}

    try:
        item = dict(row)
    except Exception:
        item = row

    return {"news_id": nid, "item": item, "db_location": str(db_path)}

if __name__ == "__main__":
    mcp.run()
    