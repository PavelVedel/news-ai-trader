"""
Script for updating both infos and fundamentals from Yahoo Finance
Fetches ticker.info once per symbol and extracts both datasets
Skips symbols updated less than 7 days ago
"""

import json
import time
from typing import Dict, Optional
from datetime import datetime

import yfinance as yf
from libs.database.connection import DatabaseConnection


def _safe_get(data: Dict, key: str, default=None):
    """Safely get value from dictionary, handling N/A values"""
    try:
        value = data.get(key, default)
        if isinstance(value, str) and value.upper() in ['N/A', 'NAN', 'INF', '-INF']:
            return default
        return value
    except:
        return default


def extract_fundamentals(symbol: str, info: Dict) -> Optional[Dict]:
    """Extract fundamentals data from ticker.info dict"""
    try:
        fundamentals = {
            'symbol': symbol,
            
            # Basic financial metrics
            'market_cap': _safe_get(info, 'marketCap'),
            'enterprise_value': _safe_get(info, 'enterpriseValue'),
            'pe_ratio': _safe_get(info, 'trailingPE'),
            'forward_pe': _safe_get(info, 'forwardPE'),
            'peg_ratio': _safe_get(info, 'pegRatio'),
            'price_to_book': _safe_get(info, 'priceToBook'),
            'price_to_sales': _safe_get(info, 'priceToSalesTrailing12Months'),
            'enterprise_to_revenue': _safe_get(info, 'enterpriseToRevenue'),
            'enterprise_to_ebitda': _safe_get(info, 'enterpriseToEbitda'),
            
            # Profitability metrics
            'return_on_equity': _safe_get(info, 'returnOnEquity'),
            'return_on_assets': _safe_get(info, 'returnOnAssets'),
            'return_on_capital': _safe_get(info, 'returnOnCapital'),
            
            # Liquidity metrics
            'current_ratio': _safe_get(info, 'currentRatio'),
            'quick_ratio': _safe_get(info, 'quickRatio'),
            'debt_to_equity': _safe_get(info, 'debtToEquity'),
            
            # Dividends
            'dividend_yield': _safe_get(info, 'dividendYield'),
            'dividend_rate': _safe_get(info, 'dividendRate'),
            'payout_ratio': _safe_get(info, 'payoutRatio'),
            'five_year_avg_dividend_yield': _safe_get(info, 'fiveYearAvgDividendYield'),
            'trailing_annual_dividend_rate': _safe_get(info, 'trailingAnnualDividendRate'),
            'trailing_annual_dividend_yield': _safe_get(info, 'trailingAnnualDividendYield'),
            
            # Technical indicators
            'beta': _safe_get(info, 'beta'),
            'fifty_two_week_high': _safe_get(info, 'fiftyTwoWeekHigh'),
            'fifty_two_week_low': _safe_get(info, 'fiftyTwoWeekLow'),
            'fifty_day_average': _safe_get(info, 'fiftyDayAverage'),
            'two_hundred_day_average': _safe_get(info, 'twoHundredDayAverage'),
            'fifty_two_week_change_percent': _safe_get(info, 'fiftyTwoWeekChangePercent'),
            'fifty_day_average_change': _safe_get(info, 'fiftyDayAverageChange'),
            'fifty_day_average_change_percent': _safe_get(info, 'fiftyDayAverageChangePercent'),
            'two_hundred_day_average_change': _safe_get(info, 'twoHundredDayAverageChange'),
            'two_hundred_day_average_change_percent': _safe_get(info, 'twoHundredDayAverageChangePercent'),
            
            # Additional financial metrics
            'book_value': _safe_get(info, 'bookValue'),
            'total_cash': _safe_get(info, 'totalCash'),
            'total_cash_per_share': _safe_get(info, 'totalCashPerShare'),
            'total_debt': _safe_get(info, 'totalDebt'),
            'total_revenue': _safe_get(info, 'totalRevenue'),
            'revenue_per_share': _safe_get(info, 'revenuePerShare'),
            'gross_profits': _safe_get(info, 'grossProfits'),
            'free_cashflow': _safe_get(info, 'freeCashflow'),
            'operating_cashflow': _safe_get(info, 'operatingCashflow'),
            'ebitda': _safe_get(info, 'ebitda'),
            'net_income_to_common': _safe_get(info, 'netIncomeToCommon'),
            
            # Growth metrics
            'earnings_growth': _safe_get(info, 'earningsGrowth'),
            'revenue_growth': _safe_get(info, 'revenueGrowth'),
            'earnings_quarterly_growth': _safe_get(info, 'earningsQuarterlyGrowth'),
            
            # Margins
            'gross_margins': _safe_get(info, 'grossMargins'),
            'ebitda_margins': _safe_get(info, 'ebitdaMargins'),
            'operating_margins': _safe_get(info, 'operatingMargins'),
            'profit_margins': _safe_get(info, 'profitMargins'),
            
            # Shares and ownership
            'shares_outstanding': _safe_get(info, 'sharesOutstanding'),
            'float_shares': _safe_get(info, 'floatShares'),
            'shares_short': _safe_get(info, 'sharesShort'),
            'shares_short_prior_month': _safe_get(info, 'sharesShortPriorMonth'),
            'shares_percent_shares_out': _safe_get(info, 'sharesPercentSharesOut'),
            'held_percent_insiders': _safe_get(info, 'heldPercentInsiders'),
            'held_percent_institutions': _safe_get(info, 'heldPercentInstitutions'),
            'short_ratio': _safe_get(info, 'shortRatio'),
            'short_percent_of_float': _safe_get(info, 'shortPercentOfFloat'),
            
            # Analyst estimates
            'target_high_price': _safe_get(info, 'targetHighPrice'),
            'target_low_price': _safe_get(info, 'targetLowPrice'),
            'target_mean_price': _safe_get(info, 'targetMeanPrice'),
            'target_median_price': _safe_get(info, 'targetMedianPrice'),
            'recommendation_mean': _safe_get(info, 'recommendationMean'),
            'recommendation_key': _safe_get(info, 'recommendationKey'),
            'number_of_analyst_opinions': _safe_get(info, 'numberOfAnalystOpinions'),
            'average_analyst_rating': _safe_get(info, 'averageAnalystRating'),
            
            # ESG risks
            'audit_risk': _safe_get(info, 'auditRisk'),
            'board_risk': _safe_get(info, 'boardRisk'),
            'compensation_risk': _safe_get(info, 'compensationRisk'),
            'share_holder_rights_risk': _safe_get(info, 'shareHolderRightsRisk'),
            'overall_risk': _safe_get(info, 'overallRisk'),
            
            # Timestamps
            'last_fiscal_year_end': _safe_get(info, 'lastFiscalYearEnd'),
            'next_fiscal_year_end': _safe_get(info, 'nextFiscalYearEnd'),
            'most_recent_quarter': _safe_get(info, 'mostRecentQuarter'),
            'ex_dividend_date': _safe_get(info, 'exDividendDate'),
            'dividend_date': _safe_get(info, 'dividendDate'),
            'last_dividend_date': _safe_get(info, 'lastDividendDate'),
            'earnings_timestamp': _safe_get(info, 'earningsTimestamp'),
            'earnings_timestamp_start': _safe_get(info, 'earningsTimestampStart'),
            'earnings_timestamp_end': _safe_get(info, 'earningsTimestampEnd'),
            
            # Stock splits
            'last_split_factor': _safe_get(info, 'lastSplitFactor'),
            'last_split_date': _safe_get(info, 'lastSplitDate'),
            
            # Metadata
            'sector': _safe_get(info, 'sector'),
            'industry': _safe_get(info, 'industry'),
            'country': _safe_get(info, 'country'),
            'currency': _safe_get(info, 'currency'),
            'exchange': _safe_get(info, 'exchange'),
            'quote_type': _safe_get(info, 'quoteType'),
            'market_state': _safe_get(info, 'marketState'),
            
            # Service fields
            'last_updated': datetime.now().isoformat(),
            'data_source': 'yahoo_finance'
        }
        return fundamentals
    except Exception as e:
        print(f"Error extracting fundamentals for {symbol}: {e}")
        return None


def extract_infos(symbol: str, info: Dict) -> Optional[Dict]:
    """Extract infos data from ticker.info dict"""
    try:
        # Prepare officer list (only name/title)
        officers = info.get("companyOfficers") or []
        officers_small = []
        for o in officers:
            if not isinstance(o, dict):
                continue
            name = o.get("name")
            title = o.get("title")
            if name or title:
                officers_small.append({"name": name, "title": title})

        payload = {
            # Key
            "symbol": symbol,

            # Names
            "long_name": _safe_get(info, "longName"),
            "short_name": _safe_get(info, "shortName"),
            "display_name": _safe_get(info, "displayName"),

            # Website/IR/Contacts
            "website": _safe_get(info, "website"),
            "ir_website": _safe_get(info, "irWebsite"),
            "phone": _safe_get(info, "phone"),

            # Address
            "address1": _safe_get(info, "address1"),
            "city": _safe_get(info, "city"),
            "state": _safe_get(info, "state"),
            "zip": _safe_get(info, "zip"),
            "country": _safe_get(info, "country"),

            # Sector/Industry
            "sector": _safe_get(info, "sector"),
            "industry": _safe_get(info, "industry"),

            # Employees and description
            "full_time_employees": _safe_get(info, "fullTimeEmployees"),
            "long_business_summary": _safe_get(info, "longBusinessSummary"),

            # Exchange/Currency (metadata)
            "exchange": _safe_get(info, "fullExchangeName") or _safe_get(info, "exchange"),
            "currency": _safe_get(info, "currency"),

            # Officers
            "officers_json": json.dumps(officers_small, ensure_ascii=False),

            # Raw JSON for further parsing if needed
            "raw_info_json": json.dumps(info, ensure_ascii=False),

            # Service fields
            "last_updated": datetime.now().isoformat(),
            "data_source": "yahoo_finance",
        }
        return payload
    except Exception as e:
        print(f"Error extracting infos for {symbol}: {e}")
        return None


def update_all(
    db: DatabaseConnection,
    min_age_days: int = 7,
    delay_seconds: float = 0.0,
    max_symbols: Optional[int] = None
):
    """
    Update both fundamentals and infos for symbols needing update
    
    Args:
        db: Database connection
        min_age_days: Minimum age in days to require update (default: 7)
        delay_seconds: Delay between requests (default: 0.0)
        max_symbols: Maximum number of symbols to process (optional)
    """
    # Ensure tables exist
    if not db.ensure_fundamentals_table():
        print("[ERROR] Failed to create/check fundamentals table")
        return
    
    if not db.ensure_infos_table():
        print("[ERROR] Failed to create/check infos table")
        return
    
    # Get all symbols from database
    all_symbols = db.get_all_symbols(filter_strange=True)
    if not all_symbols:
        print("No symbols found in database")
        return
    
    # Get symbols needing fundamentals update
    fundamentals_symbols = db.get_fundamentals_symbols_needing_update(max_age_days=min_age_days)
    
    # Get symbols needing infos update
    infos_symbols = db.get_infos_symbols_needing_update(all_symbols, max_age_days=min_age_days)
    
    # Create union of both lists (symbols needing either update)
    symbols_to_update = sorted(set(fundamentals_symbols + infos_symbols))
    
    if not symbols_to_update:
        print(f"All symbols are up to date (updated less than {min_age_days} days ago)")
        return
    
    # Apply max_symbols limit if provided
    if max_symbols is not None:
        symbols_to_update = symbols_to_update[:max_symbols]
    
    print(f"Found {len(symbols_to_update)} symbols needing update")
    print(f"Fundamentals: {len(fundamentals_symbols)}, Infos: {len(infos_symbols)}")
    print(f"Processing with {min_age_days} day minimum age threshold")
    print("=" * 60)
    
    # Track statistics
    fundamentals_ok = 0
    fundamentals_fail = 0
    infos_ok = 0
    infos_fail = 0
    tic = time.time()

    # Process each symbol
    for i, symbol in enumerate(symbols_to_update, 1):
        try:
            needs_fundamentals = symbol in fundamentals_symbols
            needs_infos = symbol in infos_symbols
            
            print(f"[{i}/{len(symbols_to_update)}|{i/len(symbols_to_update)*100:.2f}%] Processing {symbol}...", end=" ")
            
            # Fetch ticker.info once
            ticker = yf.Ticker(symbol)
            info = ticker.info

            """
            Note: 
            info does not work for $symbols because it's social-tag, e.g. $SHIB
            but it's working with SHIB-USD or fast_info
            however we use info because info contains everything (fundamentals and description)
            """
            
            if not info:
                print("No data")
                if needs_fundamentals:
                    fundamentals_fail += 1
                if needs_infos:
                    infos_fail += 1
                continue
            
            # Extract and save fundamentals if needed
            if needs_fundamentals:
                fundamentals = extract_fundamentals(symbol, info)
                if fundamentals:
                    if db.save_fundamentals(fundamentals):
                        fundamentals_ok += 1
                        print("Fundamentals OK;", end=" ")
                    else:
                        fundamentals_fail += 1
                        print("Fundamentals FAIL;", end=" ")
                else:
                    fundamentals_fail += 1
                    print("Fundamentals FAIL;", end=" ")
            
            # Extract and save infos if needed
            if needs_infos:
                infos = extract_infos(symbol, info)
                if infos:
                    if db.save_infos(infos):
                        infos_ok += 1
                        print("Infos OK;", end=" ")
                    else:
                        infos_fail += 1
                        print("Infos FAIL;", end=" ")
                else:
                    infos_fail += 1
                    print("Infos FAIL;", end=" ")
            
            toc = time.time()
            approx_time_left = (toc-tic)/i*(len(symbols_to_update)-i)
            approx_time_left_str = f"{int(approx_time_left // 60):02d}:{int(approx_time_left % 60):02d}"
            print(f"\nApproximate time left: {approx_time_left_str}")
            print()  # New line
            
            # Delay between requests
            if delay_seconds > 0 and i < len(symbols_to_update):
                time.sleep(delay_seconds)
                
                
        except KeyboardInterrupt:
            print("\n\nUpdate interrupted by user")
            break
        except Exception as e:
            print(f"ERROR: {e}")
            if needs_fundamentals:
                fundamentals_fail += 1
            if needs_infos:
                infos_fail += 1
            continue
    
    # Print final statistics
    print("=" * 60)
    print("UPDATE COMPLETED")
    print("=" * 60)
    print(f"Total symbols processed: {len(symbols_to_update)}")
    print(f"\nFundamentals:")
    print(f"  Success: {fundamentals_ok}")
    print(f"  Failed: {fundamentals_fail}")
    if fundamentals_ok + fundamentals_fail > 0:
        print(f"  Success rate: {(fundamentals_ok/(fundamentals_ok+fundamentals_fail)*100):.1f}%")
    
    print(f"\nInfos:")
    print(f"  Success: {infos_ok}")
    print(f"  Failed: {infos_fail}")
    if infos_ok + infos_fail > 0:
        print(f"  Success rate: {(infos_ok/(infos_ok+infos_fail)*100):.1f}%")


def main():
    """Main function"""
    print("=" * 60)
    print("UPDATING INFOS AND FUNDAMENTALS FROM YAHOO FINANCE")
    print("=" * 60)
    
    try:
        db = DatabaseConnection("data/db/news.db")
        
        # Update with default parameters
        # min_age_days=7: skip symbols updated less than 7 days ago
        # delay_seconds=0.0: no delay between requests
        # max_symbols=None: process all symbols
        update_all(
            db=db,
            min_age_days=7,
            delay_seconds=0.0,
            max_symbols=None
        )
        
        db.close()
        
    except KeyboardInterrupt:
        print("\nUpdate interrupted by user")
    except Exception as e:
        print(f"Critical error: {e}")


if __name__ == "__main__":
    main()

