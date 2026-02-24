#!/usr/bin/env python3
"""
S&P 500 52-Week Highs & Lows Tracker
=====================================
- Initial mode: Downloads ~10 years of data and calculates daily 52w high/low counts
- Update mode: Fetches only recent data and appends new counts
- Outputs JSON for Chart.js dashboard

Usage:
  python fetch_52w_data.py --mode initial    # First-time full build (~30 min)
  python fetch_52w_data.py --mode update     # Daily incremental update (~5 min)
"""

import json
import os
import sys
import argparse
import time
from datetime import datetime, timedelta
from pathlib import Path

import yfinance as yf
import pandas as pd
import numpy as np

# ─── S&P 500 Tickers ───────────────────────────────────────────────────────────
SP500_TICKERS = [
    "MMM","AOS","ABT","ABBV","ACN","ADBE","AMD","AES","AFL","A",
    "APD","ABNB","AKAM","ALB","ARE","ALGN","ALLE","LNT","ALL","GOOGL",
    "GOOG","MO","AMZN","AMCR","AEE","AEP","AXP","AIG","AMT","AWK",
    "AMP","AME","AMGN","APH","ADI","AON","APA","APO","AAPL","AMAT",
    "APP","APTV","ACGL","ADM","ARES","ANET","AJG","AIZ","T","ATO",
    "ADSK","ADP","AZO","AVB","AVY","AXON","BKR","BALL","BAC","BAX",
    "BDX","BRK-B","BBY","TECH","BIIB","BLK","BX","XYZ","BK","BA",
    "BKNG","BSX","BMY","AVGO","BR","BRO","BF-B","BLDR","BG","BXP",
    "CHRW","CDNS","CPT","CPB","COF","CAH","CCL","CARR","CVNA","CAT",
    "CBOE","CBRE","CDW","COR","CNC","CNP","CF","CRL","SCHW","CHTR",
    "CVX","CMG","CB","CHD","CIEN","CI","CINF","CTAS","CSCO","C",
    "CFG","CLX","CME","CMS","KO","CTSH","COIN","CL","CMCSA","FIX",
    "CAG","COP","ED","STZ","CEG","COO","CPRT","GLW","CPAY","CTVA",
    "CSGP","COST","CTRA","CRH","CRWD","CCI","CSX","CMI","CVS","DHR",
    "DRI","DDOG","DVA","DECK","DE","DELL","DAL","DVN","DXCM","FANG",
    "DLR","DG","DLTR","D","DPZ","DASH","DOV","DOW","DHI","DTE",
    "DUK","DD","ETN","EBAY","ECL","EIX","EW","EA","ELV","EME",
    "EMR","ETR","EOG","EPAM","EQT","EFX","EQIX","EQR","ERIE","ESS",
    "EL","EG","EVRG","ES","EXC","EXE","EXPE","EXPD","EXR","XOM",
    "FFIV","FDS","FICO","FAST","FRT","FDX","FIS","FITB","FSLR","FE",
    "FISV","F","FTNT","FTV","FOXA","FOX","BEN","FCX","GRMN","IT",
    "GE","GEHC","GEV","GEN","GNRC","GD","GIS","GM","GPC","GILD",
    "GPN","GL","GDDY","GS","HAL","HIG","HAS","HCA","DOC","HSIC",
    "HSY","HPE","HLT","HOLX","HD","HON","HRL","HST","HWM","HPQ",
    "HUBB","HUM","HBAN","HII","IBM","IEX","IDXX","ITW","INCY","IR",
    "PODD","INTC","IBKR","ICE","IFF","IP","INTU","ISRG","IVZ","INVH",
    "IQV","IRM","JBHT","JBL","JKHY","J","JNJ","JCI","JPM","KVUE",
    "KDP","KEY","KEYS","KMB","KIM","KMI","KKR","KLAC","KHC","KR",
    "LHX","LH","LRCX","LW","LVS","LDOS","LEN","LII","LLY","LIN",
    "LYV","LMT","L","LOW","LULU","LYB","MTB","MPC","MAR","MRSH",
    "MLM","MAS","MA","MTCH","MKC","MCD","MCK","MDT","MRK","META",
    "MET","MTD","MGM","MCHP","MU","MSFT","MAA","MRNA","MOH","TAP",
    "MDLZ","MPWR","MNST","MCO","MS","MOS","MSI","MSCI","NDAQ","NTAP",
    "NFLX","NEM","NWSA","NWS","NEE","NKE","NI","NDSN","NSC","NTRS",
    "NOC","NCLH","NRG","NUE","NVDA","NVR","NXPI","ORLY","OXY","ODFL",
    "OMC","ON","OKE","ORCL","OTIS","PCAR","PKG","PLTR","PANW","PSKY",
    "PH","PAYX","PAYC","PYPL","PNR","PEP","PFE","PCG","PM","PSX",
    "PNW","PNC","POOL","PPG","PPL","PFG","PG","PGR","PLD","PRU",
    "PEG","PTC","PSA","PHM","PWR","QCOM","DGX","Q","RL","RJF",
    "RTX","O","REG","REGN","RF","RSG","RMD","RVTY","HOOD","ROK",
    "ROL","ROP","ROST","RCL","SPGI","CRM","SNDK","SBAC","SLB","STX",
    "SRE","NOW","SHW","SPG","SWKS","SJM","SW","SNA","SOLV","SO",
    "LUV","SWK","SBUX","STT","STLD","STE","SYK","SMCI","SYF","SNPS",
    "SYY","TMUS","TROW","TTWO","TPR","TRGP","TGT","TEL","TDY","TER",
    "TSLA","TXN","TPL","TXT","TMO","TJX","TKO","TTD","TSCO","TT",
    "TDG","TRV","TRMB","TFC","TYL","TSN","USB","UBER","UDR","ULTA",
    "UNP","UAL","UPS","URI","UNH","UHS","VLO","VTR","VLTO","VRSN",
    "VRSK","VZ","VRTX","VTRS","VICI","V","VST","VMC","WRB","GWW",
    "WAB","WMT","DIS","WBD","WM","WAT","WEC","WFC","WELL","WST",
    "WDC","WY","WSM","WMB","WTW","WDAY","WYNN","XEL","XYL","YUM",
    "ZBRA","ZBH","ZTS"
]

# ─── Config ─────────────────────────────────────────────────────────────────────
DATA_DIR = Path(__file__).parent / "data"
OUTPUT_FILE = DATA_DIR / "sp500_52w_highs_lows.json"
PRICES_CACHE = DATA_DIR / "prices_cache"
ROLLING_WINDOW = 252  # trading days in 52 weeks


def ensure_dirs():
    DATA_DIR.mkdir(exist_ok=True)
    PRICES_CACHE.mkdir(exist_ok=True)


def download_prices(tickers, start_date, end_date, batch_size=50):
    """Download price data in batches to avoid rate limits."""
    all_data = {}
    total_batches = (len(tickers) + batch_size - 1) // batch_size

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        batch_num = i // batch_size + 1
        print(f"  Downloading batch {batch_num}/{total_batches}: {batch[0]}...{batch[-1]}")

        try:
            data = yf.download(
                batch,
                start=start_date,
                end=end_date,
                auto_adjust=True,
                threads=True,
                progress=False,
            )

            if data.empty:
                print(f"  ⚠ Batch {batch_num} returned empty data")
                continue

            # Handle single ticker case
            if len(batch) == 1:
                ticker = batch[0]
                if "Close" in data.columns:
                    all_data[ticker] = data[["High", "Low", "Close"]].copy()
            else:
                for ticker in batch:
                    try:
                        ticker_data = data[[("High", ticker), ("Low", ticker), ("Close", ticker)]].copy()
                        ticker_data.columns = ["High", "Low", "Close"]
                        ticker_data = ticker_data.dropna()
                        if not ticker_data.empty:
                            all_data[ticker] = ticker_data
                    except (KeyError, Exception):
                        pass  # ticker not in this batch result

        except Exception as e:
            print(f"  ✗ Batch {batch_num} error: {e}")

        # Rate limiting
        if batch_num < total_batches:
            time.sleep(1)

    return all_data


def calculate_52w_counts(all_data):
    """
    For each trading day, count how many S&P 500 stocks are at:
    - 52-week high (today's high >= rolling 252-day max high)
    - 52-week low (today's low <= rolling 252-day min low)
    """
    print("\n📊 Calculating 52-week highs/lows...")

    # Get union of all dates
    all_dates = set()
    for ticker_data in all_data.values():
        all_dates.update(ticker_data.index.tolist())
    all_dates = sorted(all_dates)

    results = []

    for date in all_dates:
        high_count = 0
        low_count = 0
        total_active = 0

        for ticker, df in all_data.items():
            if date not in df.index:
                continue

            # Get data up to this date
            mask = df.index <= date
            historical = df[mask].tail(ROLLING_WINDOW)

            if len(historical) < 20:  # need minimum data
                continue

            total_active += 1
            today_high = historical["High"].iloc[-1]
            today_low = historical["Low"].iloc[-1]
            rolling_max = historical["High"].max()
            rolling_min = historical["Low"].min()

            # At 52-week high: today's high equals the rolling max
            if today_high >= rolling_max * 0.999:  # 0.1% tolerance
                high_count += 1

            # At 52-week low: today's low equals the rolling min
            if today_low <= rolling_min * 1.001:  # 0.1% tolerance
                low_count += 1

        if total_active > 0:
            date_str = date.strftime("%Y-%m-%d") if hasattr(date, "strftime") else str(date)[:10]
            results.append({
                "date": date_str,
                "highs": high_count,
                "lows": low_count,
                "active": total_active,
            })

    return results


def fetch_spy_prices(start_date, end_date):
    """Download SPY ETF prices for overlay."""
    print("\n📈 Fetching SPY price data...")
    try:
        spy = yf.download("SPY", start=start_date, end=end_date, auto_adjust=True, progress=False)
        if spy.empty:
            print("  ⚠ SPY data empty")
            return {}
        spy_dict = {}
        for date, row in spy.iterrows():
            date_str = date.strftime("%Y-%m-%d")
            close_val = row["Close"]
            # Handle both Series and scalar
            if hasattr(close_val, "iloc"):
                close_val = close_val.iloc[0]
            spy_dict[date_str] = round(float(close_val), 2)
        print(f"  ✅ SPY: {len(spy_dict)} data points")
        return spy_dict
    except Exception as e:
        print(f"  ✗ SPY download error: {e}")
        return {}


def merge_spy_data(results, spy_prices):
    """Merge SPY closing prices into results."""
    merged = 0
    for item in results:
        if item["date"] in spy_prices:
            item["spy_close"] = spy_prices[item["date"]]
            merged += 1
    print(f"  📊 Merged SPY prices: {merged}/{len(results)} data points")
    return results


def load_spy_csv():
    """Load SPY data from local CSV if available."""
    spy_csv = DATA_DIR / "SPY.csv"
    if not spy_csv.exists():
        return {}
    print("  📂 Loading SPY.csv...")
    spy_dict = {}
    try:
        df = pd.read_csv(spy_csv)
        for _, row in df.iterrows():
            date_str = str(row.get("Date", "")).strip()[:10]
            close_val = row.get("Close", None)
            if date_str and close_val and not pd.isna(close_val):
                spy_dict[date_str] = round(float(close_val), 2)
        print(f"  ✅ Loaded {len(spy_dict)} SPY prices from CSV")
    except Exception as e:
        print(f"  ⚠ CSV load error: {e}")
    return spy_dict


def save_results(results):
    """Save results as JSON for the dashboard."""
    output = {
        "last_updated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_tickers": len(SP500_TICKERS),
        "data": results,
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f)

    print(f"\n✅ Saved {len(results)} data points to {OUTPUT_FILE}")


def run_initial():
    """Full historical build (~10 years)."""
    ensure_dirs()

    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=365 * 11)).strftime("%Y-%m-%d")
    # Extra year for rolling window warmup

    print(f"🚀 Initial build: {start_date} → {end_date}")
    print(f"   Downloading {len(SP500_TICKERS)} tickers...\n")

    all_data = download_prices(SP500_TICKERS, start_date, end_date)
    print(f"\n✅ Downloaded data for {len(all_data)} tickers")

    # Cache prices for future incremental updates
    for ticker, df in all_data.items():
        df.to_csv(PRICES_CACHE / f"{ticker}.csv")
    print(f"💾 Cached price data to {PRICES_CACHE}")

    results = calculate_52w_counts(all_data)

    # Trim to ~10 years (remove the warmup year)
    cutoff = (datetime.now() - timedelta(days=365 * 10)).strftime("%Y-%m-%d")
    results = [r for r in results if r["date"] >= cutoff]

    # Merge SPY data (CSV first, then API for missing)
    spy_prices = load_spy_csv()
    api_spy = fetch_spy_prices(cutoff, end_date)
    spy_prices.update(api_spy)
    results = merge_spy_data(results, spy_prices)

    save_results(results)


def run_update():
    """Incremental daily update."""
    ensure_dirs()

    # Load existing results
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE) as f:
            existing = json.load(f)
        existing_data = existing.get("data", [])
        last_date = existing_data[-1]["date"] if existing_data else None
        print(f"📂 Loaded {len(existing_data)} existing data points (last: {last_date})")
    else:
        print("⚠ No existing data found. Running initial build instead.")
        return run_initial()

    # Download recent data (last 300 trading days for rolling window + buffer)
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")

    print(f"\n📡 Fetching recent data: {start_date} → {end_date}")
    all_data = download_prices(SP500_TICKERS, start_date, end_date)
    print(f"✅ Downloaded data for {len(all_data)} tickers")

    # Calculate counts for new dates only
    results = calculate_52w_counts(all_data)

    # Merge: keep existing data, append only new dates
    existing_dates = {r["date"] for r in existing_data}
    new_results = [r for r in results if r["date"] not in existing_dates]

    if new_results:
        combined = existing_data + new_results
        # Also update the last few days in case of data corrections
        recent_cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        updated_dates = {r["date"] for r in results if r["date"] >= recent_cutoff}

        combined_filtered = [r for r in combined if r["date"] not in updated_dates]
        recent_updates = [r for r in results if r["date"] >= recent_cutoff]
        combined = sorted(combined_filtered + recent_updates, key=lambda x: x["date"])

        # Merge SPY data
        spy_prices = load_spy_csv()
        api_spy = fetch_spy_prices(start_date, end_date)
        spy_prices.update(api_spy)
        combined = merge_spy_data(combined, spy_prices)

        save_results(combined)
        print(f"📈 Added {len(new_results)} new data points")
    else:
        print("ℹ No new data to add")


def main():
    parser = argparse.ArgumentParser(description="S&P 500 52-Week Highs/Lows Tracker")
    parser.add_argument(
        "--mode",
        choices=["initial", "update"],
        default="update",
        help="'initial' for full 10-year build, 'update' for daily incremental",
    )
    args = parser.parse_args()

    if args.mode == "initial":
        run_initial()
    else:
        run_update()


if __name__ == "__main__":
    main()
