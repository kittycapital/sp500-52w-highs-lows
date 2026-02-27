#!/usr/bin/env python3
"""
Market Sentiment Dashboard - 데이터 수집
=========================================
기존 52w highs/lows JSON 데이터를 재활용하고,
VIX/TLT/HYG/LQD + S&P500 전종목 Close를 추가 다운로드하여
6개 센티먼트 지표 + 종합 공포/탐욕 지수를 계산합니다.

Usage:
  python fetch_sentiment.py              # 일반 업데이트 (2년치)
  python fetch_sentiment.py --full       # 전체 빌드 (10년치, 52w JSON 범위)
"""
import json
import argparse
import time
from datetime import datetime, timedelta
from pathlib import Path

import yfinance as yf
import pandas as pd
import numpy as np

# ─── S&P 500 Tickers (52w 스크립트와 동일) ─────────────────────────────────────
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

# 추가 다운로드 티커
EXTRA_TICKERS = ["^VIX", "SPY", "TLT", "HYG", "LQD"]

DATA_DIR = Path(__file__).parent / "data"
HIGHS_LOWS_FILE = DATA_DIR / "sp500_52w_highs_lows.json"
SENTIMENT_FILE = DATA_DIR / "sentiment.json"


def download_batch(tickers, start_date, end_date, batch_size=50):
    """배치 다운로드 (Close만)"""
    all_close = {}
    total_batches = (len(tickers) + batch_size - 1) // batch_size

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        batch_num = i // batch_size + 1
        print(f"  배치 {batch_num}/{total_batches}: {batch[0]}...{batch[-1]}")

        try:
            data = yf.download(
                batch, start=start_date, end=end_date,
                auto_adjust=True, threads=True, progress=False
            )
            if data.empty:
                continue

            if len(batch) == 1:
                ticker = batch[0]
                if "Close" in data.columns:
                    close_col = data["Close"]
                    if isinstance(close_col, pd.DataFrame):
                        close_col = close_col.iloc[:, 0]
                    all_close[ticker] = close_col.dropna()
            else:
                for ticker in batch:
                    try:
                        if isinstance(data.columns, pd.MultiIndex):
                            col = data[("Close", ticker)].dropna()
                        else:
                            col = data["Close"].dropna()
                        if not col.empty:
                            all_close[ticker] = col
                    except (KeyError, Exception):
                        pass
        except Exception as e:
            print(f"  ✗ 배치 {batch_num} 에러: {e}")

        if batch_num < total_batches:
            time.sleep(1)

    return all_close


def load_highs_lows():
    """기존 52w highs/lows JSON 로드"""
    if not HIGHS_LOWS_FILE.exists():
        print("⚠ sp500_52w_highs_lows.json 없음")
        return {}
    with open(HIGHS_LOWS_FILE) as f:
        data = json.load(f)
    records = data.get("data", [])
    result = {}
    for r in records:
        result[r["date"]] = {
            "highs": r.get("highs", 0),
            "lows": r.get("lows", 0),
            "active": r.get("active", 500),
            "spy_close": r.get("spy_close"),
        }
    print(f"  ✅ 52w 데이터 로드: {len(result)}일")
    return result


# ─── 개별 지표 계산 ─────────────────────────────────────────────────────────────

def calc_vix_score(vix_series):
    """
    VIX 백분위 기반 점수 (0=극단적 공포, 100=극단적 탐욕)
    VIX 높을수록 공포 → 점수 낮음
    1년 롤링 백분위 사용
    """
    scores = {}
    vix_vals = vix_series.sort_index()
    for i in range(252, len(vix_vals)):
        date = vix_vals.index[i]
        date_str = date.strftime("%Y-%m-%d")
        window = vix_vals.iloc[i - 252:i + 1]
        current = vix_vals.iloc[i]
        percentile = (window < current).sum() / len(window) * 100
        # VIX 높으면 공포 → 점수 반전
        scores[date_str] = {
            "value": round(float(current), 2),
            "score": round(100 - percentile, 1),
        }
    return scores


def calc_momentum_score(spy_series):
    """
    SPY vs 125일 이동평균선
    위에 있으면 탐욕(100), 아래면 공포(0)
    거리에 따라 그라데이션
    """
    scores = {}
    spy = spy_series.sort_index()
    ma125 = spy.rolling(125).mean()

    for i in range(125, len(spy)):
        date = spy.index[i]
        date_str = date.strftime("%Y-%m-%d")
        price = spy.iloc[i]
        ma = ma125.iloc[i]
        if pd.isna(ma) or ma == 0:
            continue
        deviation = (price - ma) / ma * 100  # % above/below MA
        # Clamp to -10% ~ +10% range, map to 0~100
        score = max(0, min(100, (deviation + 10) / 20 * 100))
        scores[date_str] = {
            "value": round(float(deviation), 2),
            "ma125": round(float(ma), 2),
            "score": round(score, 1),
        }
    return scores


def calc_highlow_score(highs_lows_data):
    """
    52주 신고/신저 비율 기반
    Net highs % = (highs - lows) / active * 100
    높을수록 탐욕
    """
    scores = {}
    for date_str, d in sorted(highs_lows_data.items()):
        active = d.get("active", 500)
        if active == 0:
            continue
        net = d["highs"] - d["lows"]
        net_pct = net / active * 100
        # -20% ~ +20% 범위를 0~100으로
        score = max(0, min(100, (net_pct + 20) / 40 * 100))
        scores[date_str] = {
            "highs": d["highs"],
            "lows": d["lows"],
            "net_pct": round(net_pct, 2),
            "score": round(score, 1),
        }
    return scores


def calc_breadth_score(all_close, dates):
    """
    S&P 500 종목 중 200일 이동평균선 위에 있는 비율
    높을수록 탐욕
    """
    scores = {}

    # 각 종목의 200일 MA 계산
    ma200_dict = {}
    for ticker, series in all_close.items():
        if ticker in EXTRA_TICKERS:
            continue
        ma200_dict[ticker] = series.rolling(200).mean()

    for date in dates:
        above = 0
        total = 0
        ts = pd.Timestamp(date)
        for ticker, series in all_close.items():
            if ticker in EXTRA_TICKERS:
                continue
            if ts not in series.index or ticker not in ma200_dict:
                continue
            ma = ma200_dict[ticker]
            if ts not in ma.index or pd.isna(ma.loc[ts]):
                continue
            total += 1
            if series.loc[ts] > ma.loc[ts]:
                above += 1

        if total > 50:
            pct = above / total * 100
            score = max(0, min(100, pct))
            scores[date] = {
                "above200": above,
                "total": total,
                "pct": round(pct, 1),
                "score": round(score, 1),
            }
    return scores


def calc_safe_haven_score(tlt_series, spy_series):
    """
    Safe Haven Demand: TLT vs SPY 20일 상대 수익률
    TLT가 아웃퍼폼 → 채권으로 도피 → 공포
    SPY가 아웃퍼폼 → 주식 선호 → 탐욕
    """
    scores = {}
    tlt = tlt_series.sort_index()
    spy = spy_series.sort_index()

    # 인덱스 맞추기
    common = tlt.index.intersection(spy.index)
    tlt = tlt.loc[common]
    spy = spy.loc[common]

    tlt_ret20 = tlt.pct_change(20) * 100
    spy_ret20 = spy.pct_change(20) * 100

    for i in range(20, len(common)):
        date = common[i]
        date_str = date.strftime("%Y-%m-%d")
        diff = float(spy_ret20.iloc[i] - tlt_ret20.iloc[i])  # SPY - TLT
        if pd.isna(diff):
            continue
        # -15% ~ +15% → 0~100
        score = max(0, min(100, (diff + 15) / 30 * 100))
        scores[date_str] = {
            "spy_ret20": round(float(spy_ret20.iloc[i]), 2),
            "tlt_ret20": round(float(tlt_ret20.iloc[i]), 2),
            "diff": round(diff, 2),
            "score": round(score, 1),
        }
    return scores


def calc_junkbond_score(hyg_series, lqd_series):
    """
    Junk Bond Spread: HYG/LQD 비율의 변화
    비율 상승 → 하이일드 선호 → 탐욕
    비율 하락 → 안전자산 선호 → 공포
    """
    scores = {}
    hyg = hyg_series.sort_index()
    lqd = lqd_series.sort_index()

    common = hyg.index.intersection(lqd.index)
    hyg = hyg.loc[common]
    lqd = lqd.loc[common]

    ratio = hyg / lqd
    ratio_ma20 = ratio.rolling(20).mean()

    for i in range(252, len(common)):
        date = common[i]
        date_str = date.strftime("%Y-%m-%d")
        current_ratio = float(ratio.iloc[i])
        ma = float(ratio_ma20.iloc[i])
        if pd.isna(ma) or ma == 0:
            continue

        # 1년 롤링 백분위
        window = ratio.iloc[i - 252:i + 1]
        percentile = (window < current_ratio).sum() / len(window) * 100
        scores[date_str] = {
            "ratio": round(current_ratio, 4),
            "ratio_ma20": round(ma, 4),
            "score": round(percentile, 1),
        }
    return scores


# ─── 종합 점수 계산 ────────────────────────────────────────────────────────────

def build_composite(vix, momentum, highlow, breadth, safe_haven, junkbond):
    """6개 지표의 평균으로 종합 센티먼트 점수 계산"""
    # 공통 날짜 찾기
    all_dates = set()
    for d in [vix, momentum, highlow, breadth, safe_haven, junkbond]:
        all_dates.update(d.keys())
    all_dates = sorted(all_dates)

    results = []
    for date in all_dates:
        scores = []
        indicators = {}

        if date in vix:
            scores.append(vix[date]["score"])
            indicators["vix"] = vix[date]
        if date in momentum:
            scores.append(momentum[date]["score"])
            indicators["momentum"] = momentum[date]
        if date in highlow:
            scores.append(highlow[date]["score"])
            indicators["highlow"] = highlow[date]
        if date in breadth:
            scores.append(breadth[date]["score"])
            indicators["breadth"] = breadth[date]
        if date in safe_haven:
            scores.append(safe_haven[date]["score"])
            indicators["safe_haven"] = safe_haven[date]
        if date in junkbond:
            scores.append(junkbond[date]["score"])
            indicators["junkbond"] = junkbond[date]

        # 최소 4개 지표 있어야 유효
        if len(scores) < 4:
            continue

        composite = round(sum(scores) / len(scores), 1)

        # 레이블
        if composite >= 80:
            label = "Extreme Greed"
        elif composite >= 60:
            label = "Greed"
        elif composite >= 40:
            label = "Neutral"
        elif composite >= 20:
            label = "Fear"
        else:
            label = "Extreme Fear"

        results.append({
            "date": date,
            "composite": composite,
            "label": label,
            "count": len(scores),
            "indicators": indicators,
        })

    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--full", action="store_true", help="전체 빌드 (10년치)")
    args = parser.parse_args()

    DATA_DIR.mkdir(exist_ok=True)

    print("=" * 55)
    print("  Market Sentiment Dashboard - 데이터 수집")
    print("=" * 55)

    # 기간 설정
    end_date = datetime.now().strftime("%Y-%m-%d")
    if args.full:
        start_date = (datetime.now() - timedelta(days=365 * 11)).strftime("%Y-%m-%d")
        print(f"\n🚀 전체 빌드: {start_date} → {end_date}")
    else:
        start_date = (datetime.now() - timedelta(days=800)).strftime("%Y-%m-%d")
        print(f"\n📡 업데이트 모드: {start_date} → {end_date}")

    # 1) 기존 52w highs/lows 데이터 로드
    print("\n[1/4] 52w 신고/신저 데이터 로드...")
    highs_lows = load_highs_lows()

    # 2) 추가 티커 다운로드 (VIX, SPY, TLT, HYG, LQD)
    print("\n[2/4] VIX/TLT/HYG/LQD/SPY 다운로드...")
    extra = download_batch(EXTRA_TICKERS, start_date, end_date, batch_size=10)
    print(f"  ✅ {len(extra)}개 티커 완료")

    for t in EXTRA_TICKERS:
        if t in extra:
            print(f"    {t}: {len(extra[t])}일")
        else:
            print(f"    {t}: ⚠ 데이터 없음")

    # 3) S&P 500 전종목 Close 다운로드 (200일 MA용)
    print(f"\n[3/4] S&P 500 종목 Close 다운로드 ({len(SP500_TICKERS)}개)...")
    all_close = download_batch(SP500_TICKERS, start_date, end_date, batch_size=50)
    print(f"  ✅ {len(all_close)}개 종목 완료")

    # extra 데이터도 합침
    all_close.update(extra)

    # 4) 각 지표 계산
    print("\n[4/4] 센티먼트 지표 계산 중...")

    vix_scores = {}
    if "^VIX" in extra:
        vix_scores = calc_vix_score(extra["^VIX"])
        print(f"  VIX 점수: {len(vix_scores)}일")

    momentum_scores = {}
    if "SPY" in extra:
        momentum_scores = calc_momentum_score(extra["SPY"])
        print(f"  모멘텀 점수: {len(momentum_scores)}일")

    highlow_scores = calc_highlow_score(highs_lows)
    print(f"  신고/신저 점수: {len(highlow_scores)}일")

    # 200일 MA용 날짜 리스트
    common_dates = sorted(set(highs_lows.keys()))
    breadth_scores = calc_breadth_score(all_close, common_dates)
    print(f"  200일선 위 비율: {len(breadth_scores)}일")

    safe_haven_scores = {}
    if "TLT" in extra and "SPY" in extra:
        safe_haven_scores = calc_safe_haven_score(extra["TLT"], extra["SPY"])
        print(f"  Safe Haven 점수: {len(safe_haven_scores)}일")

    junkbond_scores = {}
    if "HYG" in extra and "LQD" in extra:
        junkbond_scores = calc_junkbond_score(extra["HYG"], extra["LQD"])
        print(f"  정크본드 점수: {len(junkbond_scores)}일")

    # 종합 점수 계산
    print("\n📊 종합 센티먼트 지수 계산...")
    results = build_composite(
        vix_scores, momentum_scores, highlow_scores,
        breadth_scores, safe_haven_scores, junkbond_scores
    )
    print(f"  ✅ {len(results)}일 데이터 생성")

    # 저장
    output = {
        "last_updated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_indicators": 6,
        "indicator_names": {
            "vix": "VIX 변동성 지수",
            "momentum": "시장 모멘텀 (SPY vs 125MA)",
            "highlow": "52주 신고/신저 비율",
            "breadth": "200일선 위 종목 비율",
            "safe_haven": "Safe Haven 수요 (채권 vs 주식)",
            "junkbond": "정크본드 스프레드 (HYG/LQD)",
        },
        "data": results,
    }

    with open(SENTIMENT_FILE, "w") as f:
        json.dump(output, f)

    print(f"\n✅ 저장: {SENTIMENT_FILE} ({len(results)}일)")

    # 최신 데이터 프리뷰
    if results:
        last = results[-1]
        print(f"\n📊 최신 ({last['date']}):")
        print(f"   종합 점수: {last['composite']} — {last['label']}")
        for k, v in last.get("indicators", {}).items():
            print(f"   {k}: {v.get('score', '?')}")

    print("\n완료!")


if __name__ == "__main__":
    main()
