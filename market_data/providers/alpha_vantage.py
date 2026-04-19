"""
market_data/providers/alpha_vantage.py
Alpha Vantage adapter — OHLCV, fundamentals, earnings, news/sentiment.
Free tier: 25 calls/day. Premium: higher limits.
"""

import logging
from datetime import date, datetime, timezone
from typing import Optional

import httpx
import pandas as pd

from market_data.config import settings
from market_data.models import DataType, Interval
from market_data.providers.base import BaseProvider, RateLimitConfig

logger = logging.getLogger(__name__)

BASE_URL = "https://www.alphavantage.co/query"

# Alpha Vantage interval strings
AV_INTERVAL_MAP = {
    Interval.ONE_MIN:     "1min",
    Interval.FIVE_MIN:    "5min",
    Interval.FIFTEEN_MIN: "15min",
    Interval.ONE_HOUR:    "60min",
    Interval.ONE_DAY:     None,   # uses TIME_SERIES_DAILY
}


class AlphaVantageProvider(BaseProvider):
    name = "alpha_vantage"

    def __init__(self):
        self.api_key = settings.alpha_vantage_api_key
        if not self.api_key:
            logger.warning("ALPHA_VANTAGE_API_KEY not set — provider will fail on requests")
        super().__init__()

    def get_rate_limit_config(self) -> RateLimitConfig:
        return RateLimitConfig(
            calls_per_day=25,
            calls_per_minute=5,
            min_interval_seconds=12.0,  # conservative: 5 req/min = 12s apart
        )

    def supported_data_types(self) -> list[DataType]:
        return [
            DataType.OHLCV,
            DataType.OHLCV_INTRADAY,
            DataType.FUNDAMENTALS,
            DataType.EARNINGS,
            DataType.DIVIDENDS,
            DataType.NEWS_SENTIMENT,
        ]

    # ── OHLCV ──────────────────────────────────────────────────────────────

    def _fetch_ohlcv(self, symbol: str, start: date, end: date, interval: Interval) -> pd.DataFrame:
        if interval == Interval.ONE_DAY or interval == Interval.ONE_WEEK:
            return self._fetch_daily(symbol, start, end)
        else:
            return self._fetch_intraday(symbol, start, end, interval)

    def _fetch_daily(self, symbol: str, start: date, end: date) -> pd.DataFrame:
        # Use "full" when requesting more than ~100 trading days of history
        from datetime import date as date_type
        trading_days_approx = (end - start).days * 5 // 7
        outputsize = "full" if trading_days_approx > 100 else "compact"
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "outputsize": outputsize,
            "datatype": "json",
            "apikey": self.api_key,
        }
        data = self._get(params)
        ts = data.get("Time Series (Daily)", {})
        rows = []
        for dt_str, vals in ts.items():
            dt = datetime.strptime(dt_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            row_date = dt.date()
            if start <= row_date <= end:
                rows.append({
                    "timestamp": dt,
                    "open":      float(vals["1. open"]),
                    "high":      float(vals["2. high"]),
                    "low":       float(vals["3. low"]),
                    "close":     float(vals["4. close"]),
                    "volume":    int(vals["5. volume"]),
                    "adj_close": float(vals["4. close"]),  # unadjusted; no split data on free tier
                })
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        df = df.sort_values("timestamp").reset_index(drop=True)
        return self._enforce_ohlcv_schema(df, symbol)

    def _fetch_intraday(self, symbol: str, start: date, end: date, interval: Interval) -> pd.DataFrame:
        av_interval = AV_INTERVAL_MAP.get(interval, "5min")
        params = {
            "function":   "TIME_SERIES_INTRADAY",
            "symbol":     symbol,
            "interval":   av_interval,
            "outputsize": "full",
            "extended_hours": "false",
            "datatype": "json",
            "apikey": self.api_key,
        }
        data = self._get(params)
        key = f"Time Series ({av_interval})"
        ts = data.get(key, {})
        rows = []
        for dt_str, vals in ts.items():
            dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            if start <= dt.date() <= end:
                rows.append({
                    "timestamp": dt,
                    "open":   float(vals["1. open"]),
                    "high":   float(vals["2. high"]),
                    "low":    float(vals["3. low"]),
                    "close":  float(vals["4. close"]),
                    "volume": int(vals["5. volume"]),
                })
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        df = df.sort_values("timestamp").reset_index(drop=True)
        return self._enforce_ohlcv_schema(df, symbol)

    # ── Fundamentals ───────────────────────────────────────────────────────

    def _fetch_fundamentals(self, symbol: str) -> pd.DataFrame:
        params = {
            "function": "OVERVIEW",
            "symbol": symbol,
            "apikey": self.api_key,
        }
        data = self._get(params)
        if not data or "Symbol" not in data:
            return pd.DataFrame()

        today = date.today()
        row = {
            "snapshot_date":   today,
            "symbol":          symbol.upper(),
            "pe_ratio":        self._float(data.get("PERatio")),
            "eps":             self._float(data.get("EPS")),
            "revenue":         self._int(data.get("RevenueTTM")),
            "market_cap":      self._int(data.get("MarketCapitalization")),
            "debt_to_equity":  self._float(data.get("DebtToEquityRatio")),
            "roe":             self._float(data.get("ReturnOnEquityTTM")),
            "sector":          data.get("Sector"),
            "industry":        data.get("Industry"),
            "raw_data":        data,
            "provider":        self.name,
        }
        return pd.DataFrame([row])

    # ── Earnings ──────────────────────────────────────────────────────────

    def _fetch_earnings(self, symbol: str) -> pd.DataFrame:
        params = {
            "function": "EARNINGS",
            "symbol": symbol,
            "apikey": self.api_key,
        }
        data = self._get(params)
        quarterly = data.get("quarterlyEarnings", [])
        rows = []
        for item in quarterly:
            rows.append({
                "report_date":       self._date(item.get("reportedDate")),
                "symbol":            symbol.upper(),
                "eps_actual":        self._float(item.get("reportedEPS")),
                "eps_estimate":      self._float(item.get("estimatedEPS")),
                "eps_surprise":      self._float(item.get("surprise")),
                "fiscal_quarter":    item.get("fiscalDateEnding"),
                "provider":          self.name,
            })
        return pd.DataFrame(rows).dropna(subset=["report_date"])

    # ── News/Sentiment ─────────────────────────────────────────────────────

    def _fetch_news_sentiment(self, symbol: str, start: date, end: date) -> pd.DataFrame:
        params = {
            "function":   "NEWS_SENTIMENT",
            "tickers":    symbol,
            "time_from":  start.strftime("%Y%m%dT0000"),
            "time_to":    end.strftime("%Y%m%dT2359"),
            "limit":      200,
            "apikey":     self.api_key,
        }
        data = self._get(params)
        feed = data.get("feed", [])
        rows = []
        for item in feed:
            published = self._parse_av_datetime(item.get("time_published", ""))
            ticker_sentiment = next(
                (t for t in item.get("ticker_sentiment", []) if t["ticker"] == symbol.upper()),
                {}
            )
            rows.append({
                "published_at":     published,
                "symbol":           symbol.upper(),
                "headline":         item.get("title", ""),
                "source":           item.get("source", ""),
                "sentiment_score":  self._float(ticker_sentiment.get("ticker_sentiment_score")),
                "sentiment_label":  ticker_sentiment.get("ticker_sentiment_label"),
                "url":              item.get("url"),
                "provider":         self.name,
            })
        return pd.DataFrame(rows)

    # ── Dividends ──────────────────────────────────────────────────────────

    def _fetch_dividends(self, symbol: str) -> pd.DataFrame:
        params = {
            "function": "DIVIDENDS",
            "symbol": symbol,
            "apikey": self.api_key,
        }
        data = self._get(params)
        dividends = data.get("data", [])
        rows = []
        for item in dividends:
            rows.append({
                "ex_date":          self._date(item.get("ex_dividend_date")),
                "symbol":           symbol.upper(),
                "amount":           self._float(item.get("amount")) or 0.0,
                "pay_date":         self._date(item.get("payment_date")),
                "declaration_date": self._date(item.get("declaration_date")),
                "provider":         self.name,
            })
        return pd.DataFrame(rows).dropna(subset=["ex_date"])

    # ── Health Check ──────────────────────────────────────────────────────

    def _health_check(self) -> bool:
        params = {
            "function": "GLOBAL_QUOTE",
            "symbol": "IBM",
            "apikey": self.api_key,
        }
        data = self._get(params)
        return "Global Quote" in data

    # ── HTTP Utility ──────────────────────────────────────────────────────

    def _get(self, params: dict) -> dict:
        with httpx.Client(timeout=settings.request_timeout_seconds) as client:
            resp = client.get(BASE_URL, params=params)
            resp.raise_for_status()
            data = resp.json()

        if "Note" in data:
            raise RuntimeError(f"Alpha Vantage rate limit note: {data['Note']}")
        if "Information" in data and "rate limit" in data["Information"].lower():
            raise RuntimeError(f"Alpha Vantage API limit: {data['Information']}")
        return data

    # ── Type Helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _float(val) -> Optional[float]:
        try:
            v = float(val)
            return None if v != v else v   # NaN check
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _int(val) -> Optional[int]:
        try:
            return int(float(val))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _date(val: Optional[str]) -> Optional[date]:
        if not val:
            return None
        try:
            return datetime.strptime(val, "%Y-%m-%d").date()
        except ValueError:
            return None

    @staticmethod
    def _parse_av_datetime(val: str) -> Optional[datetime]:
        """Parse AV datetime format: 20240117T123000"""
        if not val:
            return None
        try:
            return datetime.strptime(val, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
        except ValueError:
            return None
