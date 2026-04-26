"""
market_data/providers/router.py
Provider router — selects the best provider for each data type request.
Implements fallback chains and health-aware routing.
"""

import logging
from typing import Optional

from market_data.config import settings
from market_data.models import DataType, Interval
from market_data.providers.base import BaseProvider
from market_data.providers.alpha_vantage import AlphaVantageProvider
from market_data.providers.finnhub import FinnhubProvider
from market_data.providers.databento import DatabentoProvider
from market_data.providers.tastytrade import TastyTradeProvider
from market_data.providers.yfinance_provider import YFinanceProvider

logger = logging.getLogger(__name__)

# Priority order per data type — first available and healthy wins
PROVIDER_PRIORITY: dict[DataType, list[str]] = {
    DataType.OHLCV:           ["yfinance", "alpha_vantage", "finnhub", "databento", "tastytrade"],
    DataType.OHLCV_INTRADAY:  ["databento", "finnhub", "alpha_vantage", "tastytrade"],
    DataType.OPTIONS_CHAIN:   ["tastytrade", "databento", "finnhub"],
    DataType.FUNDAMENTALS:    ["alpha_vantage", "finnhub"],
    DataType.NEWS_SENTIMENT:  ["finnhub", "alpha_vantage"],
    DataType.EARNINGS:        ["alpha_vantage", "finnhub"],
    DataType.DIVIDENDS:       ["alpha_vantage"],
    DataType.IV_RANK:         ["databento", "finnhub"],  # databento: OPRA B-S IV; finnhub: ATM chain IV
    DataType.TICK:            ["databento"],
    DataType.FUTURES_OHLCV:   ["databento"],
}


class ProviderRouter:
    """
    Routes data requests to the appropriate provider.
    Handles provider instantiation, health checks, and fallback.
    """

    def __init__(self):
        self._providers: dict[str, BaseProvider] = {}
        self._health_cache: dict[str, bool] = {}
        self._init_providers()

    def _init_providers(self):
        """Initialize providers that have API keys configured."""
        if settings.alpha_vantage_api_key:
            self._providers["alpha_vantage"] = AlphaVantageProvider()
            logger.info("Initialized Alpha Vantage provider")
        else:
            logger.warning("Alpha Vantage not configured — no API key")

        if settings.finnhub_api_key:
            self._providers["finnhub"] = FinnhubProvider()
            logger.info("Initialized Finnhub provider")
        else:
            logger.warning("Finnhub not configured — no API key")

        if settings.databento_api_key:
            self._providers["databento"] = DatabentoProvider()
            logger.info("Initialized Databento provider")
        else:
            logger.warning("Databento not configured — no API key")

        if settings.tastytrade_client_id and settings.tastytrade_refresh_token:
            self._providers["tastytrade"] = TastyTradeProvider()
            env = "sandbox" if settings.tastytrade_sandbox else "production"
            logger.info(f"Initialized TastyTrade provider ({env})")
        else:
            logger.warning("TastyTrade not configured — no client_id or refresh_token")

        # yfinance requires no API key — always available
        try:
            import yfinance  # noqa: F401
            self._providers["yfinance"] = YFinanceProvider()
            logger.info("Initialized yfinance provider")
        except ImportError:
            logger.warning("yfinance not installed — skipping")

    def select(
        self,
        data_type: DataType,
        preferred: Optional[str] = None,
    ) -> BaseProvider:
        """
        Select the best provider for a given data type.
        Falls through the priority list until a healthy, capable provider is found.

        Args:
            data_type: The type of data being requested
            preferred: Optionally override the default priority (e.g., "finnhub")

        Returns:
            A configured, healthy provider instance

        Raises:
            ValueError: If no capable provider is available
        """
        priority = self._build_priority(data_type, preferred)

        for name in priority:
            provider = self._providers.get(name)
            if provider is None:
                logger.debug(f"Provider '{name}' not initialized (no key?)")
                continue
            if not provider.supports(data_type):
                logger.debug(f"Provider '{name}' does not support {data_type}")
                continue
            # Skip providers known to be unhealthy from a prior health_check_all() call.
            # If the cache has no entry for this provider, assume healthy (benefit of doubt).
            if self._health_cache.get(name) is False:
                logger.warning(f"Provider '{name}' skipped — last health check failed")
                continue
            logger.debug(f"Selected provider '{name}' for {data_type}")
            return provider

        available = list(self._providers.keys())
        raise ValueError(
            f"No configured provider available for data type '{data_type}'. "
            f"Initialized providers: {available}. "
            f"Priority chain for {data_type}: {priority}. "
            f"Check that relevant API keys are set in your .env file."
        )

    def _build_priority(self, data_type: DataType, preferred: Optional[str]) -> list[str]:
        """Build the priority list, inserting the preferred provider first if set."""
        default_priority = PROVIDER_PRIORITY.get(data_type, [])
        if preferred and preferred in self._providers:
            # Move preferred to front without duplicating
            priority = [preferred] + [p for p in default_priority if p != preferred]
        else:
            priority = default_priority
        return priority

    def health_check_all(self) -> dict[str, bool]:
        """Run health checks on all initialized providers."""
        results = {}
        for name, provider in self._providers.items():
            results[name] = provider.health_check()
            self._health_cache[name] = results[name]
            status = "✓" if results[name] else "✗"
            logger.info(f"  Provider {name}: {status}")
        return results

    def list_providers(self) -> dict[str, list[DataType]]:
        """Return a map of provider name → supported data types."""
        return {
            name: provider.supported_data_types()
            for name, provider in self._providers.items()
        }

    def iter_for_type(self, data_type: DataType) -> list[BaseProvider]:
        """Return all configured, healthy providers for a data type in priority order."""
        priority = PROVIDER_PRIORITY.get(data_type, [])
        result = []
        for name in priority:
            provider = self._providers.get(name)
            if provider is None:
                continue
            if not provider.supports(data_type):
                continue
            if self._health_cache.get(name) is False:
                continue
            result.append(provider)
        return result

    def get(self, name: str) -> Optional[BaseProvider]:
        """Get a specific provider by name."""
        return self._providers.get(name)
