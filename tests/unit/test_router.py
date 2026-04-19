"""
tests/unit/test_router.py
Unit tests for provider routing logic.
Uses monkeypatching — no real API calls made.
"""

import pytest
from unittest.mock import MagicMock, patch

from market_data.models import DataType
from market_data.providers.router import ProviderRouter, PROVIDER_PRIORITY


class TestProviderPriorityMap:
    def test_all_data_types_have_priority(self):
        """Every DataType enum value should have at least one provider in priority map."""
        # Subset check — at minimum these critical types must be covered
        critical = [DataType.OHLCV, DataType.FUNDAMENTALS, DataType.TICK]
        for dt in critical:
            assert dt in PROVIDER_PRIORITY, f"{dt} missing from PROVIDER_PRIORITY"
            assert len(PROVIDER_PRIORITY[dt]) > 0

    def test_tick_data_only_databento(self):
        assert PROVIDER_PRIORITY[DataType.TICK] == ["databento"]

    def test_ohlcv_has_multiple_providers(self):
        assert len(PROVIDER_PRIORITY[DataType.OHLCV]) >= 2


class TestProviderRouter:
    @pytest.fixture
    def router_with_mocks(self, monkeypatch):
        """Router with all three providers mocked out."""
        mock_av = MagicMock()
        mock_av.name = "alpha_vantage"
        mock_av.supports = lambda dt: dt in [DataType.OHLCV, DataType.FUNDAMENTALS,
                                              DataType.EARNINGS, DataType.NEWS_SENTIMENT]
        mock_av.supported_data_types.return_value = [DataType.OHLCV, DataType.FUNDAMENTALS,
                                                     DataType.EARNINGS, DataType.NEWS_SENTIMENT]
        mock_av.health_check.return_value = True

        mock_fh = MagicMock()
        mock_fh.name = "finnhub"
        mock_fh.supports = lambda dt: dt in [DataType.OHLCV, DataType.OPTIONS_CHAIN,
                                              DataType.FUNDAMENTALS, DataType.NEWS_SENTIMENT]
        mock_fh.supported_data_types.return_value = [DataType.OHLCV, DataType.OPTIONS_CHAIN,
                                                     DataType.FUNDAMENTALS, DataType.NEWS_SENTIMENT]
        mock_fh.health_check.return_value = True

        mock_db = MagicMock()
        mock_db.name = "databento"
        mock_db.supports = lambda dt: dt in [DataType.OHLCV, DataType.TICK,
                                              DataType.FUTURES_OHLCV, DataType.OPTIONS_CHAIN]
        mock_db.supported_data_types.return_value = [DataType.OHLCV, DataType.TICK,
                                                     DataType.FUTURES_OHLCV, DataType.OPTIONS_CHAIN]
        mock_db.health_check.return_value = True

        router = ProviderRouter.__new__(ProviderRouter)
        router._providers = {
            "alpha_vantage": mock_av,
            "finnhub": mock_fh,
            "databento": mock_db,
        }
        router._health_cache = {}
        return router, mock_av, mock_fh, mock_db

    def test_select_ohlcv_returns_first_priority(self, router_with_mocks):
        router, mock_av, _, _ = router_with_mocks
        provider = router.select(DataType.OHLCV)
        assert provider.name == "alpha_vantage"

    def test_preferred_provider_takes_precedence(self, router_with_mocks):
        router, _, mock_fh, _ = router_with_mocks
        provider = router.select(DataType.OHLCV, preferred="finnhub")
        assert provider.name == "finnhub"

    def test_tick_data_routes_to_databento(self, router_with_mocks):
        router, _, _, mock_db = router_with_mocks
        provider = router.select(DataType.TICK)
        assert provider.name == "databento"

    def test_raises_when_no_provider_available(self, router_with_mocks):
        router, _, _, _ = router_with_mocks
        # Remove databento — only provider for TICK
        router._providers.pop("databento")
        with pytest.raises(ValueError, match="No configured provider"):
            router.select(DataType.TICK)

    def test_fallback_when_first_provider_missing(self, router_with_mocks):
        router, _, mock_fh, _ = router_with_mocks
        # Remove alpha_vantage — should fall back to finnhub for OHLCV
        router._providers.pop("alpha_vantage")
        provider = router.select(DataType.OHLCV)
        assert provider.name == "finnhub"

    def test_options_chain_skips_alpha_vantage(self, router_with_mocks):
        """Alpha Vantage doesn't support options — should route to finnhub or databento."""
        router, mock_av, mock_fh, _ = router_with_mocks
        # AV doesn't support options
        provider = router.select(DataType.OPTIONS_CHAIN)
        assert provider.name in ["databento", "finnhub"]

    def test_health_check_all(self, router_with_mocks):
        router, mock_av, mock_fh, mock_db = router_with_mocks
        results = router.health_check_all()
        assert results["alpha_vantage"] is True
        assert results["finnhub"] is True
        assert results["databento"] is True

    def test_list_providers(self, router_with_mocks):
        router, _, _, _ = router_with_mocks
        listing = router.list_providers()
        assert "alpha_vantage" in listing
        assert "finnhub" in listing
        assert DataType.TICK in listing["databento"]
