"""
Unit tests for LighterClient.

All tests are fully mocked — no network calls, no lighter-sdk import required.
The lighter-sdk is patched at the module level so these tests run in CI
without lighter-sdk installed.
"""

from __future__ import annotations

import json
import sys
import types
from unittest.mock import MagicMock, patch, call

import pytest


# ---------------------------------------------------------------------------
# Helpers to build a mock lighter SDK module tree
# ---------------------------------------------------------------------------

def _make_lighter_sdk_mock():
    """Return a minimal mock of the lighter package expected by LighterClient.__init__."""
    mod = types.ModuleType("lighter")

    class Configuration:
        def __init__(self, host=""):
            self.host = host

    class ApiClient:
        def __init__(self, configuration=None):
            self.configuration = configuration

    class OrderApi:
        def __init__(self, api_client=None):
            pass

    class AccountApi:
        def __init__(self, api_client=None):
            pass

    class TransactionApi:
        def __init__(self, api_client=None):
            pass

    class SignerClient:
        def __init__(self, private_key=""):
            self.private_key = private_key

        def create_auth_token_with_expiry(self, expiry_seconds=3600):
            return "mock-token"

        def sign_create_order(self, **kwargs):
            return {"signed": True, **kwargs}

        def sign_cancel_order(self, **kwargs):
            return {"signed": True, **kwargs}

    mod.Configuration = Configuration
    mod.ApiClient = ApiClient
    mod.OrderApi = OrderApi
    mod.AccountApi = AccountApi
    mod.TransactionApi = TransactionApi
    mod.SignerClient = SignerClient
    return mod


# Patch lighter into sys.modules before importing LighterClient
_lighter_mock = _make_lighter_sdk_mock()
sys.modules.setdefault("lighter", _lighter_mock)


from app.services.live_trading.lighter import LighterClient  # noqa: E402
from app.services.live_trading.base import LiveTradingError  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def client():
    c = LighterClient(
        private_key="0x" + "a" * 64,
        account_index=1,
        testnet=True,
    )
    return c


@pytest.fixture()
def client_with_markets(client):
    """Pre-populate the market cache so _resolve_market_id skips refresh."""
    import time
    with client._market_cache_lock:
        client._market_cache["BTC"] = (1, time.monotonic())
        client._market_cache["ETH"] = (2, time.monotonic())
    return client


# ---------------------------------------------------------------------------
# Symbol normalization
# ---------------------------------------------------------------------------

from app.services.live_trading.symbols import to_lighter_coin  # noqa: E402


@pytest.mark.parametrize("symbol,expected", [
    ("BTC/USDT", "BTC"),
    ("BTC/USDT:USDT", "BTC"),
    ("ETH-PERP", "ETH"),
    ("SOLUSDT", "SOL"),
    ("ETH/USD", "ETH"),
    ("BTC", "BTC"),
])
def test_to_lighter_coin(symbol, expected):
    assert to_lighter_coin(symbol) == expected


# ---------------------------------------------------------------------------
# Market cache and refresh
# ---------------------------------------------------------------------------

def test_refresh_markets_dict_response(client):
    mock_markets = [
        {"id": 1, "base_asset_symbol": "BTC"},
        {"id": 2, "base_asset_symbol": "ETH"},
    ]
    client.account_api.get_markets = MagicMock(return_value=mock_markets)
    client._refresh_markets()
    with client._market_cache_lock:
        assert client._market_cache["BTC"][0] == 1
        assert client._market_cache["ETH"][0] == 2


def test_refresh_markets_object_response(client):
    m1 = MagicMock()
    m1.id = 3
    m1.base_asset_symbol = "SOL"
    client.account_api.get_markets = MagicMock(return_value=[m1])
    client._refresh_markets()
    with client._market_cache_lock:
        assert client._market_cache["SOL"][0] == 3


def test_resolve_market_id_hits_cache(client_with_markets):
    client_with_markets.account_api.get_markets = MagicMock()
    mid = client_with_markets._resolve_market_id("BTC/USDT")
    assert mid == 1
    client_with_markets.account_api.get_markets.assert_not_called()


def test_resolve_market_id_unknown_raises(client):
    client.account_api.get_markets = MagicMock(return_value=[])
    with pytest.raises(LiveTradingError, match="unknown symbol"):
        client._resolve_market_id("XYZ/USDT")


# ---------------------------------------------------------------------------
# Status normalization
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw,expected", [
    ("filled", "filled"),
    ("fully_filled", "filled"),
    ("partially_filled", "partial"),
    ("canceled", "canceled"),
    ("cancelled", "canceled"),
    ("rejected", "canceled"),
    ("open", "open"),
    ("pending", "open"),
    ("processing", "open"),
])
def test_normalize_status(raw, expected):
    assert LighterClient._normalize_status(raw) == expected


# ---------------------------------------------------------------------------
# Fill snapshot parsing
# ---------------------------------------------------------------------------

def test_parse_fill_snapshot_dict():
    resp = {
        "status": "filled",
        "filled_quantity": "0.01",
        "avg_fill_price": "68000.5",
    }
    snap = LighterClient._parse_fill_snapshot(resp)
    assert snap["status"] == "filled"
    assert snap["filled"] == pytest.approx(0.01)
    assert snap["avg_price"] == pytest.approx(68000.5)
    assert snap["fee"] == 0.0
    assert snap["fee_ccy"] == "USDC"


def test_parse_fill_snapshot_object():
    obj = MagicMock()
    obj.status = "partially_filled"
    obj.filled_quantity = 0.005
    obj.filledQuantity = None
    obj.filled_size = None
    obj.avg_fill_price = 67800.0
    obj.avgFillPrice = None
    obj.average_price = None
    obj.price = None
    snap = LighterClient._parse_fill_snapshot(obj)
    assert snap["status"] == "partial"
    assert snap["filled"] == pytest.approx(0.005)
    assert snap["avg_price"] == pytest.approx(67800.0)


# ---------------------------------------------------------------------------
# Position parsing
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("row,expected_side,expected_qty", [
    ({"market_id": 1, "size": "0.02", "avg_price": "68500"}, "long", 0.02),
    ({"market_id": 1, "size": "-0.015", "avg_price": "3200"}, "short", 0.015),
    ({"market_id": 1, "size": "0.008", "side": "ask", "avg_price": "69000"}, "short", 0.008),
    ({"market_id": 1, "size": "0.003", "side": "bid", "avg_price": "70000"}, "long", 0.003),
    ({"market_id": 1, "size": "0.01", "side": "long"}, "long", 0.01),
    ({"market_id": 1, "size": "0.01", "side": "short"}, "short", 0.01),
])
def test_parse_position_dict(row, expected_side, expected_qty):
    parsed = LighterClient._parse_position(row)
    assert parsed["side"] == expected_side
    assert parsed["base_qty"] == pytest.approx(expected_qty)


def test_parse_position_from_fixture():
    """Verify Lighter position parsing using inline fixture data."""
    # Lighter uses its own net-position row format (market_id + size) which differs from
    # CEX row formats tested by test_exchange_position_contract_fixtures.py, so we test
    # Lighter position parsing directly here.
    cases = [
        {
            "id": "lighter_net_long_positive_size",
            "row": {"market_id": 1, "size": "0.02", "avg_price": "68500"},
            "expected": {"inferred_side": "long", "base_qty": 0.02},
        },
        {
            "id": "lighter_net_short_negative_size",
            "row": {"market_id": 2, "size": "-0.015", "avg_price": "3200"},
            "expected": {"inferred_side": "short", "base_qty": 0.015},
        },
        {
            "id": "lighter_net_short_side_field",
            "row": {"market_id": 1, "size": "0.008", "side": "ask", "avg_price": "69000"},
            "expected": {"inferred_side": "short", "base_qty": 0.008},
        },
    ]

    for case in cases:
        parsed = LighterClient._parse_position(case["row"])
        exp = case["expected"]
        assert parsed["side"] == exp["inferred_side"], f"{case['id']}: side mismatch"
        assert parsed["base_qty"] == pytest.approx(exp["base_qty"]), f"{case['id']}: qty mismatch"


# ---------------------------------------------------------------------------
# Order placement
# ---------------------------------------------------------------------------

def test_place_limit_order_buy(client_with_markets):
    signed_tx = {"signed": True}
    tx_resp = {"order_id": "42"}
    client_with_markets.signer.sign_create_order = MagicMock(return_value=signed_tx)
    client_with_markets.tx_api.send_tx = MagicMock(return_value=tx_resp)

    result = client_with_markets.place_limit_order(
        symbol="BTC/USDT",
        side="buy",
        qty=0.01,
        price=68000.0,
    )

    client_with_markets.signer.sign_create_order.assert_called_once()
    call_kwargs = client_with_markets.signer.sign_create_order.call_args.kwargs
    assert call_kwargs["market_id"] == 1
    assert call_kwargs["is_ask"] is False
    assert call_kwargs["price"] == 68000.0
    assert call_kwargs["amount"] == 0.01

    assert result.exchange_id == "lighter"
    assert result.exchange_order_id == "42"


def test_place_limit_order_sell_post_only(client_with_markets):
    client_with_markets.signer.sign_create_order = MagicMock(return_value={})
    client_with_markets.tx_api.send_tx = MagicMock(return_value={"id": "99"})

    client_with_markets.place_limit_order(
        symbol="ETH/USDT",
        side="sell",
        qty=0.5,
        price=3200.0,
        post_only=True,
    )

    kwargs = client_with_markets.signer.sign_create_order.call_args.kwargs
    assert kwargs["is_ask"] is True
    assert kwargs["market_id"] == 2
    assert kwargs["post_only"] is True


def test_place_market_order(client_with_markets):
    client_with_markets.signer.sign_create_order = MagicMock(return_value={})
    client_with_markets.tx_api.send_tx = MagicMock(return_value={"order_id": "77"})

    result = client_with_markets.place_market_order(
        symbol="BTC/USDT",
        side="sell",
        qty=0.02,
    )

    kwargs = client_with_markets.signer.sign_create_order.call_args.kwargs
    assert kwargs["order_type"] == "market"
    assert kwargs["is_ask"] is True
    assert kwargs["price"] == 0
    assert result.exchange_order_id == "77"


def test_place_order_sdk_error_wraps(client_with_markets):
    client_with_markets.signer.sign_create_order = MagicMock(side_effect=RuntimeError("boom"))
    with pytest.raises(LiveTradingError, match="place_limit_order failed"):
        client_with_markets.place_limit_order("BTC/USDT", "buy", 0.01, 68000.0)


# ---------------------------------------------------------------------------
# Fill polling
# ---------------------------------------------------------------------------

def test_wait_for_fill_immediate(client):
    client.order_api.get_order = MagicMock(return_value={
        "status": "filled",
        "filled_quantity": "0.01",
        "avg_fill_price": "68000",
    })
    snap = client.wait_for_fill("42", max_wait_sec=5.0)
    assert snap["status"] == "filled"
    assert snap["filled"] == pytest.approx(0.01)


def test_wait_for_fill_polls_until_filled(client):
    responses = [
        {"status": "open", "filled_quantity": "0", "avg_fill_price": "0"},
        {"status": "open", "filled_quantity": "0", "avg_fill_price": "0"},
        {"status": "filled", "filled_quantity": "0.01", "avg_fill_price": "68000"},
    ]
    client.order_api.get_order = MagicMock(side_effect=responses)

    with patch("app.services.live_trading.lighter.time") as mock_time:
        # monotonic() sequence: first call sets deadline, subsequent calls simulate elapsed time
        mock_time.monotonic.side_effect = [0.0, 0.0, 0.0, 1.0, 1.0, 2.0, 2.0, 10.0]
        mock_time.sleep = MagicMock()

        snap = client.wait_for_fill("42", max_wait_sec=30.0)

    assert snap["status"] == "filled"
    assert client.order_api.get_order.call_count == 3


def test_wait_for_fill_timeout_returns_last(client):
    client.order_api.get_order = MagicMock(return_value={
        "status": "open",
        "filled_quantity": "0",
        "avg_fill_price": "0",
    })

    with patch("app.services.live_trading.lighter.time") as mock_time:
        mock_time.monotonic.side_effect = [0.0, 0.0, 100.0]
        mock_time.sleep = MagicMock()

        snap = client.wait_for_fill("42", max_wait_sec=1.0)

    assert snap["status"] == "open"


# ---------------------------------------------------------------------------
# Cancel
# ---------------------------------------------------------------------------

def test_cancel_order(client):
    client.signer.sign_cancel_order = MagicMock(return_value={"signed": True})
    client.tx_api.send_tx = MagicMock(return_value={"ok": True})

    result = client.cancel_order("42")

    client.signer.sign_cancel_order.assert_called_once_with(
        client_order_index=1,
        order_id=42,
    )
    assert result == {"ok": True}


def test_cancel_order_error_returns_empty(client):
    client.signer.sign_cancel_order = MagicMock(side_effect=RuntimeError("oops"))
    result = client.cancel_order("99")
    assert result == {}


# ---------------------------------------------------------------------------
# Positions
# ---------------------------------------------------------------------------

def test_get_all_positions_empty(client):
    client.account_api.get_account_positions = MagicMock(return_value=[])
    assert client.get_all_positions() == []


def test_get_all_positions_filters_zero(client):
    rows = [
        {"market_id": 1, "size": "0.0", "avg_price": "68000"},
        {"market_id": 2, "size": "0.01", "avg_price": "3200"},
    ]
    client.account_api.get_account_positions = MagicMock(return_value=rows)
    positions = client.get_all_positions()
    assert len(positions) == 1
    assert positions[0]["market_id"] == 2


def test_get_all_positions_error_returns_empty(client):
    client.account_api.get_account_positions = MagicMock(side_effect=Exception("network"))
    assert client.get_all_positions() == []


def test_get_position_by_symbol(client_with_markets):
    rows = [{"market_id": 1, "size": "0.02", "avg_price": "68500"}]
    client_with_markets.account_api.get_account_positions = MagicMock(return_value=rows)
    pos = client_with_markets.get_position("BTC/USDT")
    assert pos["market_id"] == 1
    assert pos["base_qty"] == pytest.approx(0.02)


def test_get_position_not_found_returns_empty(client_with_markets):
    client_with_markets.account_api.get_account_positions = MagicMock(return_value=[])
    pos = client_with_markets.get_position("BTC/USDT")
    assert pos == {}


# ---------------------------------------------------------------------------
# Connect liveness check
# ---------------------------------------------------------------------------

def test_connect_success(client):
    client.account_api.get_account_positions = MagicMock(return_value=[])
    assert client.connect() is True


def test_connect_failure(client):
    client.account_api.get_account_positions = MagicMock(side_effect=Exception("timeout"))
    assert client.connect() is False


# ---------------------------------------------------------------------------
# safe_exchange_config_for_log redacts private_key
# ---------------------------------------------------------------------------

def test_safe_exchange_config_redacts_private_key():
    from app.services.exchange_execution import safe_exchange_config_for_log

    cfg = {
        "exchange_id": "lighter",
        "private_key": "0x" + "a" * 64,
        "account_index": 1,
    }
    safe = safe_exchange_config_for_log(cfg)
    assert "lighter" in safe["exchange_id"]
    assert safe["private_key"] != cfg["private_key"]
    assert "***" in safe["private_key"] or "..." in safe["private_key"]
    assert safe.get("account_index") == 1


def test_safe_exchange_config_redacts_wallet_private_key():
    from app.services.exchange_execution import safe_exchange_config_for_log

    cfg = {"walletPrivateKey": "0x" + "b" * 64}
    safe = safe_exchange_config_for_log(cfg)
    assert safe["walletPrivateKey"] != cfg["walletPrivateKey"]


# ---------------------------------------------------------------------------
# capabilities + broker_market_policy integration
# ---------------------------------------------------------------------------

def test_lighter_in_capabilities():
    from app.services.live_trading.capabilities import CRYPTO_VENUE_CAPABILITIES

    assert "lighter" in CRYPTO_VENUE_CAPABILITIES
    cap = CRYPTO_VENUE_CAPABILITIES["lighter"]
    assert cap.supports_swap is True
    assert cap.supports_spot is False


def test_lighter_in_broker_market_policy():
    from app.services.broker_market_policy import BROKER_MARKETS, allowed_market_types

    assert "lighter" in BROKER_MARKETS
    mts = allowed_market_types("lighter", "Crypto")
    assert "swap" in mts
    assert "spot" not in mts


# ---------------------------------------------------------------------------
# factory.py routing
# ---------------------------------------------------------------------------

def test_factory_creates_lighter_client():
    from app.services.live_trading import factory

    # Reset lazy cache so the test re-imports
    factory.LighterClient = None

    cfg = {
        "exchange_id": "lighter",
        "private_key": "0x" + "a" * 64,
        "account_index": "1",
        "testnet": True,
    }
    client = factory.create_client(cfg, market_type="swap")
    assert isinstance(client, LighterClient)
    assert client._testnet is True
    assert client.account_index == 1


def test_factory_lighter_missing_private_key_raises():
    from app.services.live_trading import factory
    from app.services.live_trading.base import LiveTradingError

    cfg = {"exchange_id": "lighter"}
    with pytest.raises(LiveTradingError, match="private_key"):
        factory.create_client(cfg)


# ---------------------------------------------------------------------------
# WebSocket fill listener
# ---------------------------------------------------------------------------

def test_on_account_update_resolves_filled_watcher(client):
    """WS callback resolves a registered watcher when order is filled."""
    from app.services.live_trading.lighter import _FillWatcher

    watcher = _FillWatcher()
    with client._fill_watchers_lock:
        client._fill_watchers["42"] = watcher

    msg = {
        "type": "update/account_all",
        "orders": [
            {"id": "42", "status": "filled", "filled_quantity": "0.01", "avg_fill_price": "68000"}
        ],
    }
    client._on_account_update("1", msg)

    assert watcher.event.is_set()
    assert watcher.result["status"] == "filled"
    assert watcher.result["filled"] == pytest.approx(0.01)
    # Watcher removed after resolution
    with client._fill_watchers_lock:
        assert "42" not in client._fill_watchers


def test_on_account_update_resolves_canceled_watcher(client):
    from app.services.live_trading.lighter import _FillWatcher

    watcher = _FillWatcher()
    with client._fill_watchers_lock:
        client._fill_watchers["99"] = watcher

    msg = {"orders": [{"id": "99", "status": "canceled", "filled_quantity": "0", "avg_fill_price": "0"}]}
    client._on_account_update("1", msg)

    assert watcher.event.is_set()
    assert watcher.result["status"] == "canceled"


def test_on_account_update_ignores_open_orders(client):
    from app.services.live_trading.lighter import _FillWatcher

    watcher = _FillWatcher()
    with client._fill_watchers_lock:
        client._fill_watchers["77"] = watcher

    msg = {"orders": [{"id": "77", "status": "open", "filled_quantity": "0", "avg_fill_price": "0"}]}
    client._on_account_update("1", msg)

    assert not watcher.event.is_set()
    with client._fill_watchers_lock:
        assert "77" in client._fill_watchers


def test_on_account_update_sets_ws_ready(client):
    client._ws_ready.clear()
    client._on_account_update("1", {"orders": []})
    assert client._ws_ready.is_set()


def test_on_account_update_ignores_unwatched_orders(client):
    """No error when the message contains orders we are not watching."""
    msg = {"orders": [{"id": "111", "status": "filled", "filled_quantity": "1", "avg_fill_price": "100"}]}
    client._on_account_update("1", msg)  # no exception


def test_wait_for_fill_ws_path(client_with_markets):
    """wait_for_fill returns immediately when WS resolves the watcher."""
    from app.services.live_trading.lighter import _FillWatcher

    fill_result = {
        "filled": 0.01, "avg_price": 68000.0,
        "status": "filled", "fee": 0.0, "fee_ccy": "USDC",
    }

    def _fake_start():
        # Simulate WS becoming ready and delivering a fill
        client_with_markets._ws_ready.set()
        with client_with_markets._fill_watchers_lock:
            watchers = dict(client_with_markets._fill_watchers)
        for oid, w in watchers.items():
            w.result = fill_result
            w.event.set()

    client_with_markets._start_ws_if_needed = _fake_start

    snap = client_with_markets.wait_for_fill("42", max_wait_sec=5.0)
    assert snap["status"] == "filled"
    assert snap["filled"] == pytest.approx(0.01)


def test_wait_for_fill_falls_back_to_rest_when_ws_not_ready(client):
    """wait_for_fill uses REST when WS never becomes ready."""
    client._start_ws_if_needed = MagicMock()  # don't actually start WS
    # _ws_ready never gets set → falls back to REST immediately

    client.order_api.get_order = MagicMock(return_value={
        "status": "filled",
        "filled_quantity": "0.02",
        "avg_fill_price": "67500",
    })

    snap = client.wait_for_fill("55", max_wait_sec=3.0)
    assert snap["status"] == "filled"
    assert snap["filled"] == pytest.approx(0.02)
    client.order_api.get_order.assert_called()


def test_wait_for_fill_empty_order_id_returns_empty(client):
    snap = client.wait_for_fill("", max_wait_sec=1.0)
    assert snap["status"] == "open"
    assert snap["filled"] == 0.0


def test_stop_ws_sets_stop_event(client):
    client.stop_ws()
    assert client._ws_stop.is_set()
    assert client._ws_ready.is_set()  # unblocks any waiting threads
