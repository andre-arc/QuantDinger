"""
Lighter DEX (zk-rollup L2) live-trading client.

Authentication:
    Wallet private key signing via lighter-sdk's SignerClient.
    Auth tokens are created per-session with a configurable expiry.

Key differences from CEX clients:
    - No HMAC API key/secret; authentication is ECDSA wallet signing.
    - Net position mode only — no pos_side (hedge mode) support.
    - Symbols are integer market IDs; a refreshable cache maps base asset -> market_id.
    - Zero maker/taker fees for retail accounts.
    - exchange_order_id = str(client_order_index), a small integer WE assign at
      creation. cancel_order(order_index=N) references this same integer. tx_hash
      from RespSendTx is stored in LiveOrderResult.raw["tx_hash"] for display only.

WebSocket fill listener:
    A daemon thread runs WsClient subscribed to account_all/{account_index}.
    wait_for_fill() registers a threading.Event; the callback resolves it
    instantly when the target order reaches a terminal state (filled/canceled).
    Falls back to REST polling if the WebSocket is not yet connected or misses
    the event within the deadline.

Testnet: https://testnet.zklighter.elliot.ai
Mainnet: https://mainnet.zklighter.elliot.ai
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from dataclasses import dataclass, field
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple

from app.services.live_trading.base import LiveOrderResult, LiveTradingError
from app.services.live_trading.symbols import to_lighter_coin

logger = logging.getLogger(__name__)

# ── Persistent event loop for lighter async SDK ───────────────────────────────
# The lighter SDK uses aiohttp internally, which requires asyncio.get_running_loop()
# during construction and for all REST calls. Flask threads have no running loop,
# so we spin a single daemon thread that runs an event loop forever and submit
# all lighter SDK work to it via asyncio.run_coroutine_threadsafe().

_lighter_loop: Optional[asyncio.AbstractEventLoop] = None
_lighter_loop_lock = threading.Lock()


def _get_lighter_loop() -> asyncio.AbstractEventLoop:
    """Return the module-level running event loop, creating it if needed."""
    global _lighter_loop
    with _lighter_loop_lock:
        if _lighter_loop is None or _lighter_loop.is_closed():
            loop = asyncio.new_event_loop()
            t = threading.Thread(
                target=loop.run_forever,
                name="lighter-asyncio",
                daemon=True,
            )
            t.start()
            _lighter_loop = loop
    return _lighter_loop


def _lighter_run(coro, timeout: float = 30.0):
    """Await *coro* on the lighter event loop, blocking the calling thread."""
    loop = _get_lighter_loop()
    return asyncio.run_coroutine_threadsafe(coro, loop).result(timeout=timeout)


def _lighter_call(fn, *args, timeout: float = 10.0, **kwargs):
    """
    Call sync *fn* inside the lighter event loop thread where
    asyncio.get_running_loop() works. Required for constructors
    that touch aiohttp.TCPConnector during __init__.
    """
    async def _wrapper():
        return fn(*args, **kwargs)
    return _lighter_run(_wrapper(), timeout=timeout)

_TESTNET_HOST = "https://testnet.zklighter.elliot.ai"
_MAINNET_HOST = "https://mainnet.zklighter.elliot.ai"

# WebSocket reconnect delays
_WS_RECONNECT_BASE_SEC = 2.0
_WS_RECONNECT_MAX_SEC = 30.0

# How long to wait for the WS connection to be ready before falling back to REST
_WS_READY_TIMEOUT_SEC = 5.0


@dataclass
class _FillWatcher:
    """Tracks one pending order waiting for a terminal WebSocket event."""
    event: threading.Event = field(default_factory=threading.Event)
    result: Optional[Dict[str, Any]] = None


class LighterClient:
    """
    Live-trading client for Lighter DEX perpetuals.

    Does not inherit BaseRestClient — Lighter uses SDK-level signing,
    not direct HMAC-authenticated HTTP calls.
    """

    exchange_id = "lighter"

    def __init__(self, private_key: str, account_index: int = 1, api_key_index: int = 255, *, testnet: bool = False):
        try:
            from lighter import (  # type: ignore[import]
                SignerClient,
                AccountApi,
                OrderApi,
                TransactionApi,
                ApiClient,
                Configuration,
            )
        except ImportError:
            raise LiveTradingError(
                "Lighter DEX requires lighter-sdk>=1.1.0. Run: pip install 'lighter-sdk>=1.1.0'"
            )

        pk = str(private_key or "").strip()
        if not pk:
            raise LiveTradingError("LighterClient requires a non-empty wallet private_key")

        host = _TESTNET_HOST if testnet else _MAINNET_HOST
        self._testnet = testnet
        self._host = host
        self.account_index = int(account_index or 1)
        self.api_key_index = int(api_key_index or 255)

        # lighter-sdk uses aiohttp which calls asyncio.get_running_loop() during
        # __init__. All SDK objects must be constructed inside the running loop.
        self.signer = _lighter_call(
            SignerClient, host, self.account_index, {self.api_key_index: pk}
        )
        _cfg = Configuration(host=host)
        self._api_client = _lighter_call(ApiClient, configuration=_cfg)
        self.order_api = OrderApi(self._api_client)
        self.account_api = AccountApi(self._api_client)
        self.tx_api = TransactionApi(self._api_client)

        # coin (e.g. "BTC") -> (market_id, size_decimals, price_decimals, fetched_at_monotonic)
        self._market_cache: Dict[str, Tuple[int, int, int, float]] = {}
        self._market_cache_lock = Lock()
        self._market_cache_ttl = 300.0

        # WebSocket fill-listener state
        self._ws_thread: Optional[threading.Thread] = None
        self._ws_thread_lock = Lock()
        self._ws_ready = threading.Event()   # set once the WS is subscribed
        self._ws_stop = threading.Event()    # set to signal the thread to exit
        self._fill_watchers: Dict[str, _FillWatcher] = {}
        self._fill_watchers_lock = Lock()

        # Monotonic counter for client_order_index — the integer Lighter uses to
        # identify orders for cancel/modify. Each placed order gets a unique value.
        # Lighter cancel_order(order_index=N) references THIS index, not a tx_hash.
        self._coi_counter = 0
        self._coi_lock = Lock()

    # ── client_order_index counter ────────────────────────────────────────────

    def _next_coi(self) -> int:
        """Return a unique client_order_index for one order placement."""
        with self._coi_lock:
            self._coi_counter = (self._coi_counter + 1) % 100_000
            return self._coi_counter

    # ── Auth ──────────────────────────────────────────────────────────────────

    def _auth_header(self) -> str:
        # deadline=-1 → SDK default of 10 minutes.
        # Lighter's REST endpoints expect the raw token string with NO "Bearer " prefix.
        token, err = self.signer.create_auth_token_with_expiry(
            deadline=-1, api_key_index=self.api_key_index
        )
        if err:
            raise LiveTradingError(f"Lighter auth token error: {err}")
        return token

    # ── Symbol / market-ID resolution ─────────────────────────────────────────

    def _refresh_markets(self) -> None:
        """Populate the coin->market_id cache from order_api.order_books()."""
        try:
            resp = _lighter_run(self.order_api.order_books())
        except Exception as e:
            raise LiveTradingError(f"Lighter: failed to fetch market list: {e}") from e

        now = time.monotonic()
        # resp.order_books is a list of OrderBook objects with .symbol and .market_id
        books = getattr(resp, "order_books", None) or []
        with self._market_cache_lock:
            for m in books:
                if isinstance(m, dict):
                    mid = int(m.get("market_id") or 0)
                    coin = str(m.get("symbol") or "").strip().upper()
                    size_dec = int(m.get("supported_size_decimals") or 0)
                    price_dec = int(m.get("supported_price_decimals") or 0)
                else:
                    mid = int(getattr(m, "market_id", 0) or 0)
                    coin = str(getattr(m, "symbol", "") or "").strip().upper()
                    size_dec = int(getattr(m, "supported_size_decimals", 0) or 0)
                    price_dec = int(getattr(m, "supported_price_decimals", 0) or 0)
                if coin and mid:
                    self._market_cache[coin] = (mid, size_dec, price_dec, now)

    def _resolve_market(self, symbol: str) -> Tuple[int, int, int]:
        """Return (market_id, size_decimals, price_decimals) for *symbol*."""
        coin = to_lighter_coin(symbol)
        with self._market_cache_lock:
            entry = self._market_cache.get(coin)
            if entry and (time.monotonic() - entry[3]) < self._market_cache_ttl:
                return entry[0], entry[1], entry[2]
        self._refresh_markets()
        with self._market_cache_lock:
            entry = self._market_cache.get(coin)
        if not entry:
            raise LiveTradingError(
                f"Lighter: unknown symbol '{symbol}' (resolved coin='{coin}'). "
                "Verify the market exists on Lighter for this network."
            )
        return entry[0], entry[1], entry[2]

    def _resolve_market_id(self, symbol: str) -> int:
        return self._resolve_market(symbol)[0]

    def _market_id_to_symbol(self, market_id: int) -> str:
        """Reverse-lookup: market_id → base-coin string (e.g. 1 → 'BTC')."""
        with self._market_cache_lock:
            for coin, (mid, *_) in self._market_cache.items():
                if mid == market_id:
                    return coin
        # Cache may be empty — refresh once and retry
        try:
            self._refresh_markets()
        except Exception:
            pass
        with self._market_cache_lock:
            for coin, (mid, *_) in self._market_cache.items():
                if mid == market_id:
                    return coin
        return str(market_id)

    @staticmethod
    def _to_base_amount(qty: float, size_decimals: int) -> int:
        """Convert a decimal quantity to integer base units (lots)."""
        return int(round(qty * (10 ** size_decimals)))

    @staticmethod
    def _to_price_units(price: float, price_decimals: int) -> int:
        """Convert a decimal price to integer price units."""
        return int(round(price * (10 ** price_decimals)))

    # ── Response parsing ──────────────────────────────────────────────────────

    @staticmethod
    def _normalize_status(raw: Any) -> str:
        s = str(raw or "").lower().strip()
        if s in ("filled", "fully_filled", "completely_filled"):
            return "filled"
        if s in ("partial", "partially_filled"):
            return "partial"
        if s in ("canceled", "cancelled", "rejected", "expired"):
            return "canceled"
        if s in ("open", "processing", "pending", "new", "active"):
            return "open"
        return s

    @staticmethod
    def _parse_fill_snapshot(order_data: Any) -> Dict[str, Any]:
        """Produce a normalized fill snapshot compatible with apply_fill_snapshot().

        Order model fields (from Lighter SDK docs):
          filled_base_amount  — filled qty in base currency (str)
          filled_quote_amount — filled notional in quote currency (str)
          status              — "open" | "filled" | "canceled" etc.
        avg_price = filled_quote_amount / filled_base_amount when both > 0.
        """
        if isinstance(order_data, dict):
            raw_status = order_data.get("status") or order_data.get("order_status") or ""
            filled_base = float(order_data.get("filled_base_amount") or 0.0)
            filled_quote = float(order_data.get("filled_quote_amount") or 0.0)
        else:
            raw_status = (
                getattr(order_data, "status", None)
                or getattr(order_data, "order_status", "")
                or ""
            )
            filled_base = float(getattr(order_data, "filled_base_amount", 0) or 0.0)
            filled_quote = float(getattr(order_data, "filled_quote_amount", 0) or 0.0)

        avg_price = (filled_quote / filled_base) if filled_base > 0 else 0.0

        return {
            "filled": filled_base,
            "avg_price": avg_price,
            "status": LighterClient._normalize_status(raw_status),
            "fee": 0.0,  # Lighter has zero maker/taker fees for retail
            "fee_ccy": "USDC",
        }

    @staticmethod
    def _extract_order_id(resp: Any) -> str:
        if isinstance(resp, dict):
            return str(
                resp.get("order_id")
                or resp.get("id")
                or resp.get("orderId")
                or resp.get("tx_hash")
                or ""
            )
        return str(
            getattr(resp, "order_id", None)
            or getattr(resp, "id", None)
            or getattr(resp, "orderId", None)
            or getattr(resp, "tx_hash", None)
            or ""
        )

    # ── WebSocket fill listener ────────────────────────────────────────────────

    def _on_account_update(self, account_id: Any, message: Any) -> None:
        """
        Called by WsClient on every account_all update.

        Parses orders in the message and resolves any registered fill watchers
        whose order has reached a terminal state (filled or canceled).
        """
        # Mark WS as ready on the first callback (subscribed/account_all fires first)
        self._ws_ready.set()

        # Extract orders list from the message
        if isinstance(message, dict):
            orders = message.get("orders") or []
        else:
            orders = getattr(message, "orders", None) or []

        if not orders:
            return

        with self._fill_watchers_lock:
            if not self._fill_watchers:
                return
            watched = dict(self._fill_watchers)

        for order in orders:
            # Match on client_order_index — same key used by cancel/modify.
            if isinstance(order, dict):
                oid = str(order.get("client_order_index", "") or "")
            else:
                oid = str(getattr(order, "client_order_index", "") or "")

            if not oid or oid not in watched:
                continue

            snap = self._parse_fill_snapshot(order)
            if snap["status"] in ("filled", "canceled"):
                watcher = watched[oid]
                watcher.result = snap
                watcher.event.set()
                with self._fill_watchers_lock:
                    self._fill_watchers.pop(oid, None)

    def _ws_loop(self) -> None:
        """
        Daemon thread body: runs WsClient with automatic reconnection.

        Exponential backoff on disconnect: 2s → 4s → 8s … capped at 30s.
        Stops cleanly when _ws_stop is set.
        """
        try:
            from lighter import WsClient  # type: ignore[import]
        except ImportError:
            logger.warning("Lighter WsClient not available; WebSocket fill listener disabled")
            return

        delay = _WS_RECONNECT_BASE_SEC
        ws_host = self._host.replace("https://", "")

        while not self._ws_stop.is_set():
            self._ws_ready.clear()
            try:
                ws = WsClient(
                    host=ws_host,
                    account_ids=[self.account_index],
                    on_account_update=self._on_account_update,
                    # Order book not needed for fill tracking; use a dummy empty list
                    # WsClient requires at least one subscription so we pass account only.
                )
                logger.debug(
                    "Lighter WS connecting to %s (account %s)", ws_host, self.account_index
                )
                ws.run()  # blocks until disconnect or exception
            except Exception as e:
                if self._ws_stop.is_set():
                    break
                logger.warning(
                    "Lighter WS disconnected (account %s): %s — reconnecting in %.0fs",
                    self.account_index, e, delay,
                )
                # Wake any stalled watchers so they fall back to REST immediately
                self._ws_ready.clear()
                time.sleep(delay)
                delay = min(delay * 2, _WS_RECONNECT_MAX_SEC)
            else:
                # Clean exit — reset backoff
                delay = _WS_RECONNECT_BASE_SEC

    def _start_ws_if_needed(self) -> None:
        """Start the WebSocket daemon thread if not already running."""
        with self._ws_thread_lock:
            if self._ws_thread is not None and self._ws_thread.is_alive():
                return
            self._ws_stop.clear()
            t = threading.Thread(target=self._ws_loop, daemon=True, name="lighter-ws")
            self._ws_thread = t
            t.start()

    def stop_ws(self) -> None:
        """Signal the WebSocket thread to stop (useful in tests)."""
        self._ws_stop.set()
        self._ws_ready.set()  # unblock any waiters

    def close(self) -> None:
        """Close all aiohttp sessions: the shared api_client and the signer's own api_client."""
        self.stop_ws()
        try:
            _lighter_run(self._api_client.close())
        except Exception:
            pass
        try:
            _lighter_run(self.signer.close())
        except Exception:
            pass

    def __del__(self) -> None:
        # Fire-and-forget: don't block the GC thread waiting for the coroutines.
        try:
            loop = _get_lighter_loop()
            if loop and not loop.is_closed():
                asyncio.run_coroutine_threadsafe(self._api_client.close(), loop)
                asyncio.run_coroutine_threadsafe(self.signer.close(), loop)
        except Exception:
            pass

    # ── Order placement ────────────────────────────────────────────────────────

    def _fetch_ideal_price(
        self, market_id: int, price_dec: int, is_ask: bool, ref_price: float = 0.0
    ) -> int:
        """Return ideal_price as a Lighter integer (price_units) for a market order.

        Tries in order:
          1. Best ask (for buys) / best bid (for sells) from the live order book
          2. Last traded price from recent_trades
          3. ref_price from the calling strategy signal

        Returns 0 if no price can be determined.
        """
        # 1. Order book — SDK get_best_price logic without crashing on empty sides
        try:
            ob = _lighter_run(self.order_api.order_book_orders(market_id, 1))
            asks = getattr(ob, "asks", None) or []
            bids = getattr(ob, "bids", None) or []
            # For a buy (is_ask=False) we need the best ask; for sell, best bid.
            side_levels = asks if not is_ask else bids
            if side_levels:
                price_str = getattr(side_levels[0], "price", None)
                if price_str:
                    return int(str(price_str).replace(".", ""))
        except Exception:
            pass

        # 2. Recent trades last price
        try:
            resp = _lighter_run(self.order_api.recent_trades(market_id=market_id, limit=1))
            trades = getattr(resp, "trades", None) or []
            if trades:
                raw = getattr(trades[0], "price", None) or getattr(trades[0], "trade_price", None)
                if raw:
                    return int(str(raw).replace(".", ""))
        except Exception:
            pass

        # 3. ref_price from the signal (convert float → price_units)
        if ref_price and ref_price > 0:
            return self._to_price_units(ref_price, price_dec)

        return 0

    def place_limit_order(
        self,
        symbol: str,
        side: str,
        qty: float,
        price: float,
        *,
        reduce_only: bool = False,
        post_only: bool = False,
        client_order_id: Optional[str] = None,
    ) -> LiveOrderResult:
        market_id, size_dec, price_dec = self._resolve_market(symbol)
        is_ask = str(side or "").strip().lower() == "sell"
        base_amount = self._to_base_amount(qty, size_dec)
        price_units = self._to_price_units(price, price_dec)
        tif = (self.signer.ORDER_TIME_IN_FORCE_POST_ONLY if post_only
               else self.signer.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME)
        coi = self._next_coi()
        try:
            _, resp, err = _lighter_run(
                self.signer.create_order(
                    market_id,
                    coi,
                    base_amount,
                    price_units,
                    is_ask,
                    self.signer.ORDER_TYPE_LIMIT,
                    tif,
                    reduce_only,
                    api_key_index=self.api_key_index,
                )
            )
            if err:
                raise LiveTradingError(f"Lighter place_limit_order error: {err}")
        except LiveTradingError:
            raise
        except Exception as e:
            raise LiveTradingError(f"Lighter place_limit_order failed ({symbol}): {e}") from e

        # SDK returns (tx, tx_hash, err) — resp is already the tx_hash string.
        tx_hash = resp if isinstance(resp, str) else ""
        return LiveOrderResult(
            exchange_id="lighter",
            exchange_order_id=str(coi),
            filled=0.0,
            avg_price=0.0,
            raw={"tx_hash": tx_hash, "client_order_index": coi},
        )

    def place_market_order(
        self,
        symbol: str,
        side: str,
        qty: float,
        *,
        ref_price: float = 0.0,
        reduce_only: bool = False,
        client_order_id: Optional[str] = None,
    ) -> LiveOrderResult:
        market_id, size_dec, price_dec = self._resolve_market(symbol)
        is_ask = str(side or "").strip().lower() == "sell"
        base_amount = self._to_base_amount(qty, size_dec)
        coi = self._next_coi()

        # Pre-fetch the ideal price so we can pass it explicitly to
        # create_market_order_limited_slippage. The SDK's internal get_best_price
        # crashes with IndexError when one side of the book is empty (common on
        # testnet). By providing ideal_price ourselves we bypass that call.
        ideal_price = self._fetch_ideal_price(market_id, price_dec, is_ask, ref_price)
        if not ideal_price:
            raise LiveTradingError(
                f"Lighter place_market_order ({symbol}): cannot determine market price "
                "(empty order book, no recent trades, and no ref_price)"
            )

        try:
            _, resp, err = _lighter_run(
                self.signer.create_market_order_limited_slippage(
                    market_id,
                    coi,
                    base_amount,
                    0.05,       # max_slippage: 5%
                    is_ask,
                    reduce_only,
                    ideal_price=ideal_price,
                    api_key_index=self.api_key_index,
                )
            )
            if err:
                raise LiveTradingError(f"Lighter market order error: {err}")
        except LiveTradingError:
            raise
        except Exception as e:
            raise LiveTradingError(f"Lighter place_market_order failed ({symbol}): {e}") from e

        # SDK returns (tx, tx_hash, err) — resp is already the tx_hash string.
        tx_hash = resp if isinstance(resp, str) else ""
        return LiveOrderResult(
            exchange_id="lighter",
            exchange_order_id=str(coi),
            filled=0.0,
            avg_price=0.0,
            raw={"tx_hash": tx_hash, "client_order_index": coi},
        )

    # ── Fill detection (WS-first, REST fallback) ──────────────────────────────

    def wait_for_fill(
        self,
        order_id: str,
        *,
        max_wait_sec: float = 15.0,
    ) -> Dict[str, Any]:
        """
        Wait for order_id to reach a terminal state (filled or canceled).

        Strategy:
        1. Start the WebSocket daemon thread if not running.
        2. Register a _FillWatcher for this order_id.
        3. Wait up to _WS_READY_TIMEOUT_SEC for the WS to become subscribed.
        4. If the WS resolves the watcher in time, return its result.
        5. Otherwise fall back to REST polling for the remaining deadline.
        """
        _empty = {
            "filled": 0.0,
            "avg_price": 0.0,
            "status": "open",
            "fee": 0.0,
            "fee_ccy": "USDC",
        }

        if not order_id:
            return _empty

        max_wait = float(max_wait_sec or 15.0)
        deadline = time.monotonic() + max_wait

        # Register watcher before starting the WS so we never miss an event
        watcher = _FillWatcher()
        with self._fill_watchers_lock:
            self._fill_watchers[order_id] = watcher

        self._start_ws_if_needed()

        # Give WS time to subscribe (but not longer than the total deadline)
        ws_wait = min(_WS_READY_TIMEOUT_SEC, max_wait * 0.3)
        ws_is_ready = self._ws_ready.wait(timeout=ws_wait)

        if ws_is_ready:
            # Wait on the WebSocket event for the remaining deadline
            remaining = max(0.0, deadline - time.monotonic())
            got_fill = watcher.event.wait(timeout=remaining)
            if got_fill and watcher.result is not None:
                return watcher.result

        # WS not ready or missed the event — clean up watcher and fall back to REST
        with self._fill_watchers_lock:
            self._fill_watchers.pop(order_id, None)

        return self._poll_fill_rest(order_id, deadline=deadline)

    def _poll_fill_rest(self, order_id: str, *, deadline: float) -> Dict[str, Any]:
        """REST polling fallback used when WebSocket is unavailable or timed out."""
        poll_interval = 1.5
        result: Dict[str, Any] = {
            "filled": 0.0,
            "avg_price": 0.0,
            "status": "open",
            "fee": 0.0,
            "fee_ccy": "USDC",
        }

        # Lighter has no single-order endpoint; scan inactive orders to detect fills.
        while True:
            try:
                auth = self._auth_header()
                resp = _lighter_run(
                    self.order_api.account_inactive_orders(
                        authorization=auth,
                        account_index=self.account_index,
                        limit=20,
                    )
                )
                orders = getattr(resp, "orders", None) or []
                for o in orders:
                    # order_id is str(client_order_index) — match Order.client_order_index.
                    # Lighter cancel/modify also uses client_order_index as the order key.
                    coi = (
                        str(o.get("client_order_index", ""))
                        if isinstance(o, dict)
                        else str(getattr(o, "client_order_index", "") or "")
                    )
                    if coi == order_id:
                        result = self._parse_fill_snapshot(o)
                        break
            except Exception as e:
                logger.warning("Lighter REST fill poll error (order_id=%s): %s", order_id, e)

            if result["status"] in ("filled", "canceled"):
                return result
            if time.monotonic() >= deadline:
                return result
            time.sleep(poll_interval)

    # ── Cancel ────────────────────────────────────────────────────────────────

    def cancel_order(self, order_id: str, market_index: int = 0) -> Dict[str, Any]:
        # Lighter cancel uses client_order_index — the integer WE assigned at creation
        # (now stored as exchange_order_id). Fails gracefully if a legacy tx_hash
        # string is passed (non-numeric).
        try:
            order_index = int(order_id)
        except (ValueError, TypeError):
            logger.warning(
                "Lighter cancel_order: order_id '%s' is not a numeric client_order_index "
                "— cancel skipped.",
                order_id,
            )
            return {}
        try:
            _, resp, err = _lighter_run(
                self.signer.cancel_order(
                    market_index,
                    order_index,
                    api_key_index=self.api_key_index,
                )
            )
            if err:
                logger.warning("Lighter cancel_order error (order_id=%s): %s", order_id, err)
                return {}
            return {"tx_hash": getattr(resp, "tx_hash", "")} if resp is not None else {}
        except Exception as e:
            logger.warning("Lighter cancel_order error (order_id=%s): %s", order_id, e)
            return {}

    # ── Leverage ──────────────────────────────────────────────────────────────

    def set_leverage(self, symbol: str, leverage: float, isolated: bool = False) -> None:
        """Set leverage for *symbol* on Lighter before placing an order.

        Lighter's update_leverage applies per-market and persists on the exchange
        until changed again — safe to call before every order.
        SDK signature: update_leverage(market_index, margin_mode, leverage, ...)
        margin_mode: 0 = CROSS, 1 = ISOLATED.
        """
        market_id = self._resolve_market_id(symbol)
        lev = max(1, int(round(leverage)))
        margin_mode = self.signer.ISOLATED_MARGIN_MODE if isolated else self.signer.CROSS_MARGIN_MODE
        try:
            _, _, err = _lighter_run(
                self.signer.update_leverage(
                    market_id,
                    margin_mode,
                    lev,
                    api_key_index=self.api_key_index,
                )
            )
            if err:
                logger.warning("Lighter set_leverage error (%s, %sx): %s", symbol, lev, err)
            else:
                logger.info("Lighter leverage set: %s %sx (%s)", symbol, lev, "isolated" if isolated else "cross")
        except Exception as e:
            logger.warning("Lighter set_leverage failed (%s, %sx): %s", symbol, lev, e)

    # ── Positions ─────────────────────────────────────────────────────────────

    def get_all_positions(self) -> List[Dict[str, Any]]:
        try:
            resp = _lighter_run(
                self.account_api.account(
                    by="index",
                    value=str(self.account_index),
                )
            )
            acct = resp.accounts[0] if (resp.accounts) else None
            if acct is None:
                return []
            positions = acct.positions if isinstance(acct.positions, list) else []
            return [
                self._parse_position(p)
                for p in positions
                if self._position_has_qty(p)
            ]
        except Exception as e:
            logger.warning("Lighter get_all_positions error: %s", e)
            return []

    def get_positions(self, symbol: str = None, **_kwargs) -> List[Dict[str, Any]]:
        """Alias for get_all_positions, filtered by symbol if provided.
        Satisfies the position_query.py interface used by fill recovery."""
        all_pos = self.get_all_positions()
        if not symbol:
            return all_pos
        market_id = self._resolve_market_id(symbol)
        return [p for p in all_pos if p.get("market_id") == market_id]

    def get_position(self, symbol: str) -> Dict[str, Any]:
        market_id = self._resolve_market_id(symbol)
        for pos in self.get_all_positions():
            if pos.get("market_id") == market_id:
                return pos
        return {}

    def get_mark_price(self, symbol: str) -> float:
        try:
            market_id, _size_dec, _price_dec = self._resolve_market(symbol)
            # Try recent trades first (last traded price)
            try:
                resp = _lighter_run(self.order_api.recent_trades(market_id=market_id, limit=1))
                trades = getattr(resp, "trades", None) or (resp if isinstance(resp, list) else [])
                if trades:
                    t = trades[0]
                    raw_price = (
                        t.get("price") if isinstance(t, dict)
                        else getattr(t, "price", None) or getattr(t, "trade_price", None)
                    )
                    if raw_price:
                        return float(raw_price)
            except Exception:
                pass
            # Fallback: order-book mid (or best-ask / best-bid) when no recent trades
            ob = _lighter_run(self.order_api.order_book_orders(market_id, 1))
            asks = getattr(ob, "asks", None) or []
            bids = getattr(ob, "bids", None) or []
            _px = lambda o: float(o["price"] if isinstance(o, dict) else o.price)
            if asks and bids:
                return (_px(asks[0]) + _px(bids[0])) / 2
            if asks:
                return _px(asks[0])
            if bids:
                return _px(bids[0])
        except Exception as e:
            logger.warning("Lighter get_mark_price error (%s): %s", symbol, e)
        return 0.0

    @staticmethod
    def _parse_position(pos: Any) -> Dict[str, Any]:
        """Normalize a Lighter AccountPosition into the standard shape.

        AccountPosition SDK fields (docs):
          market_id, symbol (str, e.g. "BTC"), sign (1=long, -1=short),
          position (str, base qty), avg_entry_price (str).
        """
        if isinstance(pos, dict):
            market_id = int(pos.get("market_id") or 0)
            raw_symbol = str(pos.get("symbol") or "").strip().upper()
            qty = float(pos.get("position") or pos.get("size") or 0.0)
            sign = int(pos.get("sign") or 1)
            avg_price = float(pos.get("avg_entry_price") or pos.get("avg_price") or 0.0)
        else:
            market_id = int(getattr(pos, "market_id", 0) or 0)
            raw_symbol = str(getattr(pos, "symbol", "") or "").strip().upper()
            qty = float(getattr(pos, "position", None) or getattr(pos, "size", 0) or 0.0)
            sign = int(getattr(pos, "sign", 1) or 1)
            avg_price = float(
                getattr(pos, "avg_entry_price", None) or getattr(pos, "avg_price", 0) or 0.0
            )

        # sign=1 → long (bid), sign=-1 → short (ask)
        side = "long" if sign >= 0 else "short"
        # symbol from SDK is base asset only (e.g. "BTC"); normalize to "BTC/USDC"
        symbol = f"{raw_symbol}/USDC" if raw_symbol and "/" not in raw_symbol else raw_symbol
        base = abs(qty)
        return {
            "market_id": market_id,
            "symbol": symbol,
            "side": side,
            "base_qty": base,
            "size": base,          # signed alias for position_row_parse compatibility
            "avg_price": avg_price,
            "entry_price": avg_price,
        }

    @staticmethod
    def _position_has_qty(pos: Any) -> bool:
        qty = 0.0
        if isinstance(pos, dict):
            qty = float(pos.get("position") or pos.get("size") or 0.0)
        else:
            qty = float(getattr(pos, "position", None) or getattr(pos, "size", 0) or 0.0)
        return abs(qty) > 1e-12

    # ── Market stats (derivatives data for AI analysis) ──────────────────────

    def get_market_stats(self, symbol: str) -> Dict[str, Any]:
        """
        Return derivatives market structure data for *symbol* from Lighter's
        public API — no auth required.

        Returns a dict with keys:
          funding_rate        float | None   current funding rate (e.g. 0.0001)
          open_interest       float | None   OI in USD
          volume_24h          float | None   24h notional volume in USD
          daily_price_change  float | None   24h price change fraction (e.g. 0.025 = +2.5%)
          source              str            "lighter"
        """
        result: Dict[str, Any] = {
            "funding_rate": None,
            "open_interest": None,
            "volume_24h": None,
            "daily_price_change": None,
            "source": "lighter",
        }

        coin = to_lighter_coin(symbol)

        # 1) Funding rate from FundingApi (public, no auth)
        try:
            from lighter import FundingApi  # type: ignore[import]
            funding_api = FundingApi(self._api_client)
            resp = _lighter_run(funding_api.funding_rates(), timeout=10.0)
            rates = getattr(resp, "funding_rates", None) or []
            for fr in rates:
                sym = str(getattr(fr, "symbol", "") or "").upper()
                if sym == coin.upper() or sym == f"{coin.upper()}/USDC":
                    rate = getattr(fr, "rate", None)
                    if rate is not None:
                        result["funding_rate"] = float(rate)
                    break
        except Exception as e:
            logger.debug("Lighter get_market_stats funding_rates failed (%s): %s", symbol, e)

        # 2) OI + volume from order_book_details (public, no auth)
        try:
            market_id = self._resolve_market_id(symbol)
            resp = _lighter_run(
                self.order_api.order_book_details(market_id=market_id),
                timeout=10.0,
            )
            detail_list = (
                getattr(resp, "order_book_details", None)
                or getattr(resp, "perps_order_book_detail", None)
                or getattr(resp, "order_book_detail", None)
            )
            detail = (detail_list[0] if isinstance(detail_list, list) and detail_list
                      else detail_list if isinstance(detail_list, dict) else None)
            if detail:
                oi = getattr(detail, "open_interest", None)
                vol = getattr(detail, "daily_quote_token_volume", None)
                chg = getattr(detail, "daily_price_change", None)
                if oi is not None:
                    result["open_interest"] = float(oi)
                if vol is not None:
                    result["volume_24h"] = float(vol)
                if chg is not None:
                    result["daily_price_change"] = float(chg)
        except Exception as e:
            logger.debug("Lighter get_market_stats order_book_details failed (%s): %s", symbol, e)

        return result

    # ── Liveness check (test-connection) ──────────────────────────────────────

    def ping(self) -> bool:
        """Unauthenticated liveness check — fetches the public order books list."""
        try:
            resp = _lighter_run(self.order_api.order_books(), timeout=10.0)
            return bool(getattr(resp, "order_books", None))
        except Exception as e:
            logger.warning("Lighter ping failed: %s", e)
            return False

    def connect(self) -> bool:
        try:
            # Verify signing key works (sync — no network)
            token, err = self.signer.create_auth_token_with_expiry(
                deadline=60, api_key_index=self.api_key_index
            )
            if err:
                raise LiveTradingError(f"Auth token error: {err}")
            # Verify account is reachable on the network
            resp = _lighter_run(
                self.account_api.account(
                    by="index",
                    value=str(self.account_index),
                )
            )
            if not resp.accounts:
                raise LiveTradingError(f"Account {self.account_index} not found on Lighter")
            return True
        except Exception as e:
            logger.warning("Lighter connection test failed: %s", e)
            return False
