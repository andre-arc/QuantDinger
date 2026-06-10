# DEX Perpetual Integration Plan: Hyperliquid & Lighter

**Status:** Draft  
**Date:** 2026-06-10  
**Scope:** Add Hyperliquid and Lighter as live-trading venues alongside existing CEX clients

---

## 1. Background & Motivation

QuantDinger currently supports 8 CEX perpetual venues (Binance, OKX, Bitget, Bybit, Gate, HTX, Kraken, Coinbase). All are custodial and API-key based.

DEX perpetuals operate differently:
- **Self-custody**: funds stay in the user's wallet; no withdrawal risk from exchange hacks
- **On-chain fills**: every order is cryptographically verifiable
- **Private-key signing**: no API key / secret pair — authentication is ECDSA wallet signing
- **Growing liquidity**: Hyperliquid consistently ranks top-3 by open interest among all perp venues

---

## 2. Target Platforms

### Hyperliquid
- **Type:** L1 blockchain with native on-chain order book
- **SDK:** `hyperliquid-python-sdk` (official, PyPI v0.23.0+, Python ≥ 3.10)
- **Auth:** ECDSA private key signing (supports "API Wallet" sub-keys that cannot withdraw)
- **Endpoints:** `https://api.hyperliquid.xyz/exchange` (write), `/info` (read); WebSocket `wss://api.hyperliquid.xyz/ws`
- **Order types:** Market, Limit, Trigger (SL/TP)
- **Rate limits:** Address-based; initial buffer of 10,000 requests/address

### Lighter
- **Type:** Ethereum zk-rollup L2, on-chain order book verified by zk-SNARKs
- **SDK:** `lighter-sdk` (PyPI v1.1.0+)
- **Auth:** Wallet-based signing + per-account API key (index 2–254); time-limited auth tokens via `create_auth_token_with_expiry()`
- **Settlement:** Transactions submitted to sequencer, proofs posted to Ethereum mainnet
- **Order types:** Market (`ORDER_TYPE_MARKET`), Limit (`ORDER_TYPE_LIMIT`), IOC, GTT
- **Fees:** Zero maker/taker fees for retail accounts

---

## 3. Architecture Overview

The integration follows the exact same pattern as all existing CEX clients. No changes to routes, `trading_executor.py`, or `pending_order_worker.py` — only additions to the live-trading layer.

```
pending_order_worker._execute_live_order()
  └─ live_trading/factory.create_client(exchange_config)
        ├─ exchange_id = "hyperliquid"  →  HyperliquidClient
        └─ exchange_id = "lighter"      →  LighterClient

live_order_phases.place_live_limit_order()   ← add isinstance branches
live_order_phases.place_live_market_order()  ← add isinstance branches
live_order_phases.wait_live_order_fill()     ← add isinstance branches
live_order_phases.cancel_live_limit_order()  ← add isinstance branches
```

The key architectural difference from CEX clients: **no `BaseRestClient._request()` with HMAC headers**. Both DEX clients sign payloads locally using the wallet private key before sending.

---

## 4. Files to Create

### 4.1 `app/services/live_trading/hyperliquid.py`

New client wrapping the official SDK.

```python
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from eth_account import Account

class HyperliquidClient:
    exchange_id = "hyperliquid"

    def __init__(self, private_key: str, *, testnet: bool = False):
        account = Account.from_key(private_key)
        base_url = "https://api.hyperliquid-testnet.xyz" if testnet else None
        self.info = Info(base_url=base_url)
        self.exchange = Exchange(account, base_url=base_url)
        self._wallet_address = account.address

    def place_market_order(self, symbol, side, qty, *, reduce_only=False, client_order_id=None) -> LiveOrderResult: ...
    def place_limit_order(self, symbol, side, qty, price, *, reduce_only=False, post_only=False, client_order_id=None) -> LiveOrderResult: ...
    def cancel_order(self, symbol, order_id) -> dict: ...
    def wait_for_fill(self, symbol, order_id, *, max_wait_sec=15.0) -> dict: ...
    def get_position(self, symbol) -> dict: ...
    def get_mark_price(self, symbol) -> float: ...
    def get_all_positions(self) -> list: ...
```

**Key implementation notes:**
- `exchange.market_open(coin, is_buy, sz, None, {"slippage": 0.01})` for market orders
- `exchange.order(coin, is_buy, sz, price, {"limit": {"tif": "Gtc"}})` for limit orders
- `info.user_state(address)["assetPositions"]` for positions
- Symbol format: Hyperliquid uses plain coin names (`"BTC"`, `"ETH"`) — strip the `-PERP` or `USDT` suffix
- Leverage is set per-coin via `exchange.update_leverage(leverage, coin, is_cross)`
- Fill polling: `info.query_order_by_cloid(address, cloid)` for client-order-id lookups

### 4.2 `app/services/live_trading/lighter.py`

New client wrapping the `lighter-sdk`.

```python
from lighter import SignerClient, AccountApi, OrderApi, TransactionApi, ApiClient, Configuration

class LighterClient:
    exchange_id = "lighter"

    def __init__(self, private_key: str, account_index: int = 1, *, testnet: bool = False):
        self.signer = SignerClient(private_key=private_key)
        config = Configuration(host="https://testnet.zklighter.elliot.ai" if testnet else "https://mainnet.zklighter.elliot.ai")
        api_client = ApiClient(configuration=config)
        self.order_api = OrderApi(api_client)
        self.account_api = AccountApi(api_client)
        self.tx_api = TransactionApi(api_client)
        self.account_index = account_index

    def place_market_order(self, symbol, side, qty, *, reduce_only=False, client_order_id=None) -> LiveOrderResult: ...
    def place_limit_order(self, symbol, side, qty, price, *, reduce_only=False, post_only=False, client_order_id=None) -> LiveOrderResult: ...
    def cancel_order(self, order_id) -> dict: ...
    def wait_for_fill(self, order_id, *, max_wait_sec=15.0) -> dict: ...
    def get_position(self, symbol) -> dict: ...
    def get_all_positions(self) -> list: ...
```

**Key implementation notes:**
- Orders created via `signer.sign_create_order(...)` → `tx_api.send_tx(signed_tx)`
- Cancels via `signer.sign_cancel_order(order_id)` → `tx_api.send_tx(signed_tx)`
- Fill polling: `order_api.get_order(order_id)` — poll until status `filled` or `canceled`
- Auth tokens for REST calls: `signer.create_auth_token_with_expiry(expiry_seconds=3600)`
- Symbol format: Lighter uses market IDs (integers); maintain a `symbol → market_id` map, refreshed from `account_api.get_markets()`

---

## 5. Files to Modify

### 5.1 `app/services/live_trading/capabilities.py`

Add two new entries to `CRYPTO_VENUE_CAPABILITIES`:

```python
"hyperliquid": VenueCapability("hyperliquid", frozenset({"swap"})),
"lighter": VenueCapability("lighter", frozenset({"swap"})),
```

Note: Both are perp-only (`swap`). No spot market support.

### 5.2 `app/services/live_trading/factory.py`

Add lazy imports and routing (same pattern as IBKR/Alpaca):

```python
HyperliquidClient = None
LighterClient = None

# In create_client():
if exchange_id in ("hyperliquid",):
    if HyperliquidClient is None:
        from app.services.live_trading.hyperliquid import HyperliquidClient as _C
        HyperliquidClient = _C
    private_key = _get(cfg, "private_key", "privateKey", "wallet_private_key")
    testnet = _coerce_bool(cfg.get("testnet") or cfg.get("enable_testnet"))
    return HyperliquidClient(private_key=private_key, testnet=testnet)

if exchange_id in ("lighter",):
    if LighterClient is None:
        from app.services.live_trading.lighter import LighterClient as _C
        LighterClient = _C
    private_key = _get(cfg, "private_key", "privateKey")
    account_index = int(cfg.get("account_index") or 1)
    testnet = _coerce_bool(cfg.get("testnet"))
    return LighterClient(private_key=private_key, account_index=account_index, testnet=testnet)
```

### 5.3 `app/services/pending_orders/live_order_phases.py`

Add `isinstance` branches for each of the four phase functions:

```python
from app.services.live_trading.hyperliquid import HyperliquidClient
from app.services.live_trading.lighter import LighterClient

# In place_live_limit_order():
if isinstance(client, HyperliquidClient):
    return client.place_limit_order(symbol, side, amount, price,
        reduce_only=reduce_only, post_only=(order_mode in ("maker", "limit")), client_order_id=client_order_id)
if isinstance(client, LighterClient):
    return client.place_limit_order(symbol, side, amount, price,
        reduce_only=reduce_only, client_order_id=client_order_id)

# Same pattern for place_live_market_order(), wait_live_order_fill(), cancel_live_limit_order()
```

### 5.4 `app/services/broker_market_policy.py`

Add DEX venues to the perp-allowed list so the policy validator does not reject them.

### 5.5 `backend_api_python/requirements.txt`

```
hyperliquid-python-sdk>=0.23.0
lighter-sdk>=1.1.0
```

Add as lazy/optional (same pattern as `ib_insync`, `alpaca-py`, `MetaTrader5`) — import only inside the client file, not at module load time.

---

## 6. Credential Storage & UI

### What changes in the credential form

CEX credentials have `api_key` + `secret` (+ optional `passphrase`). DEX credentials need:

| Field | Hyperliquid | Lighter |
|---|---|---|
| `private_key` | EVM wallet private key (hex) | EVM wallet private key (hex) |
| `account_index` | — | Account sub-key index (2–254) |
| `testnet` | boolean toggle | boolean toggle |

**Security requirement:** Private keys must never appear in logs. Extend `safe_exchange_config_for_log()` in `exchange_execution.py` to redact `private_key` and `wallet_private_key` field names.

### Recommended UI flow
1. User selects exchange = "Hyperliquid" or "Lighter"
2. Form shows `Private Key` field (masked input) + `Testnet` toggle
3. "Test Connection" calls `client.get_all_positions()` as a liveness check
4. Optionally display the derived wallet address so users can confirm they pasted the right key

**Recommendation:** Encourage users to use Hyperliquid's **API Wallet** feature — a sub-key that can trade but cannot withdraw. This is safer than pasting the main wallet key.

---

## 7. Symbol Normalization

Both DEX venues use different symbol conventions than CEX:

| Venue | CEX convention | DEX convention | Adapter needed |
|---|---|---|---|
| Hyperliquid | `BTCUSDT` | `BTC` (coin name only) | Strip quote currency suffix |
| Lighter | `BTCUSDT` | integer `market_id` | Lookup map from `get_markets()` |

Add a `symbols.py` helper for each:

```python
# hyperliquid
def to_hl_coin(symbol: str) -> str:
    """'BTCUSDT' → 'BTC', 'ETH-PERP' → 'ETH'"""
    s = str(symbol or "").upper().strip()
    for suffix in ("-PERP", "USDT", "USD", "BUSD"):
        if s.endswith(suffix):
            return s[: -len(suffix)]
    return s

# lighter
_lighter_market_cache: dict = {}
def to_lighter_market_id(symbol: str, api) -> int: ...
```

---

## 8. Exchange Contract Fixtures

Following the existing pattern in `tests/fixtures/exchanges/`, add:

- `order_fill_contracts.json`: add `"hyperliquid"` and `"lighter"` entries with normalized fill snapshots (filled qty, avg price, fee, status)
- `position_contracts.json`: add entries for both venues

Run existing smoke test to verify:
```bash
python scripts/exchange_smoke_test.py --offline-contracts
```

---

## 9. Tests to Add

| Test file | What it covers |
|---|---|
| `tests/test_hyperliquid_client.py` | Symbol normalization, order result parsing, position parsing — all mocked |
| `tests/test_lighter_client.py` | Market ID lookup, order signing (mock signer), fill polling |
| `tests/test_dex_live_order_phases.py` | `place_live_limit_order` / `place_live_market_order` dispatch for both DEX types |
| `tests/test_exchange_offline_contract_fixtures.py` | Extend with HL and Lighter fixture entries |

Live tests (opt-in, testnet only):
```bash
pytest -m integration tests/test_hyperliquid_client.py --testnet
```

---

## 10. Key Differences vs CEX Implementation

| Concern | CEX (e.g. Binance) | Hyperliquid | Lighter |
|---|---|---|---|
| **Auth** | HMAC-SHA256 header | ECDSA private key signing per request | Wallet sign + auth token |
| **Order ID** | String returned by exchange | `cloid` (client order ID) is primary | Integer order ID |
| **Leverage** | Set via separate API call | `exchange.update_leverage()` per coin | Set inside order params |
| **Position side** | `LONG` / `SHORT` hedge mode | Net position (no hedge mode) | Net position |
| **Symbol format** | `BTCUSDT` | `BTC` | `market_id` (int) |
| **Fill polling** | REST order status | `info.query_order_by_cloid()` | `order_api.get_order()` |
| **Testnet** | Separate base URL | `api.hyperliquid-testnet.xyz` | `testnet.zklighter.elliot.ai` |
| **No pos_side** | Pass `positionSide=LONG` | Net mode — omit `pos_side` | Net mode — omit `pos_side` |

**Critical note on hedge mode:** Hyperliquid and Lighter both use **net position mode** (no separate long/short sides). The `pos_side` parameter passed from `signal_to_side_pos_reduce()` must be ignored for DEX clients. The `reduce_only` flag still works as expected.

---

## 11. Implementation Order

1. **Add dependencies** to `requirements.txt` (lazy import — no startup cost if unused)
2. **Create `hyperliquid.py`** client with mock-testable unit boundary at the SDK level
3. **Create `lighter.py`** client
4. **Add offline contract fixtures** for both venues
5. **Update `capabilities.py`** and `factory.py`
6. **Update `live_order_phases.py`** with new `isinstance` branches
7. **Update `broker_market_policy.py`**
8. **Extend `safe_exchange_config_for_log()`** to redact `private_key`
9. **Add unit tests** (mocked)
10. **Testnet smoke test** with real (testnet) wallet
11. **Update `CLAUDE.md`** exchange integration checklist

---

## 12. Out of Scope (Future Work)

- **WebSocket kline feed from DEX:** Currently klines come from `data_sources/` (yfinance, TwelveData, etc.). Hyperliquid has a WebSocket OHLCV feed (`wss://api.hyperliquid.xyz/ws` subscription `candle`) that could replace the polling loop for HL-native symbols — this is a separate data-source work item.
- **DEX-native funding rate display:** Hyperliquid exposes funding rates on-chain; surfacing these in the dashboard requires a new data-provider module.
- **Lighter batch orders:** The SDK supports batching up to 15 transactions per call — useful for grid bot resting orders, but the grid engine would need to be DEX-aware first.
- **On-chain position reconciliation:** The position sync (`_sync_positions_best_effort`) queries the exchange via REST. For DEX, the ground truth is on-chain; a future enhancement could verify against the chain directly.
- **Gas / fee estimation for Lighter:** zk-rollup proof costs are socialized, but monitoring sequencer health is worth adding to the dashboard.
