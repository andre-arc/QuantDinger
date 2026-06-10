# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Stack Overview

QuantDinger is a self-hosted quant trading platform. The open-source repo contains:

- **`backend_api_python/`** — Flask API (gunicorn), the only editable backend source
- **`mcp_server/`** — standalone MCP server (`quantdinger-mcp` PyPI package) that wraps the Agent Gateway REST API as MCP tools
- **Frontend** — built from the private **QuantDinger-Vue** repo; released as a Docker image (`ghcr.io/brokermr810/quantdinger-frontend`); not in this tree

Runtime services (Docker Compose): `frontend` (nginx, :8888), `backend` (Flask/gunicorn, :5000), `postgres` (16, :5432), `redis` (LRU 128 MB, :6379).

## Commands

### Docker (default)

```bash
cp backend_api_python/env.example backend_api_python/.env   # set SECRET_KEY at minimum
docker compose up -d --build
# open http://localhost:8888
```

### Backend locally (no Docker)

```bash
cd backend_api_python
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp env.example .env
python run.py   # dev server on :5000 with auto-reload
```

### Tests

```bash
cd backend_api_python
pytest tests/ -v                          # all unit tests
pytest tests/test_grid_engine.py -v      # single file
pytest -m "not integration" tests/ -v    # skip live-exchange smoke tests
```

Integration tests (`-m integration`) require real testnet credentials in `.env` and are opt-in only.

### Exchange contract smoke tests (no API keys required)

```bash
python scripts/exchange_smoke_test.py --offline-contracts
python scripts/backend_quality_check.py
```

### MCP server (development)

```bash
cd mcp_server
pip install -e ".[dev]"
pytest tests/
```

### Frontend image pinning / local build

```bash
# Pin a version (in project-root .env):
echo "IMAGE_TAG=v3.0.9" >> .env
docker compose pull frontend && docker compose up -d frontend

# Build from local Vue source (clone QuantDinger-Vue into ./QuantDinger-Vue/ first):
docker compose -f docker-compose.yml -f docker-compose.build.yml up -d --build
```

## Backend Architecture

### Module boundaries

| Path | Responsibility |
|------|----------------|
| `app/routes/` | HTTP boundary only — parse request, call service, return JSON. No exchange logic here. |
| `app/services/live_trading/` | Exchange REST clients, order contracts, capability matrix, position parsing, sizing, execution helpers |
| `app/services/grid/` | Grid bot runtime, resting-order placement, fill polling, fill unit conversion, ledger reconciliation |
| `app/services/pending_orders/` | Reusable live-order building blocks (context loading, direction mapping, fill accumulation, exchange-specific phases) |
| `app/services/pending_order_worker.py` | Queue consumer for pending orders — **legacy hot spot**; prefer extracting small services rather than adding new exchange branches here |
| `app/services/trading_executor.py` | Realtime strategy loop — **legacy hot spot**; prefer narrow helpers with tests |
| `app/services/backtest.py` | Historical simulation — **legacy hot spot** |
| `app/data_sources/` | Market data adapters (CCXT, yfinance, Twelve Data, MOEX, …); failures must be explicit enough to diagnose |
| `app/data_providers/` | Market data fetchers for the global dashboard; wired into a fallback chain |
| `app/utils/` | Infrastructure only: auth, DB, cache, logging, time |
| `app/config/` | Settings, API keys, DB config |

### Single sources of truth

- Supported crypto venues → `app/services/live_trading/capabilities.py`
- Broker/market compatibility → `app/services/broker_market_policy.py`
- Stable order domain objects → `app/services/live_trading/contracts.py`
- Exchange fill/position fixtures → `tests/fixtures/exchanges/`

### Two API surfaces

- **Human Web API** (`/api/...`) — JWT auth; `{ "code": 1, "msg": "success", "data": {} }` envelope; used by the Vue SPA
- **Agent Gateway** (`/api/agent/v1/...`) — scoped agent tokens (`qd_agent_...`); do not mix with human routes without `x-agent-only` tag

### Strategy execution (`utils/safe_exec.py`)

User-provided Python strategies run inside a sandboxed executor with a strict builtin whitelist (pure computational only — no I/O, no introspection, no code generation). Never expand `safe_exec` permissions without deliberate review.

### MCP server

`mcp_server/` is a thin wrapper around the Agent Gateway that exposes read (R), workspace-write (W), and backtest (B) tools to MCP clients (Claude, Cursor, etc.). **Live trading endpoints (`quick-trade/*`) are intentionally excluded from MCP.** Credential fields are redacted in responses. See `mcp_server/README.md` for the full tool list.

## Adding Exchanges or Data Sources

**New exchange (live trading):**
1. Create client in `app/services/live_trading/<exchange>.py` inheriting `BaseLiveTrading`
2. Update `capabilities.py` with supported `spot`/`swap` market types
3. Add fill fixtures in `tests/fixtures/exchanges/order_fill_contracts.json`
4. Add position fixtures in `tests/fixtures/exchanges/position_contracts.json` (if derivatives)
5. Run offline contract tests before touching live credentials

**New data source:**
1. Implement `get_ticker(symbol)` and `get_kline(symbol, timeframe, limit)` in `app/data_sources/<name>.py`
2. Register in `data_sources/factory.py`
3. If it feeds the global dashboard, add a fetcher in `data_providers/` and wire into the fallback chain

## Key Constraints

- Do not add `isinstance(client, ExchangeClient)` branches in routes; exchange logic belongs in `live_trading/`
- Do not swallow trading-core exceptions with `except Exception: pass` — mark the order failed and log enough context
- Live API tests must be opt-in and read-only by default; real order tests require both `--allow-orders` flag and `EXCHANGE_SMOKE_ALLOW_ORDERS=1`
- Keep `tests/fixtures/exchanges/` updated before touching live trading paths
- The known legacy hot spots (`trading_executor.py`, `pending_order_worker.py`, `backtest.py`, `routes/quick_trade.py`, `routes/strategy.py`) should not get worse; prefer extracting narrow helpers

## Environment Variables

See `backend_api_python/env.example` for the full list. Key ones:

| Variable | Required | Notes |
|----------|----------|-------|
| `SECRET_KEY` | yes | JWT signing key — must not use the default |
| `ADMIN_USER` / `ADMIN_PASSWORD` | yes | Initial admin credentials |
| `OPENAI_API_KEY` or `OPENROUTER_API_KEY` | no | AI analysis features |
| `TWELVE_DATA_API_KEY` | no | Forex/commodities; Chinese stocks need paid plan |
| `CACHE_ENABLED` | no | Set `true` to use Redis (auto-set in Docker) |
