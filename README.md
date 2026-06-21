# PolyMarket Event Fetcher — v13

A Python tool for monitoring and collecting Polymarket prediction market data with strong consistency guarantees: every trade and price-change event is provably associated with the limit-order-book (LOB) snapshot whose bid/ask context was in effect when the event was received.

This repository tracks the long-form rewrite of the original Colab script. **v13 is the current version** — it carries all v12 fixes plus a substantial defect cleanup across every extraction loop in the system.

---

## What's new in v13

### Correctness

| # | Area | Change |
|---|------|--------|
| **v12** | Interval boundary | Snapshot thread is the sole writer of `interval_id`; counters + id reset under a single `Lock`. Trades and snapshots tagged with the same id always land in the same interval-summary row. |
| **v13** | Snapshot capture | Capture LOB first, then atomically `advance_interval(new_bounds=...)`. Bounds for window N+1 come from the same HTTP that built snapshot N+1 — one round-trip per boundary, not two. |
| **v13** | Failure path | If `capture_lob_snapshot` raises, a placeholder snapshot with NaN bid/ask is emitted so every `interval_id` still has an anchor row. The invariant *trades-must-have-snapshot* survives transient HTTP failures. |
| **v13** | Final close-out | Snapshot thread waits on `_ws_closed_event` (set when the WS reconnect loop exits) before closing the last open window, so in-flight trades aren't orphaned. |
| **v13** | Trade dedup | LRU on `(asset_id, ts, price, size, side)` — WS redeliveries after reconnect no longer double-count. |
| **v13** | Bounds atomicity | `on_message` reads bounds, id, and counters under one `id_lock` acquisition. `advance_interval` swaps all three atomically. No mixed (new id, old bounds) state observable. |

### Resilience

| # | Area | Change |
|---|------|--------|
| **v13** | Helper threads | `ping`, watchdog, and `timer` start once per session **before** the first WS connect. The old `if reconnect_count == 0` guard inside `on_open` would silently disable them when the first connect failed pre-`on_open`. |
| **v13** | Reconnect loop | `on_close` is now a pure logger. The outer loop in `run()` recreates the `WebSocketApp` and calls `run_forever()` — no recursive `run_forever()` inside `on_close`, so 1000 reconnects don't blow the stack. |
| **v13** | Reconnect backoff | Exponential with 10% jitter, capped at 300s. `reconnect_count` resets after a stable uptime (default 600s). |
| **v13** | Cancellable sleeps | All background sleeps go through `threading.Event.wait(timeout=…)` so Ctrl-C / timer / stop are honored promptly. |
| **v13** | HTTP timeouts | `client.get_order_book` is wrapped in a `ThreadPoolExecutor` future with a 15s default timeout (py_clob_client has no native timeout knob). |
| **v13** | Drift-free cadence | Snapshot thread sleeps to a deadline (`start_time + snapshot_interval * snapshot_count`), not "now + interval", so cadence doesn't drift later under HTTP latency. |

### Persistence

| # | Area | Change |
|---|------|--------|
| **v13** | `global_interval_id` | Saved to `<output_dir>/.global_interval_id` after every session (file-locked, cross-platform), reloaded at start. Master CSVs keep monotonically increasing ids across **process restarts**, not just across sessions within one run. |
| **v13** | Master CSV writes | File-locked (`O_CREAT \| O_EXCL` sidecar lock with stale-lock reaper). Concurrent processes against the same `output_dir` can't interleave rows. |
| **v13** | Schema drift | New columns dropped at append-time are now warned about (the v12 silent drop is gone). |
| **v13** | TZ alignment (Patch B2) | When appending tz-aware v13 rows to a pre-v13 master CSV with naive timestamps, the tz is stripped from the new rows to keep the column single-format. |

### Operational

| # | Area | Change |
|---|------|--------|
| **v13 (Patch B1)** | Importable | `ClobClient` construction moved into `_build_default_client()`, called only from the `__main__` block. `import polymarketeventfetcher_v13` is now side-effect-free (no network call). |
| **v13** | Secrets | Private key **must** come from `POLYMARKET_PRIVATE_KEY` env var. The repository copy has no inline fallback. |
| **v13** | Event selection | `fetch_active_event(lookback_hours=24)` parameterized; sorts by `startDate` descending so the newest-started active event wins. |
| **v13** | Cycle loop | Inner "wait for new event" loop respects the global time budget; transition sleep is bounded by remaining budget. |
| **v13** | Output | Per-session Excel files (`.xlsx`) + per-event-and-cross-session master CSVs (`.csv`). |

---

## Architecture

### Threading model

```
┌─────────────────────────────────────────────────────────────────┐
│  PolymarketEventFetcher.monitor_market_activity (main thread)   │
└──────────────────────────┬──────────────────────────────────────┘
                           │
        ┌──────────────────┼──────────────────────┐
        ▼                  ▼                      ▼
┌────────────────┐  ┌────────────────┐  ┌────────────────────────┐
│ snapshot       │  │ websocket      │  │ run() helper threads   │
│ thread         │  │ thread         │  │  (started by run()):   │
│                │  │                │  │   - ping  (10s)        │
│ every          │  │ run_forever()  │  │   - watchdog (ws_int)  │
│ snapshot_intvl │  │ in a reconnect │  │   - timer  (duration)  │
│ - capture LOB  │  │ outer loop     │  │                        │
│ - advance_intvl│  │                │  │ all sleeps via         │
│   under lock   │  │ on_message:    │  │ _stop_event.wait(…)    │
└────────────────┘  │  dedup → lock  │  └────────────────────────┘
        │           │  → tag id      │
        │           └────────────────┘
        ▼
  CSV/Excel export
```

### Interval boundary protocol (the core invariant)

```
        ┌───────────── window N ─────────────┐    ┌── window N+1 ──┐
events: │ trade trade ... pc trade ...       │    │ trade  trade   │
ids:    │   N     N        N    N            │    │  N+1    N+1    │
        │                                    │    │                │
        └──────────────────────────┬─────────┘    └────────────────┘
                                   │
                            snapshot thread:
                            ┌───────────────────────────────┐
                            │ capture LOB (HTTP, 1–2 s)     │
                            │ extract bounds_{N+1}          │
                            │ advance_interval(b_{N+1}):    │
                            │   ┌─ under id_lock ─────────┐ │
                            │   │ append summary(id=N)    │ │
                            │   │ reset counters          │ │
                            │   │ token_bounds = b_{N+1}  │ │
                            │   │ current_interval_id++   │ │
                            │   └─────────────────────────┘ │
                            │ tag snapshot row id=N+1       │
                            └───────────────────────────────┘
```

`on_message` reads bounds, id, and counter under the same `id_lock`. The boundary is fully atomic — no event can land in window N's counter with id N+1, or vice versa.

### Cross-session continuity

```
session 1                 session 2 (process restart OK)
───────────────────────   ───────────────────────────────
load .global_interval_id  load .global_interval_id
   → 1                       → N+1 (persisted at session 1 end)
WSOrderBook(initial=1)    WSOrderBook(initial=N+1)
   ... advances to N         ... advances to N+M
save → N+1                save → N+M+1
```

The sidecar file `<output_dir>/.global_interval_id` is the single source of truth across runs. Deleting it resets the counter; preserving it (default) preserves monotonicity.

---

## Requirements

### Python packages

```bash
pip install requests numpy pandas openpyxl websocket-client py-clob-client
```

`openpyxl` is required for the per-event `.xlsx` files. `websocket-client` is the package that provides `from websocket import WebSocketApp` (not the unrelated `websocket` package).

### Debian / Ubuntu install (with venv)

```bash
sudo apt update
sudo apt install -y python3 python3-pip python3-venv
python3 -m venv ~/polymarket-venv
source ~/polymarket-venv/bin/activate
pip install --upgrade pip
pip install requests numpy pandas openpyxl websocket-client py-clob-client
```

### Prerequisites

1. **Polymarket account** with USDC funding.
2. **Private key** exported from your wallet or magic-link:
   - Magic-link accounts: https://reveal.magic.link/polymarket
   - Web3 wallets: export from the wallet UI
3. **Proxy address** — your Polymarket deposit address (where USDC sits).

---

## Configuration

### Environment variables

```bash
export POLYMARKET_PRIVATE_KEY=0x...     # required
export POLYMARKET_PROXY_ADDRESS=0x...   # optional; defaults to the value in the script
```

Running without `POLYMARKET_PRIVATE_KEY` raises `RuntimeError` at `_build_default_client()` call time — the repository copy has no inline fallback.

---

## Usage

### Continuous monitoring (the typical entry point)

```python
from polymarketeventfetcher_v13 import PolymarketEventFetcher, _build_default_client

client  = _build_default_client()
fetcher = PolymarketEventFetcher('btc-up-or-down-daily')
fetcher.set_clob_client(client)

fetcher.run_continuous_cycle_seconds(
    total_seconds=4 * 60 * 60,   # 4-hour budget
    snapshot_interval=30,         # LOB snapshot + interval boundary every 30s
    ws_interval=30,               # WS health-check cadence (no longer drives intervals)
    lob_levels=10,                # depth of LOB to capture
    output_dir='LOB_Trial_3',     # all outputs land here, including .global_interval_id
    verbose=True,
)
```

Or just `python polymarketeventfetcher_v13.py` — the `__main__` block runs the above with sensible defaults.

### Single monitoring session

```python
fetcher = PolymarketEventFetcher('btc-up-or-down-daily')
fetcher.set_clob_client(_build_default_client())
fetcher.fetch_active_event()

results = fetcher.monitor_market_activity(
    duration_seconds=300,    # 5 minutes
    snapshot_interval=30,    # 30-second windows
    ws_interval=30,          # health-check cadence
    lob_levels=10,
    verbose=True,
)

lob_yes    = results['lob_snapshots']['YES']
lob_no     = results['lob_snapshots']['NO']
trades_yes = results['trades']['YES']
trades_no  = results['trades']['NO']
intervals  = results['intervals']
summary    = results['summary']
```

### Single LOB snapshot

```python
fetcher = PolymarketEventFetcher('btc-up-or-down-daily')
fetcher.set_clob_client(_build_default_client())
fetcher.fetch_active_event()
snap = fetcher.capture_lob_snapshot(levels=10)
print(snap['YES'])  # YES token, one-row DataFrame
print(snap['NO'])   # NO token
```

---

## Output files

For each session in `output_dir`:

| File | Content |
|------|---------|
| `<slug>_snapshots_YES_<ts>.xlsx`  | Per-session YES LOB snapshots |
| `<slug>_snapshots_NO_<ts>.xlsx`   | Per-session NO LOB snapshots |
| `<slug>_trades_YES_<ts>.xlsx`     | Per-session YES trades |
| `<slug>_trades_NO_<ts>.xlsx`      | Per-session NO trades |
| `<slug>_intervals_<ts>.xlsx`      | Per-session interval-summary rows |
| `<slug>_summary_<ts>.xlsx`        | One-row aggregate stats |

Cross-session master files (append-only, file-locked):

| File | Content |
|------|---------|
| `master_snapshots_YES_full.csv` | Every YES snapshot ever captured, joinable on `interval_id` |
| `master_snapshots_NO_full.csv`  | Every NO snapshot ever captured |
| `master_trades_YES_full.csv`    | Every YES trade ever observed |
| `master_trades_NO_full.csv`     | Every NO trade ever observed |
| `master_intervals_full.csv`     | Every interval summary ever finalized |

Plus the persistence sidecar:

| File | Content |
|------|---------|
| `.global_interval_id` | Next `interval_id` to use. Loaded at start, written under file lock at the end of every session (including failed ones, via `finally`). |

### Column reference (master CSVs)

**Snapshots (YES and NO):**
`bid_1..bid_10, ask_1..ask_10, bidsize_1..bidsize_10, asksize_1..asksize_10, spread, snapshot_timestamp, total_volume_(yes_and_no), settlement_price, current_price, price_diff, time_to_expiry, interval_id, elapsed_seconds, side, event_id, event_slug, event_title`

**Trades (YES and NO):**
`interval_id, asset_id, price, size, side, timestamp, fee_rate_bps, side_token, event_id, event_slug`

**Intervals (shared across sides):**
`interval_id, interval_number, timestamp, yes_price_changes, no_price_changes, total_price_changes, yes_trade_count, no_trade_count, total_trade_count, yes_bound_min, yes_bound_max, no_bound_min, no_bound_max, reconnect_count, event_id, event_slug`

`interval_id` is the cross-session monotonic key. `interval_number` is per-session (resets each run, kept for debugging).

---

## Invariant check

At the end of every session, the code runs (log-only):

```
✅ Interval invariant holds: trade counts match summaries and every trade interval_id has a snapshot anchor.
```

If anything ever prints `❌` here, the underlying race has resurfaced. Open an issue with the offending interval ids.

---

## Class reference

### PolymarketEventFetcher

| Method | Purpose |
|--------|---------|
| `__init__(series_slug, base_url=...)` | Initialize with a series identifier (e.g. `'btc-up-or-down-daily'`). |
| `set_clob_client(client)` | Attach an authenticated `ClobClient`. Required before any LOB call. |
| `fetch_active_event(lookback_hours=24)` | Fetch the newest active event in the series. |
| `capture_lob_snapshot(levels=10, settlement_price=None, http_timeout=15)` | One LOB snapshot for the active event. Times out at the HTTP layer. |
| `capture_multiple_lob_snapshots(num_snapshots=5, interval_seconds=3, levels=10, verbose=True, settlement_price=None)` | Standalone helper — emits snapshots without `interval_id`. |
| `monitor_market_activity(duration_seconds=300, snapshot_interval=30, ws_interval=5, lob_levels=10, verbose=True)` | One full session: WS + snapshot thread, runs the boundary protocol, ends with invariant check. |
| `run_continuous_cycle_seconds(total_seconds, snapshot_interval=30, ws_interval=30, lob_levels=10, output_dir='data', verbose=False)` | Outer loop: fetches new active events, runs session, exports, persists `global_interval_id`. |
| `export_monitoring_results(results, output_dir='data')` | Writes Excel + master CSV files. |

### WebSocketOrderBook

| Method | Purpose |
|--------|---------|
| `__init__(..., initial_interval_id=1, health_threshold_seconds=60, reconnect_count_reset_uptime=600, seen_trade_maxlen=10000)` | Construct; seeds `current_interval_id` from caller, parameterizes health and dedup behavior. |
| `run()` | Start helper threads, enter reconnect loop, return `{price_changes, trades, intervals}` when done. |
| `advance_interval(new_bounds=None)` | The atomic boundary primitive. Sole writer of `current_interval_id` after init. Returns `(finalized_id, new_id)`. |
| `update_bounds()` | Legacy: standalone bounds refresh. Not used by the main flow (folded into `advance_interval`). |
| `connection_health_watchdog()` | Background loop: forces close if no WS messages for `health_threshold_seconds`. |
| `get_trade_dataframes()` | `{'YES': df, 'NO': df}` with `timestamp` parsed to tz-aware UTC. |
| `get_interval_dataframe()` | DataFrame of finalized interval summary rows. |

---

## Troubleshooting

| Symptom | Cause / fix |
|---------|-------------|
| `RuntimeError: POLYMARKET_PRIVATE_KEY environment variable is not set` | Export the env var: `export POLYMARKET_PRIVATE_KEY=0x...`. |
| `ModuleNotFoundError: No module named 'openpyxl'` | `pip install openpyxl` — pandas needs it for `.xlsx` writes. |
| `ImportError: cannot import name 'WebSocketApp' from 'websocket'` | You installed the wrong package. `pip install websocket-client` (hyphenated). |
| `❌ YES trade/interval mismatch: {...}` at session end | The boundary invariant broke — please open an issue with the offending ids and the session log. |
| `⚠️ No WS messages for 60s, forcing reconnect` | Normal during network blips. Reconnect handled by outer loop in `run()`. |
| `⚠️ Master ... stripping tz from 'snapshot_timestamp'` | First v13 write to a pre-v13 master CSV. The tz is dropped to keep the column single-format. Expected. |
| Master CSV `interval_id` resets to 1 unexpectedly | The `.global_interval_id` sidecar was deleted or `output_dir` changed. Restore or copy the file to keep ids continuous. |
| Process hangs on Ctrl-C | `ws_thread` / `snap_thread` aren't daemon. Send SIGTERM, or wait up to `duration_seconds + grace`. Bounded joins will unstick. |

---

## Data analysis examples

### Per-window trade counts

```python
import pandas as pd

trades = pd.read_csv('LOB_Trial_3/master_trades_YES_full.csv')
intervals = pd.read_csv('LOB_Trial_3/master_intervals_full.csv')

per_window = trades.groupby('interval_id').size().rename('trades_observed')
joined = intervals.set_index('interval_id').join(per_window)
assert (joined['yes_trade_count'] == joined['trades_observed'].fillna(0)).all(), "boundary invariant violated"
print("invariant holds across all master rows")
```

### Spread over time

```python
import pandas as pd
import matplotlib.pyplot as plt

lob = pd.read_csv('LOB_Trial_3/master_snapshots_YES_full.csv',
                  parse_dates=['snapshot_timestamp'])
lob.sort_values('interval_id').plot(x='interval_id', y='spread')
plt.xlabel('interval_id (continuous across sessions)')
plt.ylabel('YES spread')
plt.show()
```

### VWAP per session

```python
import pandas as pd

trades = pd.read_csv('LOB_Trial_3/master_trades_YES_full.csv')
vwap = (trades['price'] * trades['size']).groupby(trades['event_slug']).sum() \
       / trades['size'].groupby(trades['event_slug']).sum()
print(vwap)
```

---

## Version history

### v13 (current)
- Snapshot LOB before advancing the interval boundary; bounds for the next window derive from the same HTTP (one round-trip per boundary).
- Placeholder snapshot on capture failure preserves the trade-must-have-snapshot invariant.
- `_ws_closed_event` synchronizes final close-out with WS drain so trailing trades aren't orphaned.
- Trade dedup via LRU on `(asset_id, ts, price, size, side)` — reconnects no longer double-count.
- `on_message` reads bounds + id + counter under one `id_lock`. `advance_interval(new_bounds=…)` swaps all three atomically.
- Helper threads (`ping`, watchdog, `timer`) start once in `run()` before any connect, not inside `on_open`. Removes the `if reconnect_count == 0` guard that disabled them after a pre-`on_open` failure.
- Reconnection driven by an outer loop in `run()` with exponential backoff + jitter and stable-uptime reset of `reconnect_count`. No recursive `run_forever()` inside `on_close`.
- All sleeps cancellable via `_stop_event`.
- `client.get_order_book` calls wrapped with a 15s timeout via a module-level `ThreadPoolExecutor`.
- `global_interval_id` persisted to `<output_dir>/.global_interval_id`, file-locked, reloaded on next run. Cross-restart monotonicity.
- Master CSV writes are file-locked (cross-platform sidecar lock with stale reaper); schema drift now warns.
- TZ alignment of new rows to existing master CSV format (no mixed ISO dialects).
- Patch B1: `ClobClient` construction moved under `__main__`; `import polymarketeventfetcher_v13` is now side-effect-free.
- Patch B2: `_align_datetime_tz_to_existing` strips tz from new rows when appending to pre-v13 master CSVs.
- Private key now required from `POLYMARKET_PRIVATE_KEY` env var. No inline fallback in the repo.
- `fetch_active_event(lookback_hours=…)` parameterized; sorts by `startDate` descending.
- `run_continuous_cycle_seconds` respects total budget in inner event-wait loop and transition sleep.
- Snapshot timestamps in UTC; Binance `current_price` fetched once per snapshot (was twice with inconsistent YES/NO values).

### v12
- Snapshot thread became the sole writer of `interval_id`; counters reset + id increment + interval-summary append now under one `Lock`. Closed the v11 race where trades arriving between counter reset and id increment landed in the wrong summary row.
- `interval_tracker` timer removed; its bookkeeping moved into `advance_interval()` driven by the snapshot thread. The old timer survives only as `connection_health_watchdog`.
- Session-end log line asserts trade counts per `interval_id` match summary rows.

### v11 and earlier
- Original two-clock design (snapshot timer + interval timer). Functional but contained the race that v12 fixed.

---

## License

MIT — see LICENSE.

## Disclaimer

For research and educational use. Trading prediction markets involves financial risk. The authors assume no responsibility for losses. Do your own research.

## Acknowledgments

- [py-clob-client](https://github.com/Polymarket/py-clob-client) — official Polymarket CLOB client
- [Polymarket](https://polymarket.com) — market data
- [Binance API](https://binance-docs.github.io/apidocs/) — BTC reference prices
