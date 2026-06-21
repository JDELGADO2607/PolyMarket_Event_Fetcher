# -*- coding: utf-8 -*-
"""PolyMarketEventFetcher v13.

v13 fixes (carried over the v12 interval-id race fix):
  - on_open's `reconnect_count == 0` guard removed; ping/watchdog/timer
    threads now start once per session in `run()` so a pre-on_open
    failure no longer disables the entire health stack.
  - `on_close` no longer calls `run_forever()` recursively; reconnection
    is driven by an outer loop in `run()` with exponential backoff + jitter.
  - Trade dedup via LRU on (asset_id, ts, price, size, side); reconnects
    no longer double-count.
  - `advance_interval(new_bounds=...)` applies new bounds atomically with
    the id increment.
  - Snapshot thread now captures LOB FIRST, then advances under lock with
    bounds derived from that LOB (one HTTP round-trip per boundary instead
    of two). On capture failure, emits a placeholder snapshot so every
    interval_id has an anchor row.
  - Final close-out waits on `_ws_closed_event` so trades in flight at
    session end land in the right (still-open) window, not as orphans.
  - Deadline-based snapshot scheduling — no period drift.
  - `global_interval_id` persisted to `<output_dir>/.global_interval_id`
    and reloaded on init, so master CSV ids stay unique across restarts.
  - Cross-platform file lock for master CSV writes.
  - `_call_with_timeout` wrapper enforces HTTP timeouts on
    `client.get_order_book` (py_clob_client has no native timeout).
  - `requests.Session` reused for Binance / Gamma / volume HTTP.
  - `monitor_market_activity` wraps thread joins in try/finally so
    `global_interval_id` survives partial failures.
  - `run_continuous_cycle_seconds` event-wait loop respects total budget.
  - Snapshot timestamps in UTC; Binance `current_price` fetched once per
    snapshot (was twice, producing inconsistent YES/NO values).
"""


import json
import time
import datetime
import threading
import os
import platform
import contextlib
import random
import collections
import concurrent.futures
from typing import Optional, Dict, List, Any
from datetime import datetime, timezone, timedelta

import requests
import numpy as np
import pandas as pd
from websocket import WebSocketApp
from py_clob_client.client import ClobClient


# ============================================================================
# CONFIG
# ============================================================================

host: str = "https://clob.polymarket.com"

# v13 (14c): private key MUST come from the environment. The repository copy
# intentionally has no inline fallback. The actual validation is deferred to
# `_build_default_client()` so importing this module is side-effect-free.
key: str = os.environ.get('POLYMARKET_PRIVATE_KEY', '')

chain_id: int = 137
POLYMARKET_PROXY_ADDRESS: str = os.environ.get(
    'POLYMARKET_PROXY_ADDRESS',
    '0xd6e47879c1ef7d0216dfea27c1119a49e12c9ae4'
)
MARKET_CHANNEL = 'market'
USER_CHANNEL = 'user'


# ============================================================================
# MODULE-LEVEL HELPERS  (v13)
# ============================================================================

# v13 (14b): shared HTTP session — connection pooling for Binance/Gamma/volume.
_HTTP_SESSION = requests.Session()

# v13 (10c): py_clob_client's `get_order_book` has no timeout knob. We run it
# inside a worker thread and bound it with `Future.result(timeout=…)`. A hung
# call leaks a daemon thread but doesn't block the caller.
_CLOB_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=4, thread_name_prefix='clob-http'
)


def _call_with_timeout(func, timeout, *args, **kwargs):
    """Submit `func(*args, **kwargs)` to the CLOB executor; raise TimeoutError on timeout."""
    future = _CLOB_EXECUTOR.submit(func, *args, **kwargs)
    try:
        return future.result(timeout=timeout)
    except concurrent.futures.TimeoutError:
        future.cancel()
        raise TimeoutError(
            f"{getattr(func, '__name__', 'call')} timed out after {timeout}s"
        )


@contextlib.contextmanager
def _file_lock(path, timeout=30, stale_after=300):
    """
    Cross-platform exclusive file lock via a sidecar `.lock` file.

    Uses O_CREAT|O_EXCL for atomic acquisition (works on POSIX and Windows).
    Stale locks older than `stale_after` seconds are reaped — protects against
    a previous process crashing before it released the lock.
    """
    lock_path = path + '.lock'
    start = time.time()
    while True:
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.close(fd)
            break
        except FileExistsError:
            # Stale-lock reaper
            try:
                age = time.time() - os.path.getmtime(lock_path)
                if age > stale_after:
                    print(f"⚠️ Removing stale lock {lock_path} (age={age:.0f}s)")
                    try:
                        os.unlink(lock_path)
                    except FileNotFoundError:
                        pass
                    continue
            except FileNotFoundError:
                continue
            if time.time() - start > timeout:
                raise TimeoutError(
                    f"Could not acquire lock {lock_path} within {timeout}s"
                )
            time.sleep(0.1)
    try:
        yield
    finally:
        try:
            os.unlink(lock_path)
        except FileNotFoundError:
            pass


def _build_default_client() -> ClobClient:
    """Construct the default ClobClient and derive API creds.

    Patch B1: moved out of module top-level so `import polymarketeventfetcher_v13`
    does not perform a network call. Callers that want the default client
    invoke this helper explicitly (the `__main__` entry point does).

    Raises RuntimeError if POLYMARKET_PRIVATE_KEY is not set in the
    environment — the repository copy intentionally has no inline fallback.
    """
    if not key:
        raise RuntimeError(
            "POLYMARKET_PRIVATE_KEY environment variable is not set. "
            "Export it before running, e.g. `export POLYMARKET_PRIVATE_KEY=0x...`. "
            "See https://reveal.magic.link/polymarket for magic-link accounts, "
            "or export from your Web3 wallet."
        )
    c = ClobClient(
        host, key=key, chain_id=chain_id, signature_type=1,
        funder=POLYMARKET_PROXY_ADDRESS,
    )
    print(c.create_or_derive_api_creds())
    return c


def get_lob_bounds(client, token_id_yes, token_id_no, level=10, http_timeout=15):
    """
    Fetch bid_N and ask_N for both YES and NO tokens, with timeout.

    Returns dict mapping asset_id -> {'min': bid_N, 'max': ask_N, 'side': 'YES'|'NO'}.
    """
    ob_yes = _call_with_timeout(client.get_order_book, http_timeout, token_id_yes)
    ob_no  = _call_with_timeout(client.get_order_book, http_timeout, token_id_no)

    def _bid_at(ob, level_):
        if ob.bids and len(ob.bids) >= level_:
            return float(ob.bids[-level_].price)
        return float(ob.bids[-1].price) if ob.bids else 0.0

    def _ask_at(ob, level_):
        if ob.asks and len(ob.asks) >= level_:
            return float(ob.asks[-level_].price)
        return float(ob.asks[-1].price) if ob.asks else 1.0

    yes_bid_10 = _bid_at(ob_yes, level)
    yes_ask_10 = _ask_at(ob_yes, level)
    no_bid_10  = _bid_at(ob_no, level)
    no_ask_10  = _ask_at(ob_no, level)

    print("Price Bounds Set:")
    print(f"YES: {yes_bid_10:.4f} - {yes_ask_10:.4f}")
    print(f"NO:  {no_bid_10:.4f} - {no_ask_10:.4f}\n")

    return {
        token_id_yes: {'min': yes_bid_10, 'max': yes_ask_10, 'side': 'YES'},
        token_id_no:  {'min': no_bid_10,  'max': no_ask_10,  'side': 'NO'},
    }


# ============================================================================
# PolymarketEventFetcher
# ============================================================================

class PolymarketEventFetcher:
    """
    Fetch and analyze Polymarket recurring series events with LOB snapshots.
    """

    def __init__(self, series_slug: str, base_url: str = "https://gamma-api.polymarket.com"):
        self.series_slug = series_slug
        self.base_url = base_url
        self.data = None
        self.client = None

        # Global, monotonically-increasing interval ID. Persists across event
        # rollovers via `_load_global_interval_id` / `_save_global_interval_id`.
        self.global_interval_id = 1

    # ------------------------------------------------------------------ Binance

    def get_binance_settlement_price(self, reference_date=None):
        """
        Get the Binance BTC/USDT 1m close price at a reference moment.

        For Polymarket "BTC Up or Down Daily", the right anchor is endDate - 24h.
        Returns None when the reference is in the future or the call fails.
        """
        now_utc = datetime.now(timezone.utc)

        if reference_date is not None:
            if hasattr(reference_date, 'to_pydatetime'):
                reference_date = reference_date.to_pydatetime()
            target_time = reference_date.replace(second=0, microsecond=0)
            if target_time > now_utc:
                print(f"⚠️ Reference time {target_time} is in the future. "
                      f"Cannot fetch settlement price yet.")
                return None
        else:
            if now_utc.hour >= 17:
                target_date = now_utc
            else:
                target_date = now_utc - timedelta(days=1)
            target_time = target_date.replace(hour=17, minute=0, second=0, microsecond=0)

        target_timestamp = int(target_time.timestamp() * 1000)
        url = "https://api.binance.com/api/v3/klines"
        params = {
            "symbol": "BTCUSDT",
            "interval": "1m",
            "startTime": target_timestamp,
            "limit": 1,
        }

        try:
            response = _HTTP_SESSION.get(url, params=params, timeout=5)
            data = response.json()
            if data and len(data) > 0:
                close_price = float(data[0][4])
                print(f"✅ Binance reference price at "
                      f"{target_time.strftime('%Y-%m-%d %H:%M UTC')}: ${close_price:,.2f}")
                return close_price
            print("⚠️ No data returned from Binance")
            return None
        except Exception as e:
            print(f"❌ Error fetching Binance price: {e}")
            return None

    def get_binance_current_price(self):
        """Get the most recent Binance BTC close price (1m kline)."""
        url = "https://api.binance.com/api/v3/klines"
        params = {"symbol": "BTCUSDT", "interval": "1m", "limit": 1}
        try:
            response = _HTTP_SESSION.get(url, params=params, timeout=5)
            data = response.json()
            if data and len(data) > 0:
                return float(data[0][4])
            print("⚠️ No data returned from Binance")
            return None
        except Exception as e:
            print(f"❌ Error fetching Binance price: {e}")
            return None

    # ------------------------------------------------------------------ events

    def fetch_active_event(self, lookback_hours: int = 24) -> pd.DataFrame:
        """
        Fetch the currently active daily event for this series.

        v13 (12a/12b): `lookback_hours` parameterizes the activity window
        (default 24h matches daily cadence). Events are sorted by `startDate`
        descending so the newest-started active event wins.
        """
        try:
            response = _HTTP_SESSION.get(
                f"{self.base_url}/series",
                params={'slug': self.series_slug},
                timeout=10,
            )
            response.raise_for_status()
            series_data = response.json()

            if not series_data or 'events' not in series_data[0]:
                raise ValueError(f"No events found for series: {self.series_slug}")

            df = pd.DataFrame(series_data[0]['events'])
            if df.empty:
                raise ValueError("No events available")

            df['creationDate'] = pd.to_datetime(df['creationDate'], format='ISO8601', utc=True)
            df['startDate']    = pd.to_datetime(df['startDate'],    format='ISO8601', utc=True)
            df['endDate']      = pd.to_datetime(df['endDate'],      format='ISO8601', utc=True)

            now      = pd.Timestamp.now(tz='UTC')
            cutoff   = now - pd.Timedelta(hours=lookback_hours)
            df = df[
                df['active'] &
                ~df['closed'] &
                (df['creationDate'] >= cutoff) &
                (df['creationDate'] < now)
            ]

            if df.empty:
                raise ValueError("No active events match the criteria")

            columns = ['id', 'slug', 'title', 'volume', 'startDate', 'endDate']
            # v13 (12b): pick the newest-started active event.
            df = df[columns].sort_values('startDate', ascending=False).reset_index(drop=True)

            df = self._add_clob_tokens(df)
            self.data = df
            return df

        except requests.RequestException as e:
            raise ConnectionError(f"Failed to fetch data from Polymarket: {e}")

    def _add_clob_tokens(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        response = _HTTP_SESSION.get(
            f"{self.base_url}/events/slug/{df.loc[0, 'slug']}",
            timeout=10,
        )
        response.raise_for_status()
        event_data = response.json()
        clob_tokens_str = event_data['markets'][0]['clobTokenIds']
        clob_tokens = json.loads(clob_tokens_str)
        df.loc[0, 'clob_token_id_YES'] = clob_tokens[0]
        df.loc[0, 'clob_token_id_NO']  = clob_tokens[1]
        return df

    def set_clob_client(self, client: ClobClient):
        self.client = client

    # ----------------------------------------------------- LOB snapshot capture

    def capture_lob_snapshot(self, levels: int = 10, settlement_price=None,
                              http_timeout: int = 15) -> Dict:
        """
        Capture a single LOB snapshot for the active event.

        v13:
          - Order-book calls are wrapped in `_call_with_timeout` (10c).
          - `snapshot_timestamp` is UTC-aware (10b).
          - Binance current price fetched once per snapshot, used for both
            YES and NO so `current_price` is consistent across sides (10a).
          - Volume response indexing is defensive (10e).
          - `settlement_price` comparison uses `is not None` (10d).
        """
        if self.data is None or self.data.empty:
            raise ValueError("No active event data. Call fetch_active_event() first.")
        if self.client is None:
            raise ValueError("CLOB client not set. Call set_clob_client() first.")

        event = self.data.iloc[0]
        ts = datetime.now(timezone.utc)  # v13 (10b): UTC-aware
        now    = ts.timestamp()
        target = event['endDate'].timestamp()

        # v13 (10c): bounded order-book fetches
        try:
            ob_yes = _call_with_timeout(
                self.client.get_order_book, http_timeout, event['clob_token_id_YES']
            )
        except Exception as e:
            print(f"Warning: Failed to fetch YES order book: {e}")
            ob_yes = None

        try:
            ob_no = _call_with_timeout(
                self.client.get_order_book, http_timeout, event['clob_token_id_NO']
            )
        except Exception as e:
            print(f"Warning: Failed to fetch NO order book: {e}")
            ob_no = None

        # Volume with defensive indexing (v13 10e)
        vol = np.nan
        try:
            vol_response = _HTTP_SESSION.get(
                'https://data-api.polymarket.com/live-volume',
                params={'id': event['id']},
                timeout=10,
            )
            vol_response.raise_for_status()
            vol_data = vol_response.json()
            if isinstance(vol_data, list) and len(vol_data) > 0 and isinstance(vol_data[0], dict):
                total = vol_data[0].get('total')
                if total is not None:
                    vol = float(total)
        except Exception as e:
            print(f"Warning: Failed to fetch volume: {e}")

        # v13 (10a): fetch Binance current price ONCE; share between YES and NO
        current_price = self.get_binance_current_price()

        def _build_side(ob, levels_):
            data_dict = {}
            if ob and ob.bids and ob.asks:
                try:
                    for i in range(1, levels_ + 1):
                        if i <= len(ob.bids) and i <= len(ob.asks):
                            data_dict[f'bid_{i}']     = float(ob.bids[-i].price)
                            data_dict[f'ask_{i}']     = float(ob.asks[-i].price)
                            data_dict[f'bidsize_{i}'] = float(ob.bids[-i].size)
                            data_dict[f'asksize_{i}'] = float(ob.asks[-i].size)
                        else:
                            data_dict[f'bid_{i}']     = np.nan
                            data_dict[f'ask_{i}']     = np.nan
                            data_dict[f'bidsize_{i}'] = np.nan
                            data_dict[f'asksize_{i}'] = np.nan
                    data_dict['spread'] = float(ob.asks[-1].price) - float(ob.bids[-1].price)
                except Exception as e:
                    print(f"Warning: Error building side data: {e}")
                    for i in range(1, levels_ + 1):
                        data_dict[f'bid_{i}']     = np.nan
                        data_dict[f'ask_{i}']     = np.nan
                        data_dict[f'bidsize_{i}'] = np.nan
                        data_dict[f'asksize_{i}'] = np.nan
                    data_dict['spread'] = np.nan
            else:
                for i in range(1, levels_ + 1):
                    data_dict[f'bid_{i}']     = np.nan
                    data_dict[f'ask_{i}']     = np.nan
                    data_dict[f'bidsize_{i}'] = np.nan
                    data_dict[f'asksize_{i}'] = np.nan
                data_dict['spread'] = np.nan
            return data_dict

        yes_data = _build_side(ob_yes, levels)
        no_data  = _build_side(ob_no,  levels)

        # v13 (10d): use `is not None`, not truthiness
        if current_price is not None and settlement_price is not None:
            price_diff = current_price / settlement_price - 1
        else:
            price_diff = np.nan

        for side_data in (yes_data, no_data):
            side_data['snapshot_timestamp']        = ts
            side_data['total_volume_(yes_and_no)'] = vol
            side_data['settlement_price']          = settlement_price
            side_data['current_price']             = current_price
            side_data['price_diff']                = price_diff
            side_data['time_to_expiry']            = target - now

        return {
            'YES': pd.DataFrame([yes_data]),
            'NO':  pd.DataFrame([no_data]),
            'event_id':   event['id'],
            'event_slug': event['slug'],
        }

    def _build_placeholder_snapshot(self, levels: int = 10,
                                    settlement_price=None) -> Dict:
        """
        Build a snapshot with NaN bid/ask/spread but real metadata.

        v13 (1a): used when `capture_lob_snapshot` raises, so every interval_id
        still gets an anchor row and the invariant
            every trade interval_id has a snapshot row
        keeps holding even through transient HTTP failures.
        """
        event = self.data.iloc[0]
        ts = datetime.now(timezone.utc)
        now    = ts.timestamp()
        target = event['endDate'].timestamp()

        def _empty(levels_):
            d = {}
            for i in range(1, levels_ + 1):
                d[f'bid_{i}']     = np.nan
                d[f'ask_{i}']     = np.nan
                d[f'bidsize_{i}'] = np.nan
                d[f'asksize_{i}'] = np.nan
            d['spread'] = np.nan
            return d

        yes_data = _empty(levels)
        no_data  = _empty(levels)
        for side_data in (yes_data, no_data):
            side_data['snapshot_timestamp']        = ts
            side_data['total_volume_(yes_and_no)'] = np.nan
            side_data['settlement_price']          = settlement_price
            side_data['current_price']             = np.nan
            side_data['price_diff']                = np.nan
            side_data['time_to_expiry']            = target - now

        return {
            'YES': pd.DataFrame([yes_data]),
            'NO':  pd.DataFrame([no_data]),
            'event_id':   event['id'],
            'event_slug': event['slug'],
        }

    @staticmethod
    def _extract_bounds_from_snapshot(snapshot: Dict, level: int,
                                       token_id_yes: str, token_id_no: str):
        """
        Read bid_N and ask_N from a snapshot dict to use as the next window's
        on_message price filter. Returns None if any value is NaN (placeholder
        snapshots; keep prior bounds in that case).
        """
        try:
            yes_bid = float(snapshot['YES'][f'bid_{level}'].iloc[0])
            yes_ask = float(snapshot['YES'][f'ask_{level}'].iloc[0])
            no_bid  = float(snapshot['NO'][f'bid_{level}'].iloc[0])
            no_ask  = float(snapshot['NO'][f'ask_{level}'].iloc[0])
            if any(np.isnan(v) for v in (yes_bid, yes_ask, no_bid, no_ask)):
                return None
            return {
                token_id_yes: {'min': yes_bid, 'max': yes_ask, 'side': 'YES'},
                token_id_no:  {'min': no_bid,  'max': no_ask,  'side': 'NO'},
            }
        except Exception:
            return None

    def capture_multiple_lob_snapshots(self, num_snapshots=5, interval_seconds=3,
                                        levels=10, verbose=True,
                                        settlement_price=None) -> Dict:
        """
        Capture multiple LOB snapshots over time.

        v13 (11a): `settlement_price` is now a parameter so price_diff isn't
        forced to NaN. Snapshots here carry no interval_id (11b — this helper
        is standalone, not part of the main interval-tracked flow).
        """
        if self.data is None or self.data.empty:
            raise ValueError("No active event data. Call fetch_active_event() first.")
        if self.client is None:
            raise ValueError("CLOB client not set. Call set_clob_client() first.")

        snapshots = []
        for i in range(num_snapshots):
            snapshot = self.capture_lob_snapshot(levels=levels, settlement_price=settlement_price)
            snapshots.append(snapshot)
            if verbose:
                ts = snapshot['YES']['snapshot_timestamp'].iloc[0]
                print(f"Snapshot {i+1}/{num_snapshots} captured at {ts}")
            if i < num_snapshots - 1:
                time.sleep(interval_seconds)

        all_yes = pd.concat([s['YES'] for s in snapshots], ignore_index=True)
        all_no  = pd.concat([s['NO']  for s in snapshots], ignore_index=True)
        return {
            'YES': all_yes,
            'NO':  all_no,
            'event_id':   snapshots[0]['event_id'],
            'event_slug': snapshots[0]['event_slug'],
        }

    # ----------------------------------------------------- monitoring session

    def monitor_market_activity(self, duration_seconds: int = 300,
                                 snapshot_interval: int = 30,
                                 ws_interval: int = 5,
                                 lob_levels: int = 10,
                                 verbose: bool = True) -> Dict:
        """
        Monitor market activity. Snapshot thread drives the interval boundary
        (v12 fix carries over). v13 additionally:
          - Captures LOB before advancing the interval so bounds for window N+1
            come from the same HTTP that built the snapshot — one round-trip,
            not two (1e).
          - Emits a placeholder snapshot on capture failure so the invariant
            holds even on transient errors (1a).
          - Waits on `_ws_closed_event` before final close-out so in-flight
            trades aren't orphaned (1b).
          - Deadline-based sleep, cancellable via `_stop_event` (1c, 9b).
          - try/finally around the threads so `global_interval_id` survives
            partial failures (8c).
          - Joins with timeout so a stuck WS can't block forever (9a).

        `ws_interval` is the connection-health watchdog cadence; it no longer
        drives interval bookkeeping.
        """
        if self.data is None or self.data.empty:
            raise ValueError("No active event data. Call fetch_active_event() first.")
        if self.client is None:
            raise ValueError("CLOB client not set. Call set_clob_client() first.")

        event = self.data.iloc[0]
        reference_moment = event['endDate'] - pd.Timedelta(hours=24)
        settlement_price = self.get_binance_settlement_price(reference_date=reference_moment)

        print(f"\n{'='*60}")
        print(f"Starting Market Monitoring")
        print(f"{'='*60}")
        print(f"Event: {event['title']}")
        print(f"Duration: {duration_seconds}s ({duration_seconds/60:.1f} minutes)")
        print(f"Snapshot Interval: {snapshot_interval}s")
        print(f"WS health-check cadence: {ws_interval}s")
        print(f"LOB Levels: {lob_levels}")
        print(f"{'='*60}\n")

        # Initial bounds for window 0 (replaced by the first snapshot's bounds
        # the moment that snapshot finishes capturing).
        token_bounds = get_lob_bounds(
            self.client,
            event['clob_token_id_YES'],
            event['clob_token_id_NO'],
            level=lob_levels,
        )

        results = {
            'lob_snapshots_yes': [],
            'lob_snapshots_no':  [],
            'ws_monitor': None,
            'completed': False,
        }

        url = "wss://ws-subscriptions-clob.polymarket.com"
        api_key        = self.client.derive_api_key().api_key
        api_secret     = self.client.derive_api_key().api_secret
        api_passphrase = self.client.derive_api_key().api_passphrase
        auth = {"apiKey": api_key, "secret": api_secret, "passphrase": api_passphrase}

        ws_monitor = WebSocketOrderBook(
            MARKET_CHANNEL, url, list(token_bounds.keys()), auth, None,
            verbose=False,
            token_bounds=token_bounds,
            duration_seconds=duration_seconds,
            interval_seconds=ws_interval,
            client=self.client,
            token_id_yes=event['clob_token_id_YES'],
            token_id_no=event['clob_token_id_NO'],
            lob_level=lob_levels,
            initial_interval_id=self.global_interval_id,
        )
        results['ws_monitor'] = ws_monitor

        print(f"🔢 Starting this session at global interval_id = {self.global_interval_id}")

        def websocket_thread():
            ws_monitor.run()

        def snapshot_thread():
            start_time = time.time()
            snapshot_count = 0
            first_iter = True

            while time.time() - start_time < duration_seconds and not ws_monitor.stop_flag:
                try:
                    snapshot_count += 1
                    elapsed = time.time() - start_time

                    if verbose:
                        current_for_log = ws_monitor.current_interval_id
                        expected_id = current_for_log if first_iter else current_for_log + 1
                        print(f"[{elapsed:.0f}s] Capturing LOB snapshot #{snapshot_count} "
                              f"(target interval_id={expected_id})...")

                    # v13 (1a): capture LOB first; on failure emit placeholder.
                    try:
                        snapshot = self.capture_lob_snapshot(
                            levels=lob_levels, settlement_price=settlement_price
                        )
                    except Exception as capture_err:
                        print(f"Warning: snapshot capture failed: {capture_err}; "
                              f"emitting placeholder so the invariant holds")
                        snapshot = self._build_placeholder_snapshot(
                            levels=lob_levels, settlement_price=settlement_price
                        )

                    # v13 (1e): bounds for the next window come from the SAME
                    # HTTP we just made — no second round-trip.
                    new_bounds = self._extract_bounds_from_snapshot(
                        snapshot, lob_levels,
                        event['clob_token_id_YES'], event['clob_token_id_NO'],
                    )

                    # v13: atomic boundary. Finalize prior window's summary
                    # with OLD bounds, then apply NEW bounds and advance id
                    # — all under id_lock so on_message sees a consistent
                    # (counters, bounds, id) tuple.
                    if first_iter:
                        with ws_monitor.id_lock:
                            if new_bounds:
                                ws_monitor.token_bounds.update(new_bounds)
                            current_id = ws_monitor.current_interval_id
                    else:
                        _finalized, current_id = ws_monitor.advance_interval(new_bounds=new_bounds)

                    snapshot['YES']['interval_id']     = current_id
                    snapshot['NO']['interval_id']      = current_id
                    snapshot['YES']['elapsed_seconds'] = elapsed
                    snapshot['NO']['elapsed_seconds']  = elapsed
                    results['lob_snapshots_yes'].append(snapshot['YES'])
                    results['lob_snapshots_no'].append(snapshot['NO'])

                    first_iter = False

                    # v13 (1c): deadline-based sleep so cumulative period
                    # doesn't drift past snapshot_interval * snapshot_count.
                    next_deadline = start_time + snapshot_interval * snapshot_count
                    now = time.time()
                    sleep_for = max(0.0, next_deadline - now)
                    remaining = duration_seconds - (now - start_time)
                    sleep_for = min(sleep_for, max(0.0, remaining))
                    if sleep_for > 0:
                        if ws_monitor._stop_event.wait(timeout=sleep_for):
                            break  # stop requested
                except Exception as e:
                    print(f"Error in snapshot iteration #{snapshot_count}: {e}")
                    ws_monitor._stop_event.wait(timeout=min(snapshot_interval, 30))

            # v13 (1b): final close-out — wait briefly for WS to drain so
            # in-flight trades are counted in the current window, not
            # orphaned. Then close the last open window.
            if not first_iter:
                ws_monitor._ws_closed_event.wait(timeout=15)
                try:
                    ws_monitor.advance_interval()
                except Exception as e:
                    print(f"Warning: final close-out failed: {e}")

            results['completed'] = True
            if verbose:
                print(f"\n[{duration_seconds}s] LOB snapshot collection completed.")

        ws_thread   = threading.Thread(target=websocket_thread,   name='ws-thread')
        snap_thread = threading.Thread(target=snapshot_thread,    name='snap-thread')

        try:
            ws_thread.start()
            snap_thread.start()
            # v13 (9a): bounded joins — a stuck thread doesn't hang the
            # session forever.
            join_grace = max(30, snapshot_interval * 2)
            snap_thread.join(timeout=duration_seconds + join_grace)
            if snap_thread.is_alive():
                print("⚠️ snap_thread did not exit within grace; requesting stop")
                ws_monitor.stop_flag = True
                ws_monitor._stop_event.set()
            ws_thread.join(timeout=duration_seconds + join_grace)
            if ws_thread.is_alive():
                print("⚠️ ws_thread did not exit within grace; abandoning to GC")
        finally:
            # v13 (8c): always carry global_interval_id forward, even if the
            # session crashed partway. Subtract 1 from any pending un-finalized
            # id since advance_interval has already moved it past.
            self.global_interval_id = ws_monitor.current_interval_id

        print(f"🔢 Session ended at global interval_id = {self.global_interval_id - 1}; "
              f"next session will start at {self.global_interval_id}")

        lob_yes = pd.concat(results['lob_snapshots_yes'], ignore_index=True) \
                  if results['lob_snapshots_yes'] else pd.DataFrame()
        lob_no  = pd.concat(results['lob_snapshots_no'],  ignore_index=True) \
                  if results['lob_snapshots_no']  else pd.DataFrame()
        trade_dfs   = ws_monitor.get_trade_dataframes()
        interval_df = ws_monitor.get_interval_dataframe()

        try:
            self._assert_interval_invariant(lob_yes, trade_dfs, interval_df)
        except Exception as e:
            print(f"⚠️ Interval invariant check raised: {e}")

        summary = self._calculate_summary(
            lob_yes, lob_no,
            ws_monitor.total_price_change_counts,
            trade_dfs, duration_seconds,
        )

        print(f"\n{'='*60}")
        print(f"Monitoring Complete - Summary")
        print(f"{'='*60}")
        print(f"Duration: {duration_seconds}s")
        print(f"LOB Snapshots: {len(lob_yes)}")
        print(f"WebSocket Intervals: {len(interval_df)}")
        print(f"Price Changes: YES={ws_monitor.total_price_change_counts['YES']} "
              f"NO={ws_monitor.total_price_change_counts['NO']}")
        print(f"Trades: YES={len(trade_dfs['YES'])} NO={len(trade_dfs['NO'])}")
        if not trade_dfs['YES'].empty:
            print(f"  YES Volume: ${trade_dfs['YES']['size'].sum():.2f}")
        if not trade_dfs['NO'].empty:
            print(f"  NO  Volume: ${trade_dfs['NO']['size'].sum():.2f}")
        print(f"{'='*60}\n")

        return {
            'lob_snapshots': {'YES': lob_yes, 'NO': lob_no},
            'price_changes': ws_monitor.total_price_change_counts,
            'trades':        trade_dfs,
            'intervals':     interval_df,
            'summary':       summary,
            'event_info': {
                'id':    event['id'],
                'slug':  event['slug'],
                'title': event['title'],
            },
        }

    def _assert_interval_invariant(self, lob_yes, trade_dfs, interval_df):
        """Log-only check: per-interval trade counts match summary, and every
        trade has a snapshot anchor."""
        if interval_df is None or interval_df.empty:
            print("ℹ️ Invariant check skipped: no interval rows.")
            return

        summary = interval_df.set_index('interval_id')

        def _counts(df):
            if df is None or df.empty or 'interval_id' not in df.columns:
                return pd.Series(dtype=int)
            return df.groupby('interval_id').size()

        ids_yes = _counts(trade_dfs.get('YES'))
        ids_no  = _counts(trade_dfs.get('NO'))
        mis_yes = (ids_yes - summary['yes_trade_count']).fillna(0)
        mis_no  = (ids_no  - summary['no_trade_count']).fillna(0)

        bad_yes = mis_yes[mis_yes != 0]
        bad_no  = mis_no [mis_no  != 0]
        if not bad_yes.empty:
            print(f"❌ YES trade/interval mismatch: {bad_yes.to_dict()}")
        if not bad_no.empty:
            print(f"❌ NO  trade/interval mismatch: {bad_no.to_dict()}")

        snap_ids = set(lob_yes['interval_id']) if (lob_yes is not None and not lob_yes.empty
                                                   and 'interval_id' in lob_yes.columns) else set()
        trade_ids = set(ids_yes.index) | set(ids_no.index)
        orphans = sorted(trade_ids - snap_ids)
        if orphans:
            print(f"❌ Trades without snapshot anchor: {orphans}")

        if bad_yes.empty and bad_no.empty and not orphans:
            print("✅ Interval invariant holds: trade counts match summaries and every "
                  "trade interval_id has a snapshot anchor.")

    def _calculate_summary(self, lob_yes, lob_no, price_changes, trade_dfs, duration):
        summary = {
            'duration_seconds':    duration,
            'lob_snapshot_count':  len(lob_yes),
            'price_changes_total': sum(price_changes.values()),
            'trades_total':        len(trade_dfs['YES']) + len(trade_dfs['NO']),
        }
        if not lob_yes.empty:
            summary['yes_avg_spread']        = lob_yes['spread'].mean()
            summary['yes_min_spread']        = lob_yes['spread'].min()
            summary['yes_max_spread']        = lob_yes['spread'].max()
            summary['yes_spread_volatility'] = lob_yes['spread'].std()
        if not lob_no.empty:
            summary['no_avg_spread']        = lob_no['spread'].mean()
            summary['no_min_spread']        = lob_no['spread'].min()
            summary['no_max_spread']        = lob_no['spread'].max()
            summary['no_spread_volatility'] = lob_no['spread'].std()
        if not trade_dfs['YES'].empty:
            summary['yes_trade_volume'] = trade_dfs['YES']['size'].sum()
            summary['yes_vwap'] = ((trade_dfs['YES']['price'] * trade_dfs['YES']['size']).sum()
                                   / trade_dfs['YES']['size'].sum())
            summary['yes_buy_volume']  = trade_dfs['YES'][trade_dfs['YES']['side'] == 'BUY']['size'].sum()
            summary['yes_sell_volume'] = trade_dfs['YES'][trade_dfs['YES']['side'] == 'SELL']['size'].sum()
        if not trade_dfs['NO'].empty:
            summary['no_trade_volume'] = trade_dfs['NO']['size'].sum()
            summary['no_vwap'] = ((trade_dfs['NO']['price'] * trade_dfs['NO']['size']).sum()
                                  / trade_dfs['NO']['size'].sum())
            summary['no_buy_volume']  = trade_dfs['NO'][trade_dfs['NO']['side'] == 'BUY']['size'].sum()
            summary['no_sell_volume'] = trade_dfs['NO'][trade_dfs['NO']['side'] == 'SELL']['size'].sum()
        return summary

    # ------------------------------------------- continuous cycle + persistence

    def _global_id_path(self, output_dir: str) -> str:
        return os.path.join(output_dir, '.global_interval_id')

    def _load_global_interval_id(self, output_dir: str):
        """v13 (8b/14a): rehydrate global_interval_id from sidecar so master CSVs
        keep monotonically-increasing ids across process restarts."""
        path = self._global_id_path(output_dir)
        if not os.path.exists(path):
            print(f"ℹ️ No persisted global_interval_id at {path}; starting at "
                  f"{self.global_interval_id}.")
            return
        try:
            with open(path, 'r') as f:
                loaded = int(f.read().strip())
            if loaded < 1:
                print(f"⚠️ Persisted global_interval_id={loaded} < 1; ignoring.")
                return
            self.global_interval_id = loaded
            print(f"✅ Loaded global_interval_id={loaded} from {path}.")
        except Exception as e:
            print(f"⚠️ Failed to load global_interval_id from {path}: {e}")

    def _save_global_interval_id(self, output_dir: str):
        """Persist global_interval_id under a file lock."""
        path = self._global_id_path(output_dir)
        try:
            os.makedirs(output_dir, exist_ok=True)
            with _file_lock(path):
                with open(path, 'w') as f:
                    f.write(str(self.global_interval_id))
        except Exception as e:
            print(f"⚠️ Failed to save global_interval_id to {path}: {e}")

    def run_continuous_cycle_seconds(self, total_seconds: int,
                                      snapshot_interval: int = 30,
                                      ws_interval: int = 30,
                                      lob_levels: int = 10,
                                      output_dir: str = 'data',
                                      verbose: bool = False):
        """
        Run monitoring continuously for a total number of seconds.

        v13 changes:
          - Loads / saves `global_interval_id` to `<output_dir>/.global_interval_id`
            so ids stay unique across process restarts (8b/14a).
          - Inner event-wait loop respects the total budget (8a).
          - Transition sleep is bounded by the remaining budget (8e).
          - Duration check reordered so we don't compute negative durations (8d).
        """
        os.makedirs(output_dir, exist_ok=True)
        self._load_global_interval_id(output_dir)

        print(f"\n🚀 Starting Continuous Monitoring Cycle for {total_seconds} seconds")
        print(f"🔄 Event rollovers driven by Polymarket-provided endDate.")

        start_time = datetime.now(timezone.utc)

        def _remaining():
            return total_seconds - (datetime.now(timezone.utc) - start_time).total_seconds()

        while True:
            remaining_budget = _remaining()
            if remaining_budget <= 0:
                print("\n⏰ Total time budget reached. Stopping continuous cycle.")
                break

            # PHASE 1: fetch active event — v13 (8a): budget-aware retry
            print("\n🔍 Fetching active event...")
            while True:
                if _remaining() <= 0:
                    print("⏰ Budget exhausted while waiting for an active event.")
                    return
                try:
                    self.fetch_active_event()
                    event = self.data.iloc[0]
                    print(f"✅ Active Event Found: {event['slug']}")
                    print(f"   ↳ endDate (from Polymarket): {event['endDate']}")
                    break
                except Exception as e:
                    sleep_for = min(10, max(1, _remaining()))
                    print(f"⏳ Waiting for new event to appear... ({e}); "
                          f"retrying in {sleep_for:.0f}s")
                    time.sleep(sleep_for)

            # PHASE 2: compute session duration — v13 (8d): reordered
            now            = datetime.now(timezone.utc)
            event_end      = event['endDate'].to_pydatetime()
            seconds_to_end = (event_end - now).total_seconds() - 10  # buffer

            if seconds_to_end < 60:
                wait_for = min(30, max(1, _remaining()))
                print(f"⏳ Too close to event cutoff ({seconds_to_end:.0f}s). "
                      f"Waiting {wait_for:.0f}s and retrying...")
                time.sleep(wait_for)
                continue

            duration_seconds = int(min(seconds_to_end, _remaining()))
            if duration_seconds < 60:
                wait_for = min(30, max(1, _remaining()))
                print(f"⏳ Remaining budget {duration_seconds}s < 60s. "
                      f"Waiting {wait_for:.0f}s and retrying...")
                time.sleep(wait_for)
                continue

            print(f"⏱️ This session duration: {duration_seconds}s "
                  f"({duration_seconds/3600:.2f} hours); "
                  f"budget remaining after: {(_remaining()-duration_seconds)/3600:.2f} hours")

            # PHASE 3: run one monitoring session
            results = None
            try:
                results = self.monitor_market_activity(
                    duration_seconds=duration_seconds,
                    snapshot_interval=snapshot_interval,
                    ws_interval=ws_interval,
                    lob_levels=lob_levels,
                    verbose=verbose,
                )
            except Exception as e:
                print(f"❌ Error during monitoring session: {e}")
            finally:
                # v13 (8b/8c): persist whatever id we advanced to, even if the
                # session itself errored.
                self._save_global_interval_id(output_dir)

            # PHASE 4: export
            if results is not None:
                try:
                    self.export_monitoring_results(results, output_dir=output_dir)
                except Exception as e:
                    print(f"❌ Error exporting results: {e}")

            # PHASE 5: transition — v13 (8e): bounded by remaining budget
            transition = min(60, max(0, _remaining()))
            if transition > 0:
                print(f"\n🔄 Session finished. Transition pause: {transition:.0f}s")
                time.sleep(transition)
            self.data = None

    # ---------------------------------------------------------------- exports

    def export_monitoring_results(self, results: Dict, output_dir: str = 'data'):
        """Per-event Excel files + cross-event master CSV appends."""
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        event_slug  = results['event_info']['slug']
        event_id    = results['event_info']['id']
        event_title = results['event_info'].get('title', '')

        def _sorted_by(df: pd.DataFrame, cols) -> pd.DataFrame:
            if df is None or df.empty:
                return df
            use = [c for c in cols if c in df.columns]
            if not use:
                return df
            return df.sort_values(by=use, kind='mergesort').reset_index(drop=True)

        lob_yes = results['lob_snapshots']['YES'].copy() if not results['lob_snapshots']['YES'].empty else pd.DataFrame()
        lob_no  = results['lob_snapshots']['NO'].copy()  if not results['lob_snapshots']['NO'].empty  else pd.DataFrame()

        if not lob_yes.empty:
            lob_yes['side']        = 'YES'
            lob_yes['event_id']    = event_id
            lob_yes['event_slug']  = event_slug
            lob_yes['event_title'] = event_title
            lob_yes = _sorted_by(lob_yes, ['interval_id'])
        if not lob_no.empty:
            lob_no['side']        = 'NO'
            lob_no['event_id']    = event_id
            lob_no['event_slug']  = event_slug
            lob_no['event_title'] = event_title
            lob_no = _sorted_by(lob_no, ['interval_id'])

        trades_yes = results['trades']['YES'].copy() if not results['trades']['YES'].empty else pd.DataFrame()
        trades_no  = results['trades']['NO'].copy()  if not results['trades']['NO'].empty  else pd.DataFrame()
        if not trades_yes.empty:
            trades_yes['side_token'] = 'YES'
            trades_yes['event_id']   = event_id
            trades_yes['event_slug'] = event_slug
            trades_yes = _sorted_by(trades_yes, ['interval_id', 'timestamp'])
        if not trades_no.empty:
            trades_no['side_token']  = 'NO'
            trades_no['event_id']    = event_id
            trades_no['event_slug']  = event_slug
            trades_no = _sorted_by(trades_no, ['interval_id', 'timestamp'])

        intervals_df = results['intervals'].copy() if not results['intervals'].empty else pd.DataFrame()
        if not intervals_df.empty:
            intervals_df['event_id']   = event_id
            intervals_df['event_slug'] = event_slug
            intervals_df = _sorted_by(intervals_df, ['interval_id'])

        def _write_xlsx(df, path, label):
            if df is None or df.empty:
                return
            self._strip_tz_for_excel(df).to_excel(path, index=False)
            print(f"✓ Saved {label:<14} → {path} ({len(df)} rows)")

        _write_xlsx(lob_yes,      f"{output_dir}/{event_slug}_snapshots_YES_{timestamp}.xlsx", "YES snapshots")
        _write_xlsx(lob_no,       f"{output_dir}/{event_slug}_snapshots_NO_{timestamp}.xlsx",  "NO snapshots")
        _write_xlsx(trades_yes,   f"{output_dir}/{event_slug}_trades_YES_{timestamp}.xlsx",    "YES trades")
        _write_xlsx(trades_no,    f"{output_dir}/{event_slug}_trades_NO_{timestamp}.xlsx",     "NO trades")
        _write_xlsx(intervals_df, f"{output_dir}/{event_slug}_intervals_{timestamp}.xlsx",     "intervals")

        # v13 (13c): use the wrapper so summary respects the empty/tz contract
        summary_df = pd.DataFrame([results['summary']])
        summary_df['event_id']   = event_id
        summary_df['event_slug'] = event_slug
        _write_xlsx(summary_df, f"{output_dir}/{event_slug}_summary_{timestamp}.xlsx", "summary")

        # Master CSVs — v13 (13a/13b): file locked + warn on column drift
        self._append_to_master(os.path.join(output_dir, 'master_snapshots_YES_full.csv'), lob_yes,      label='snapshots-YES')
        self._append_to_master(os.path.join(output_dir, 'master_snapshots_NO_full.csv'),  lob_no,       label='snapshots-NO')
        self._append_to_master(os.path.join(output_dir, 'master_trades_YES_full.csv'),    trades_yes,   label='trades-YES')
        self._append_to_master(os.path.join(output_dir, 'master_trades_NO_full.csv'),     trades_no,    label='trades-NO')
        self._append_to_master(os.path.join(output_dir, 'master_intervals_full.csv'),     intervals_df, label='intervals')

        print(f"\n✓ All data exported to '{output_dir}/' directory\n")

    @staticmethod
    def _strip_tz_for_excel(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        out = df.copy()
        for col in out.columns:
            s = out[col]
            if pd.api.types.is_datetime64_any_dtype(s) and getattr(s.dt, 'tz', None) is not None:
                out[col] = s.dt.tz_convert('UTC').dt.tz_localize(None)
        return out

    @staticmethod
    def _align_datetime_tz_to_existing(path: str, new_df: pd.DataFrame) -> pd.DataFrame:
        """
        Patch B2: when appending to an existing master CSV, coerce datetime
        columns in `new_df` to match the tz-state of the existing rows.

        Old (v11/v12) master CSVs have naive `snapshot_timestamp` and
        intervals' `timestamp` (written as `datetime.now()`). v13 produces
        tz-aware UTC for those columns. Without alignment, the column ends
        up with mixed ISO-8601 dialects ("2026-06-20 12:00:00" vs
        "2026-06-20 12:00:00+00:00") and downstream parsers break.

        Strategy: sniff the FIRST non-null value in each shared column from
        the existing file. If it looks naive (no '+' offset and no 'Z'),
        strip the tz from `new_df`'s column. If it looks tz-aware (or if
        the column is empty / unreadable), leave `new_df` alone.

        Never adds a tz to existing naive rows — that's the user's choice
        to migrate via a separate one-shot script if they want.
        """
        if new_df is None or new_df.empty:
            return new_df
        if not (os.path.exists(path) and os.path.getsize(path) > 0):
            return new_df
        try:
            sample = pd.read_csv(path, nrows=1, parse_dates=False)
        except Exception:
            return new_df
        if sample.empty:
            return new_df

        out = new_df  # copy-on-modify
        for col in new_df.columns:
            if col not in sample.columns:
                continue
            new_series = new_df[col]
            # We only care about columns that are tz-aware in the new data.
            if not pd.api.types.is_datetime64_any_dtype(new_series):
                continue
            if getattr(new_series.dt, 'tz', None) is None:
                continue
            existing_val = sample[col].iloc[0]
            if not isinstance(existing_val, str):
                # Number, NaN, or non-string — can't infer tz from this.
                continue
            # ISO 8601 tz-aware ends in 'Z' or has '+HH:MM'/'-HH:MM' as the
            # final 6 chars (e.g. "...+00:00"). Plain date dashes (YYYY-MM-DD)
            # are not at this position, so this avoids the false-positive on
            # naive timestamps.
            s = existing_val.strip()
            existing_has_tz = (
                s.endswith('Z')
                or (len(s) >= 6 and s[-6] in '+-' and s[-3] == ':')
            )
            if existing_has_tz:
                continue
            # Existing is naive — strip tz from the new rows so the column
            # stays single-format in the master file.
            if out is new_df:
                out = new_df.copy()
            out[col] = new_series.dt.tz_convert('UTC').dt.tz_localize(None)
            print(f"ℹ️ Master {os.path.basename(path)}: stripping tz from '{col}' "
                  f"to match naive timestamps already in the file.")
        return out

    def _append_to_master(self, path: str, new_df: pd.DataFrame, label: str = ''):
        """
        v13 (13a): warns on schema drift (new columns silently dropped).
        v13 (13b): file-locked append so concurrent processes don't interleave rows.
        Patch B2:  aligns tz-state of datetime columns to existing rows so
                   v13 appends to pre-v13 master CSVs without mixed ISO dialects.
        """
        if new_df is None or new_df.empty:
            return
        try:
            with _file_lock(path):
                file_exists = os.path.exists(path) and os.path.getsize(path) > 0
                if file_exists:
                    try:
                        existing_cols = pd.read_csv(path, nrows=0).columns.tolist()
                        new_cols = [c for c in new_df.columns if c not in existing_cols]
                        if new_cols:
                            print(f"⚠️ Master {label}: dropping {len(new_cols)} new column(s) "
                                  f"not in existing header: {new_cols}")
                        for col in existing_cols:
                            if col not in new_df.columns:
                                new_df[col] = np.nan
                        new_df = new_df[existing_cols]
                    except Exception as e:
                        print(f"⚠️ Could not read header of {path} for alignment: {e}")

                    # Patch B2: align tz before writing.
                    new_df = self._align_datetime_tz_to_existing(path, new_df)

                    new_df.to_csv(path, mode='a', header=False, index=False)
                    print(f"✓ Appended {len(new_df)} rows → master {label}: {path}")
                else:
                    new_df.to_csv(path, mode='w', header=True, index=False)
                    print(f"✓ Created master {label}: {path} ({len(new_df)} rows)")
        except Exception as e:
            print(f"❌ Failed to append to master {label} file {path}: {e}")


# ============================================================================
# WebSocketOrderBook
# ============================================================================

class WebSocketOrderBook:
    """
    v13 changes vs. v12:
      - Helper threads (ping, watchdog, timer) start once per session in
        `run()`, NOT inside on_open — eliminates the
        `if self.reconnect_count == 0` guard that disabled them when the
        first connect failed before on_open (3a/5a/6a).
      - `run()` is now an outer reconnection loop with exponential backoff
        and jitter. `on_close` no longer calls `run_forever()` recursively,
        so 1000 reconnects don't blow the stack (4a). Exhausting attempts
        sets stop_flag so the snapshot thread exits (4b).
      - `reconnect_count` resets after a stable uptime (4d).
      - `_stop_event` makes ping/watchdog/timer/snapshot sleeps cancellable
        (5b, 6b).
      - on_message dedups trades on (asset_id, ts, price, size, side) so
        WS redeliveries after reconnect don't double-count (2a). The price /
        bounds / counter / id reads happen under one id_lock acquisition, so
        bounds-and-id mismatch races are also closed (2c).
      - `advance_interval(new_bounds=...)` atomically applies new bounds
        with the id advance (folds 1e into the existing v12 lock).
      - `health_threshold_seconds` is parameterizable (3b).
    """

    def __init__(self, channel_type, url, data, auth, message_callback, verbose,
                 token_bounds, duration_seconds=None, interval_seconds=5,
                 client=None, token_id_yes=None, token_id_no=None, lob_level=10,
                 initial_interval_id=1, health_threshold_seconds=60,
                 reconnect_count_reset_uptime=600,
                 seen_trade_maxlen=10000):
        self.channel_type      = channel_type
        self.url               = url
        self.data              = data
        self.auth              = auth
        self.message_callback  = message_callback
        self.verbose           = verbose
        self.token_bounds      = token_bounds
        self.duration_seconds  = duration_seconds
        self.interval_seconds  = interval_seconds
        self.start_time        = None
        self.stop_flag         = False

        # v13: cancellable sleeps
        self._stop_event       = threading.Event()
        # v13 (1b): set when run()'s connect loop exits, so snapshot thread
        # can wait for trade drain before the final close-out.
        self._ws_closed_event  = threading.Event()

        # v12 lock guarding (counters, current_interval_id, trades buffer,
        # bounds). v13 also covers the bounds-swap in advance_interval.
        self.id_lock = threading.Lock()
        self.current_interval_id = initial_interval_id

        self.client       = client
        self.token_id_yes = token_id_yes
        self.token_id_no  = token_id_no
        self.lob_level    = lob_level

        # Reconnection settings
        self.max_reconnect_attempts        = 1000
        self.reconnect_delay               = 5
        self.connection_lost               = False
        self.reconnect_count               = 0
        self.reconnect_count_reset_uptime  = reconnect_count_reset_uptime

        # v13 (3b): connection-health threshold parameterized
        self.health_threshold_seconds = health_threshold_seconds

        self.current_interval_price_changes = {'YES': 0, 'NO': 0}
        self.current_interval_trades        = {'YES': 0, 'NO': 0}
        self.interval_history               = []
        self.trades                         = {'YES': [], 'NO': []}
        self.total_price_change_counts      = {'YES': 0, 'NO': 0}

        # v13 (2a): LRU of trade-event fingerprints for reconnect dedup.
        # Single-threaded access (websocket-client serializes on_message),
        # so no lock is needed here.
        self._seen_trade_keys   = collections.OrderedDict()
        self._seen_trade_maxlen = seen_trade_maxlen

        # WS is (re)created inside run()'s connect loop, not in __init__.
        self.ws = None
        self.orderbooks = {}
        self.interval_thread = None
        self.last_message_time = time.time()
        self._last_open_time   = None

    # -------------------------------------------------------- bounds (legacy)

    def update_bounds(self):
        """
        v13: kept for backwards compatibility. The v13 main flow drives bounds
        through `advance_interval(new_bounds=...)` so the same HTTP that built
        the snapshot also supplies the next window's filter bounds. This
        method still works for ad-hoc callers — it just costs an extra
        round-trip.
        """
        if not self.client or not self.token_id_yes or not self.token_id_no:
            return
        try:
            ob_yes = _call_with_timeout(self.client.get_order_book, 15, self.token_id_yes)
            ob_no  = _call_with_timeout(self.client.get_order_book, 15, self.token_id_no)

            def _bid_at(ob, level_):
                if ob.bids and len(ob.bids) >= level_:
                    return float(ob.bids[-level_].price)
                return float(ob.bids[-1].price) if ob.bids else 0.0

            def _ask_at(ob, level_):
                if ob.asks and len(ob.asks) >= level_:
                    return float(ob.asks[-level_].price)
                return float(ob.asks[-1].price) if ob.asks else 1.0

            yes_bid = _bid_at(ob_yes, self.lob_level)
            yes_ask = _ask_at(ob_yes, self.lob_level)
            no_bid  = _bid_at(ob_no,  self.lob_level)
            no_ask  = _ask_at(ob_no,  self.lob_level)

            with self.id_lock:
                self.token_bounds[self.token_id_yes] = {'min': yes_bid, 'max': yes_ask, 'side': 'YES'}
                self.token_bounds[self.token_id_no]  = {'min': no_bid,  'max': no_ask,  'side': 'NO'}

            if self.verbose:
                print(f"\n[Bounds Updated] YES: {yes_bid:.4f} - {yes_ask:.4f}  "
                      f"NO: {no_bid:.4f} - {no_ask:.4f}")
        except Exception as e:
            print(f"Warning: Failed to update bounds: {e}")

    # --------------------------------------------------------------- messages

    def on_message(self, ws, message):
        """Handle incoming WebSocket messages."""
        self.last_message_time = time.time()
        if message == "PONG":
            return

        try:
            data = json.loads(message)
            event_type = data.get('event_type')

            if event_type == 'price_change':
                price_changes = data.get('price_changes', [])
                if not isinstance(price_changes, list):
                    return
                for change in price_changes:
                    if not isinstance(change, dict):
                        continue
                    asset_id = change.get('asset_id')
                    if asset_id not in self.token_bounds:
                        continue
                    try:
                        price = float(change.get('price', 0))
                    except (TypeError, ValueError):
                        continue
                    # v13 (2c): read bounds, increment counters all under one
                    # lock so the (bounds, id, counter) triple is consistent
                    # with what advance_interval atomically swaps.
                    with self.id_lock:
                        bounds = self.token_bounds.get(asset_id)
                        if not bounds:
                            continue
                        min_price = bounds['min']
                        max_price = bounds['max']
                        side      = bounds['side']
                        if min_price <= price <= max_price:
                            self.current_interval_price_changes[side] += 1
                            self.total_price_change_counts[side]      += 1
                            recorded_count = self.current_interval_price_changes[side]
                        else:
                            recorded_count = None
                    if recorded_count is not None and self.verbose:
                        print(f"\n[{side}] Price change (Interval: {recorded_count}) "
                              f"price={price} range=[{min_price}, {max_price}]")

            elif event_type == 'last_trade_price':
                asset_id = data.get('asset_id')
                if asset_id not in self.token_bounds:
                    return

                # v13 (2a): dedup on (asset_id, ts, price, size, side). WS
                # redeliveries after a reconnect would otherwise inflate the
                # trade list and per-window counters.
                ts_raw  = data.get('timestamp')
                pr_raw  = data.get('price')
                sz_raw  = data.get('size')
                sd_raw  = data.get('side')
                dedup_key = (str(asset_id), str(ts_raw), str(pr_raw),
                             str(sz_raw), str(sd_raw))
                if dedup_key in self._seen_trade_keys:
                    self._seen_trade_keys.move_to_end(dedup_key)
                    if self.verbose:
                        print(f"⚠️ Skipping duplicate trade {dedup_key}")
                    return
                self._seen_trade_keys[dedup_key] = None
                if len(self._seen_trade_keys) > self._seen_trade_maxlen:
                    self._seen_trade_keys.popitem(last=False)

                # v13 (2d): cast fee_rate_bps to float
                try:
                    fee_bps = float(data.get('fee_rate_bps', 0) or 0)
                except (TypeError, ValueError):
                    fee_bps = 0.0

                with self.id_lock:
                    side = self.token_bounds[asset_id]['side']
                    self.current_interval_trades[side] += 1
                    trade_info = {
                        'interval_id': self.current_interval_id,
                        'asset_id':    asset_id,
                        'price':       float(pr_raw) if pr_raw is not None else np.nan,
                        'size':        float(sz_raw) if sz_raw is not None else np.nan,
                        'side':        sd_raw,
                        'timestamp':   ts_raw,
                        'fee_rate_bps': fee_bps,
                    }
                    self.trades[side].append(trade_info)
                    recorded_count = self.current_interval_trades[side]

                if self.verbose:
                    print(f"\n[{side}] Trade (Interval: {recorded_count}) "
                          f"price={trade_info['price']} size={trade_info['size']}")

        except json.JSONDecodeError:
            pass
        except Exception as e:
            if self.verbose:
                print(f"Error processing message: {e}")

    # ----------------------------------------------- interval boundary control

    def advance_interval(self, new_bounds: Optional[Dict] = None):
        """
        Atomically: finalize current window's summary using OLD bounds, apply
        NEW bounds (if provided) for the next window, advance current_interval_id.
        Returns (finalized_id, new_id).
        """
        timestamp = datetime.now(timezone.utc)
        with self.id_lock:
            # Read OLD bounds BEFORE applying the new ones, so the summary row
            # reflects the bounds that were in effect during the window.
            yes_bounds = self.token_bounds.get(self.token_id_yes, {})
            no_bounds  = self.token_bounds.get(self.token_id_no, {})

            finalized_id = self.current_interval_id
            interval_data = {
                'interval_id':         finalized_id,
                'interval_number':     len(self.interval_history) + 1,
                'timestamp':           timestamp,
                'yes_price_changes':   self.current_interval_price_changes['YES'],
                'no_price_changes':    self.current_interval_price_changes['NO'],
                'total_price_changes': (self.current_interval_price_changes['YES']
                                        + self.current_interval_price_changes['NO']),
                'yes_trade_count':     self.current_interval_trades['YES'],
                'no_trade_count':      self.current_interval_trades['NO'],
                'total_trade_count':   (self.current_interval_trades['YES']
                                        + self.current_interval_trades['NO']),
                'yes_bound_min':       yes_bounds.get('min', np.nan),
                'yes_bound_max':       yes_bounds.get('max', np.nan),
                'no_bound_min':        no_bounds.get('min', np.nan),
                'no_bound_max':        no_bounds.get('max', np.nan),
                'reconnect_count':     self.reconnect_count,
            }
            self.interval_history.append(interval_data)

            self.current_interval_price_changes = {'YES': 0, 'NO': 0}
            self.current_interval_trades        = {'YES': 0, 'NO': 0}

            # v13: apply new bounds inside the same critical section as the
            # id increment, so on_message never observes (new id, old bounds)
            # or (old id, new bounds).
            if new_bounds:
                self.token_bounds.update(new_bounds)

            self.current_interval_id += 1
            new_id = self.current_interval_id

        if self.verbose:
            print(f"\n[Interval finalized id={finalized_id} → next id={new_id}] "
                  f"yes_trades={interval_data['yes_trade_count']} "
                  f"no_trades={interval_data['no_trade_count']}")
        return finalized_id, new_id

    # ----------------------------------------------------- background threads

    def connection_health_watchdog(self):
        """Watch for dead WS; force close to trigger run()'s reconnect loop."""
        while not self.stop_flag:
            if self._stop_event.wait(timeout=self.interval_seconds):
                break
            if self.stop_flag:
                break
            if time.time() - self.last_message_time > self.health_threshold_seconds:
                print(f"\n⚠️ No WS messages for {self.health_threshold_seconds}s, "
                      f"forcing reconnect")
                self.connection_lost = True
                ws = self.ws
                if ws is not None:
                    try:
                        ws.close()
                    except Exception:
                        pass

    def timer(self):
        """v13 (5b): cancellable via _stop_event."""
        if self._stop_event.wait(timeout=self.duration_seconds):
            return
        print(f"\n\n⏰ Time limit reached ({self.duration_seconds}s). "
              f"Closing connection...")
        self.stop_flag = True
        self._stop_event.set()
        ws = self.ws
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

    def ping(self):
        """v13 (6b): reads self.ws each iteration (so it works across reconnects);
        cancellable via _stop_event."""
        while not self.stop_flag:
            try:
                ws = self.ws
                if ws is not None:
                    ws.send("PING")
            except Exception:
                pass  # transient; run()'s reconnect loop will recover
            if self._stop_event.wait(timeout=10):
                break

    # --------------------------------------------------------- WS event hooks

    def on_error(self, ws, error):
        print(f"❌ WebSocket Error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        """v13 (4a): no longer drives reconnection. Just logs; the outer
        loop in run() decides whether to retry."""
        print(f"\n⚠️ WebSocket connection closed: status={close_status_code} "
              f"msg={close_msg}")

    def on_open(self, ws):
        """Subscribe to the channel. v13: thread starts moved to run()."""
        if self.start_time is None:
            self.start_time = time.time()
        self._last_open_time   = time.time()
        self.last_message_time = time.time()
        self.connection_lost   = False

        if self.reconnect_count > 0:
            print(f"✅ WebSocket reconnected successfully (attempt #{self.reconnect_count})")

        if self.channel_type == MARKET_CHANNEL:
            ws.send(json.dumps({
                "assets_ids": list(self.token_bounds.keys()),
                "type": MARKET_CHANNEL,
            }))
            ws.send(json.dumps({
                "assets_ids": list(self.token_bounds.keys()),
                "operation": "subscribe",
            }))
        elif self.channel_type == USER_CHANNEL and self.auth:
            ws.send(json.dumps({
                "markets": self.data,
                "type":    USER_CHANNEL,
                "auth":    self.auth,
            }))

    # ---------------------------------------------------------- run + connect

    def _start_session_threads(self):
        """v13 (3a/5a/6a): start once per session, BEFORE any WS connect attempt,
        so a failure pre-on_open doesn't disable health/timer/ping."""
        ping_thread = threading.Thread(target=self.ping, name='ws-ping', daemon=True)
        ping_thread.start()

        self.interval_thread = threading.Thread(
            target=self.connection_health_watchdog, name='ws-watchdog', daemon=True
        )
        self.interval_thread.start()

        if self.duration_seconds:
            timer_thread = threading.Thread(target=self.timer, name='ws-timer', daemon=True)
            timer_thread.start()

    def run(self):
        """
        v13: outer reconnection loop. Non-recursive (no `run_forever` inside
        `on_close`). Exponential backoff with jitter; stable uptime resets the
        reconnect counter.
        """
        self._start_session_threads()
        furl = self.url + "/ws/" + self.channel_type

        while not self.stop_flag and self.reconnect_count <= self.max_reconnect_attempts:
            self.ws = WebSocketApp(
                furl,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
                on_open=self.on_open,
            )
            try:
                self.ws.run_forever()
            except Exception as e:
                print(f"❌ run_forever raised: {e}")

            if self.stop_flag:
                break

            # v13 (4d): reset reconnect_count if the connection was stable
            if self._last_open_time is not None:
                uptime = time.time() - self._last_open_time
                if uptime > self.reconnect_count_reset_uptime and self.reconnect_count > 0:
                    print(f"♻️ Connection stable for {uptime:.0f}s; resetting reconnect_count "
                          f"(was {self.reconnect_count})")
                    self.reconnect_count = 0

            if self.reconnect_count >= self.max_reconnect_attempts:
                # v13 (4b): tell other threads to stop instead of leaving them
                # running against a dead WS.
                print(f"❌ Max reconnect attempts ({self.max_reconnect_attempts}) "
                      f"exhausted; ending session.")
                self.stop_flag = True
                self._stop_event.set()
                break

            self.reconnect_count += 1
            # v13 (4c): exponential backoff (capped at 300s) with 10% jitter
            base = min(self.reconnect_delay * (2 ** min(self.reconnect_count - 1, 6)), 300)
            delay = base + random.uniform(0, base * 0.1)
            print(f"🔄 Reconnect attempt {self.reconnect_count}/"
                  f"{self.max_reconnect_attempts} in {delay:.1f}s")
            if self._stop_event.wait(timeout=delay):
                break

        # Final state — signal snapshot thread it can do its close-out
        self._ws_closed_event.set()
        self._summarize_session()
        return {
            'price_changes': self.total_price_change_counts,
            'trades':        self.trades,
            'intervals':     self.interval_history,
        }

    def _summarize_session(self):
        print("\n" + "=" * 60)
        print("WebSocket Session Ended - Summary")
        print("=" * 60)
        print(f"Total Reconnections: {self.reconnect_count}")
        print(f"Total Intervals: {len(self.interval_history)}")
        print(f"Total Price Changes: YES={self.total_price_change_counts['YES']} "
              f"NO={self.total_price_change_counts['NO']}")
        print(f"Total Trades: YES={len(self.trades['YES'])} NO={len(self.trades['NO'])}")
        if self.trades['YES']:
            vol = sum(t['size'] for t in self.trades['YES'])
            print(f"  YES Volume: {vol:.2f}")
        if self.trades['NO']:
            vol = sum(t['size'] for t in self.trades['NO'])
            print(f"  NO  Volume: {vol:.2f}")
        print("=" * 60)

    # ----------------------------------------- subscription / data accessors

    def subscribe_to_tokens_ids(self, assets_ids):
        if self.channel_type == MARKET_CHANNEL and self.ws is not None:
            self.ws.send(json.dumps({"assets_ids": assets_ids, "operation": "subscribe"}))

    def unsubscribe_to_tokens_ids(self, assets_ids):
        if self.channel_type == MARKET_CHANNEL and self.ws is not None:
            self.ws.send(json.dumps({"assets_ids": assets_ids, "operation": "unsubscribe"}))

    def get_trade_dataframes(self):
        yes_df = pd.DataFrame(self.trades['YES']) if self.trades['YES'] else pd.DataFrame()
        no_df  = pd.DataFrame(self.trades['NO'])  if self.trades['NO']  else pd.DataFrame()
        if not yes_df.empty:
            yes_df['timestamp'] = pd.to_datetime(yes_df['timestamp'].astype(float), unit='ms', utc=True)
        if not no_df.empty:
            no_df['timestamp']  = pd.to_datetime(no_df['timestamp'].astype(float), unit='ms', utc=True)
        return {'YES': yes_df, 'NO': no_df}

    def get_interval_dataframe(self):
        return pd.DataFrame(self.interval_history)


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == '__main__':
    # Patch B1: construct the ClobClient here, not at import time, so
    # `import polymarketeventfetcher_v13` is side-effect-free.
    client = _build_default_client()

    fetcher = PolymarketEventFetcher('btc-up-or-down-daily')
    fetcher.set_clob_client(client)

    seconds_budget = int(60 * 60 * 24 * 16)  # 16 days in seconds, to cover the 14-day event duration plus some buffer for pre-event and post-event data
    fetcher.run_continuous_cycle_seconds(
        total_seconds=seconds_budget,
        snapshot_interval=30,
        ws_interval=30,
        lob_levels=10,
        output_dir='LOB_Trial_4',
        verbose=True,
    )
