# PolyMarket Event Fetcher v2

A comprehensive Python tool for monitoring and collecting Polymarket prediction market data, including limit order book (LOB) snapshots, real-time WebSocket price changes, and trade execution data.

## Overview

This tool provides automated, continuous monitoring of Polymarket daily recurring events (such as the "BTC Up or Down Daily" series). It captures:

- **Limit Order Book (LOB) Snapshots**: Multi-level bid/ask prices and sizes at regular intervals
- **WebSocket Real-time Data**: Price changes and trade executions as they occur
- **Market Metrics**: Spreads, volumes, settlement prices, and time-to-expiry tracking
- **Automatic Event Transitions**: Seamlessly switches to new daily events at 17:00 UTC

## Features

### Core Functionality

- **Continuous Monitoring**: Run for specified time budgets with automatic event rollovers
- **Dynamic LOB Tracking**: Captures 10 levels (configurable) of order book depth
- **WebSocket Integration**: Real-time price change and trade monitoring with automatic reconnection
- **Binance Price Integration**: Fetches BTC settlement and current prices for reference
- **Multi-threaded Architecture**: Parallel LOB snapshots and WebSocket data collection
- **Robust Error Handling**: Graceful degradation and reconnection logic
- **CSV Export**: Automatic data export with timestamped filenames

### Data Collected

#### LOB Snapshots (YES/NO)
- Bid/ask prices and sizes for 10 levels
- Spread calculations
- Volume data
- Settlement price reference
- Current BTC price
- Price difference from settlement
- Time to expiry
- Elapsed time since monitoring start

#### WebSocket Data
- Price change counts per interval
- Trade executions (price, size, side, timestamp)
- Interval-by-interval metrics
- Dynamic LOB bound tracking
- Connection health monitoring

#### Summary Statistics
- Total price changes (YES/NO)
- Trade volumes and counts
- VWAP (Volume-Weighted Average Price)
- Spread statistics (mean, min, max, volatility)
- Buy/sell volume breakdown

## Requirements

### Python Packages

```bash
pip install py-clob-client
pip install requests
pip install pandas
pip install numpy
pip install websocket-client
```

### Prerequisites

1. **Polymarket Account**: You need a funded Polymarket account
2. **Private Key**: Export your private key from:
   - Email login: https://reveal.magic.link/polymarket
   - Web3 wallet: Your wallet application
3. **Proxy Address**: Your Polymarket deposit address (where you send USDC)

## Configuration

### Initial Setup

```python
# CLOB Client Configuration
host = "https://clob.polymarket.com"
key = "YOUR_PRIVATE_KEY"  # Your exported private key
chain_id = 137  # Polygon mainnet
POLYMARKET_PROXY_ADDRESS = "YOUR_WALLET_ADDRESS"  # Your deposit address

# Initialize client
client = ClobClient(
    host, 
    key=key, 
    chain_id=chain_id, 
    signature_type=1, 
    funder=POLYMARKET_PROXY_ADDRESS
)

# Verify API key derivation
print(client.derive_api_key())
```

## Usage

### Basic Usage

```python
# 1. Initialize fetcher with series slug
fetcher = PolymarketEventFetcher('btc-up-or-down-daily')

# 2. Set CLOB client
fetcher.set_clob_client(client)

# 3. Fetch active event
fetcher.fetch_active_event()

# 4. Capture single LOB snapshot
snapshot = fetcher.capture_lob_snapshot(levels=10)
print(snapshot['YES'])  # YES token LOB data
print(snapshot['NO'])   # NO token LOB data
```

### Continuous Monitoring

```python
# Run continuous monitoring with time budget
fetcher = PolymarketEventFetcher('btc-up-or-down-daily')
fetcher.set_clob_client(client)

fetcher.run_continuous_cycle_seconds(
    total_seconds=3600,              # 1 hour total
    snapshot_interval=30,            # LOB snapshot every 30s
    ws_interval=30,                  # WebSocket interval grouping
    lob_levels=10,                   # 10 levels of order book
    output_dir='./data',             # Output directory
    cutoff_hour_utc=17,              # Event reset time (17:00 UTC)
    verbose=True                     # Print progress messages
)
```

### Advanced: Single Monitoring Session

```python
# Monitor specific event for fixed duration
fetcher = PolymarketEventFetcher('btc-up-or-down-daily')
fetcher.set_clob_client(client)
fetcher.fetch_active_event()

results = fetcher.monitor_market_activity(
    duration_seconds=300,    # 5 minutes
    snapshot_interval=10,    # Snapshot every 10s
    ws_interval=5,           # WebSocket 5s intervals
    lob_levels=10,
    verbose=True
)

# Access results
lob_yes = results['lob_snapshots']['YES']
lob_no = results['lob_snapshots']['NO']
trades_yes = results['trades']['YES']
trades_no = results['trades']['NO']
intervals = results['intervals']
summary = results['summary']
```

### Export Results

```python
# Export monitoring results to CSV
fetcher.export_monitoring_results(
    results, 
    output_dir='./polymarket_data'
)
```

## Class Reference

### PolymarketEventFetcher

Main class for fetching and monitoring Polymarket events.

#### Methods

**`__init__(series_slug, base_url="https://gamma-api.polymarket.com")`**
- Initialize fetcher with series slug
- `series_slug`: Series identifier (e.g., 'btc-up-or-down-daily')

**`fetch_active_event()`**
- Fetches currently active event from the series
- Returns DataFrame with event details and CLOB token IDs
- Filters for events created in last 24 hours

**`set_clob_client(client)`**
- Sets the CLOB client for order book fetching
- Required before capturing LOB snapshots

**`capture_lob_snapshot(levels=10, settlement_price=None)`**
- Captures single LOB snapshot for YES and NO tokens
- Returns dict with YES/NO DataFrames and event metadata

**`capture_multiple_lob_snapshots(num_snapshots=5, interval_seconds=3, levels=10, verbose=True)`**
- Captures multiple LOB snapshots in sequence
- Returns concatenated DataFrames

**`monitor_market_activity(duration_seconds=300, snapshot_interval=30, ws_interval=5, lob_levels=10, verbose=True)`**
- Runs parallel LOB snapshot and WebSocket monitoring
- Returns comprehensive results dict with all data

**`run_continuous_cycle_seconds(total_seconds, snapshot_interval=30, ws_interval=30, lob_levels=10, output_dir='data', cutoff_hour_utc=17, verbose=False)`**
- Continuous monitoring with automatic event transitions
- Handles 17:00 UTC event rollovers
- Automatically exports data after each session

**`export_monitoring_results(results, output_dir='data')`**
- Exports all monitoring data to CSV files
- Creates timestamped filenames

**`get_binance_settlement_price()`**
- Fetches BTC price at most recent 17:00 UTC
- Returns close price from Binance 1-minute kline

**`get_binance_current_price()`**
- Fetches most recent BTC price from Binance
- Returns latest 1-minute close price

### WebSocketOrderBook

Handles real-time WebSocket connections for price changes and trades.

#### Features

- Automatic reconnection (up to 1000 attempts)
- Interval-based data aggregation
- Dynamic LOB bound updates
- Connection health monitoring
- Separate YES/NO token tracking

#### Methods

**`__init__(...)`**
- Initializes WebSocket with token bounds and configuration
- Sets up interval tracking and reconnection logic

**`run()`**
- Starts WebSocket connection
- Returns results dict after completion

**`get_trade_dataframes()`**
- Returns DataFrames of all trades (YES/NO)
- Converts timestamps to datetime

**`get_interval_dataframe()`**
- Returns DataFrame of interval-by-interval metrics

## Output Files

All files are saved with format: `{event_slug}_{data_type}_{timestamp}.csv`

### File Types

1. **`*_lob_yes_*.csv`**: YES token LOB snapshots
   - Columns: bid_1 through bid_10, ask_1 through ask_10, bidsize_1 through bidsize_10, asksize_1 through asksize_10, spread, snapshot_timestamp, total_volume, settlement_price, current_price, price_diff, time_to_expiry, elapsed_seconds

2. **`*_lob_no_*.csv`**: NO token LOB snapshots
   - Same structure as YES

3. **`*_trades_yes_*.csv`**: YES token trades
   - Columns: price, size, side, timestamp, fee_rate_bps

4. **`*_trades_no_*.csv`**: NO token trades
   - Same structure as YES

5. **`*_intervals_*.csv`**: Interval metrics
   - Columns: interval_number, timestamp, yes_price_changes, no_price_changes, total_price_changes, yes_trade_count, no_trade_count, total_trade_count, yes_bound_min, yes_bound_max, no_bound_min, no_bound_max, reconnect_count

6. **`*_summary_*.csv`**: Summary statistics
   - Aggregated metrics for entire session

## Architecture

### Multi-threaded Design

```
┌─────────────────────────────────────────┐
│   PolymarketEventFetcher (Main)        │
└──────────────┬──────────────────────────┘
               │
       ┌───────┴────────┐
       │                │
       ▼                ▼
┌────────────┐   ┌──────────────────┐
│ LOB Thread │   │ WebSocket Thread │
│            │   │                  │
│ Periodic   │   │ Real-time        │
│ Snapshots  │   │ Price Changes    │
│ (30s)      │   │ & Trades         │
└────────────┘   └──────────────────┘
       │                │
       └───────┬────────┘
               ▼
       ┌──────────────┐
       │ CSV Export   │
       └──────────────┘
```

### Event Lifecycle

```
Start → Fetch Active Event → Monitor Until 17:00 UTC → 
    → New Event Check → Transition → Repeat → Budget Exhausted → Stop
```

## Best Practices

### Recommended Settings

- **Snapshot Interval**: 30-60 seconds (balance between granularity and API load)
- **WebSocket Interval**: 30 seconds (good aggregation window)
- **LOB Levels**: 10 (captures significant depth without excessive data)
- **Output Directory**: Use Google Drive mount for Colab persistence

### Error Handling

- The tool includes automatic reconnection for WebSocket failures
- LOB snapshot errors are logged but don't stop execution
- Missing data is filled with NaN values
- Connection health is monitored (60s timeout)

### Performance Considerations

- Each LOB snapshot makes 2 API calls (YES + NO order books)
- WebSocket uses single connection for both tokens
- Data is accumulated in memory and exported periodically
- Large time budgets (>24 hours) may require memory monitoring

## Troubleshooting

### Common Issues

**"No active event found"**
- Wait 10 seconds and retry (events created at 17:00 UTC daily)
- Verify series slug is correct

**"CLOB client not set"**
- Call `fetcher.set_clob_client(client)` before monitoring

**"WebSocket connection closed"**
- Automatic reconnection will attempt (up to 1000 times)
- Check internet connectivity
- Verify API credentials

**"No data returned from Binance"**
- Non-critical warning, BTC price fields will be NaN
- Check Binance API availability

## Example Output

### Console Output
```
🚀 Starting Continuous Monitoring Cycle for 3600 seconds
🔄 Event Reset Time: 17:00 UTC

🔍 Fetching active event...
✅ Active Event Found: bitcoin-up-or-down-on-february-12
⏱️ This session duration: 1800s (0.50 hours)
✅ Binance settlement price at 2026-02-11 17:00 UTC: $66,071.38

============================================================
Starting Market Monitoring
============================================================
Event: Bitcoin Up or Down on February 12?
Duration: 1800s (30.0 minutes)
Snapshot Interval: 30s
WebSocket Interval: 30s
LOB Levels: 10
============================================================

Price Bounds Set:
YES: 0.1800 - 0.7000
NO:  0.3000 - 0.8200

[0s] Capturing LOB snapshot #1...
[30s] Capturing LOB snapshot #2...
...
```

## Data Analysis Examples

### Load and Analyze LOB Data

```python
import pandas as pd

# Load LOB snapshots
lob_yes = pd.read_csv('bitcoin-up-or-down_lob_yes_20260212_120000.csv')

# Calculate average spread over time
avg_spread = lob_yes['spread'].mean()
print(f"Average spread: {avg_spread:.4f}")

# Plot spread evolution
import matplotlib.pyplot as plt
plt.plot(lob_yes['elapsed_seconds'], lob_yes['spread'])
plt.xlabel('Time (seconds)')
plt.ylabel('Spread')
plt.title('YES Token Spread Over Time')
plt.show()
```

### Analyze Trade Data

```python
# Load trades
trades = pd.read_csv('bitcoin-up-or-down_trades_yes_20260212_120000.csv')

# Calculate VWAP
vwap = (trades['price'] * trades['size']).sum() / trades['size'].sum()
print(f"VWAP: {vwap:.4f}")

# Buy vs Sell volume
buy_volume = trades[trades['side'] == 'BUY']['size'].sum()
sell_volume = trades[trades['side'] == 'SELL']['size'].sum()
print(f"Buy volume: {buy_volume:.2f}")
print(f"Sell volume: {sell_volume:.2f}")
```

## License

MIT License - See LICENSE file for details

## Contributing

Contributions welcome! Please open an issue or submit a pull request.

## Disclaimer

**Important**: This tool is for research and educational purposes only. Trading prediction markets involves financial risk. The authors assume no responsibility for financial losses incurred through use of this tool. Always conduct your own research and trade responsibly.

## Support

For issues, questions, or feature requests, please open a GitHub issue in this repository.

## Acknowledgments

- Built using the official [py-clob-client](https://github.com/Polymarket/py-clob-client) library
- Market data provided by [Polymarket](https://polymarket.com)
- BTC price data from [Binance API](https://binance-docs.github.io/apidocs/)

## Version History

### v2.0 (Current)
- Added continuous monitoring with automatic event transitions
- Implemented dynamic LOB bound updates
- Added WebSocket reconnection logic
- Integrated Binance price fetching
- Enhanced error handling and robustness
- Multi-threaded architecture for parallel data collection
- Comprehensive CSV export functionality

### v1.0
- Initial release with basic LOB snapshot functionality