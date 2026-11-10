# border

A high-performance, async Python library for real-time order book reconstruction from Binance WebSocket streams. Maintains a fully synchronized local L2 order book via incremental diff application, with sequence validation, automatic resync on gap detection, and Parquet-based recording and deterministic replay.

---

## Features

- **Real-time L2 reconstruction** — incremental diff application via Binance `@depth@100ms` stream
- **Sequence validation** — enforces gapless update sequencing per Binance's sync protocol; triggers automatic resync on any detected gap
- **O(log n) updates** — price level insertions and deletions via `SortedDict`; best bid/ask access in O(1)
- **Async-first** — built entirely on `asyncio`, `websockets`, and `aiohttp`; non-blocking throughout
- **Exponential backoff reconnection** — configurable retry logic with bounded backoff
- **Feature extraction** — computes spread, order book imbalance, weighted mid price, VWAP, cumulative depth, and price impact estimates from raw book state
- **Parquet recording** — columnar storage via `pyarrow` with Snappy compression; schema-stable across sessions
- **Deterministic replay** — raw diffs and snapshots are logged in the order the synchronizer consumed them and replayed offline through the same code; replay does not reproduce timing

---

## Installation

Requires Python 3.11+. Install with `uv`:

```bash
git clone https://github.com/ArshAnan/border.git
cd border
uv sync
uv pip install -e .
```

Or with `pip`:

```bash
pip install -e .
```

---

## Quickstart

### Stream a live order book

```python
import asyncio
from orderbook.config import OrderBookConfig
from orderbook.sync import stream_book

async def main():
    config = OrderBookConfig(symbol="BTCUSDT")
    async for state in stream_book(config):
        print(f"id={state.last_update_id} mid={state.mid_price:.2f} spread={state.spread:.4f}")

asyncio.run(main())
```

`stream_book` owns the diff buffer, the REST snapshot and the book, so the
snapshot that validates the buffered diffs is the one that seeds the book. It
yields the top-N state after every applied diff and never ends on its own: a
gap, reconnect, crossed book or buffer overflow rebuilds the book from a fresh
snapshot, and REST 429/418/5xx responses are retried with backoff. Pass a
`BookSynchronizer` to read `.book` and `.stats`.

### Record to Parquet

```python
from orderbook.features import FeatureExtractor
from orderbook.storage import OrderBookRecorder

extractor = FeatureExtractor(config)
recorder = OrderBookRecorder(config)

async for state in stream_book(config):
    recorder.record(state, extractor.update(state))

recorder.close()
```

### Record raw diffs and replay them offline

```python
from orderbook.replay import RawRecorder, replay_raw

recorder = RawRecorder(config)            # data/raw/*.jsonl.gz
async for state in stream_book(config, recorder=recorder):
    ...
recorder.close()

# Later, with no network: same synchronizer, same inputs, same states.
for state in replay_raw(sorted(Path("data/raw").glob("BTCUSDT_raw_*.jsonl.gz")), config):
    ...
```

---

## Architecture

```
Binance WebSocket (@depth@100ms)
          │
          ▼
      client.py          — async WebSocket client, exponential backoff reconnection
          │
          ▼
      buffer.py          — BookSynchronizer: buffer + snapshot + book, sequence rules
      sync.py            — stream_book(): live driver (public API)
          │
          ▼
        book.py          — SortedDict L2 book, O(log n) incremental updates
          │
          ▼
     features.py         — numerical feature extraction (spread, imbalance, VWAP, depth)
          │
          ▼
      storage.py         — Parquet top-N state frames (recorder + reader)
      replay.py          — raw diff log + offline replay through the synchronizer
```

---

## Configuration

All settings are controlled via `OrderBookConfig`:

```python
from orderbook.config import OrderBookConfig

config = OrderBookConfig(
    symbol="BTCUSDT",
    depth_update_speed="100ms",   # "100ms" or "1000ms"
    snapshot_depth_limit=1000,    # 100, 500, or 1000
    num_levels=20,                # price levels to maintain
    max_reconnect_attempts=10,
    base_backoff_seconds=1.0,
    max_backoff_seconds=60.0,
    storage_dir="data",
    snapshot_interval_seconds=1.0,
)
```

---

## Feature Vector

`features.py` produces a fixed-length `float64` numpy array per book state. With `num_levels=20` (default), the vector is **129 elements**:

| Segment | Size | Description |
|---|---|---|
| Top of book | 6 | best bid/ask price and qty, mid price, spread |
| Bid depth | 2 × N | prices and quantities for top N bid levels |
| Ask depth | 2 × N | prices and quantities for top N ask levels |
| Derived | 3 | imbalance, weighted mid price, spread in bps |
| Cumulative volume | 2 × N | cumulative bid and ask volume at each level |

Order book imbalance is defined as `bid_vol / (bid_vol + ask_vol)` across the top N levels — values near 1.0 indicate buying pressure, near 0.0 indicate selling pressure.

---

## Sync Protocol

`border` follows the Binance.US "Managing a Local Order Book" procedure
(<https://docs.binance.us/#managing-a-local-order-book>; the Binance spot
document has the same steps):

1. Open the WebSocket stream and buffer diff events.
2. Fetch a REST snapshot (`GET /api/v3/depth`) concurrently; the socket keeps
   being read while the request is in flight.
3. If the snapshot's `lastUpdateId + 1` is before the first buffered `U`,
   fetch another snapshot.
4. Drop buffered events with `u <= lastUpdateId`.
5. Seed the book from that same snapshot and apply every buffered and live
   event with one rule: ignore `u <= book_id`; `U > book_id + 1` is a gap;
   otherwise apply and set `book_id = u`. The first applied event therefore
   satisfies `U <= lastUpdateId + 1 <= u`. (The docs say the first event
   should contain `lastUpdateId`; an event that starts exactly at
   `lastUpdateId + 1` misses nothing and is accepted, as the docs' own update
   procedure does.)
6. On a gap, reconnect, crossed book or buffer overflow: discard the book,
   fetch a fresh snapshot and rebuild.

The snapshot depth is `snapshot_depth_limit` (default 1000; the docs now show
5000). Levels outside the snapshot are unknown until they change.

---

## Replay

`RawRecorder` logs every input the synchronizer consumes, in the order it
consumes it: raw depth frames with a local `time.time_ns` receive time, each
REST snapshot body, reconnects and overflows. Segments are gzip JSONL (the
line format of `bench/capture_raw.py`), at most `max_records` records each,
written to a temp file, fsynced and renamed, so every segment on disk is
complete.

`replay_raw` feeds a log through a fresh `BookSynchronizer` with no network
and yields the same states the live run published, provided `num_levels` and
`max_buffer_events` match. It does not reproduce timing. From the shell:

```bash
python -m orderbook.replay --num-levels 20 data/raw/BTCUSDT_raw_*.jsonl.gz
```

prints the number of states and a SHA-256 over them.

`OrderBookReplay` in `storage.py` only re-reads the top-N state frames written
by `OrderBookRecorder` (stamped with the local clock); it does not rebuild a
book.

---

## Known Limitations / Future Work

- **Gap handling** — any sequence gap discards the book and rebuilds it from a fresh snapshot; there is no partial recovery.
- **Single symbol** — one `OrderBook` instance per symbol. Multi-symbol support would require a multiplexed WebSocket connection.
- **Float64 precision** — prices and quantities are stored as `float64`. For production execution use cases, `Decimal` arithmetic should be considered to avoid floating point rounding on financial values.

---

## Dependencies

| Package | Purpose |
|---|---|
| `websockets` | async WebSocket client |
| `aiohttp` | async HTTP for REST snapshot |
| `sortedcontainers` | O(log n) sorted price level storage |
| `pyarrow` | Parquet recording and replay |
| `numpy` | feature vector construction |

---

## License

Apache 2.0