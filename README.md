# border

An async Python library for real-time L2 order book reconstruction from Binance.US depth streams (`stream.binance.us`, `api.binance.us`). It keeps a local book in sync by applying incremental diffs with sequence validation, rebuilds it from a fresh snapshot after any gap, records the raw diffs, and replays them offline through the same sync code.

---

## Features

- **Real-time L2 reconstruction** — incremental diff application via the Binance.US `@depth@100ms` stream
- **Sequence validation** — enforces gapless update sequencing per the Binance.US order book procedure; a gap, reconnect, crossed book or buffer overflow rebuilds the book from a fresh snapshot
- **Sorted book** — price levels in `SortedDict`: an update is O(log n), best bid/ask is O(log n), and `top_levels(N)` is O(N) per call (it builds a new list each time). The book keeps every level; `num_levels` only limits what is output
- **Async-first** — built entirely on `asyncio`, `websockets`, and `aiohttp`; non-blocking throughout
- **Bounded backoff** — WebSocket reconnects and REST snapshot retries (429/418/5xx, honoring `Retry-After`) use bounded exponential backoff over one `aiohttp` session
- **Feature extraction** — top of book, top-N depth, imbalance, weighted mid, spread in bps, cumulative depth and recent mid-price returns
- **Parquet feature frames** — one row per published state (feature vector + update id + local wall clock), written atomically; the schema is the feature layout
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
Binance.US WebSocket (@depth@100ms)
          │
          ▼
      client.py          — async WebSocket client, exponential backoff reconnection
          │
          ▼
      buffer.py          — BookSynchronizer: buffer + snapshot + book, sequence rules
      sync.py            — stream_book(): live driver (public API)
          │
          ▼
        book.py          — SortedDict L2 book; O(log n) updates, O(N) top_levels
          │
          ▼
     features.py         — feature vector (layout in feature_names())
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
    snapshot_depth_limit=1000,    # REST depth limit (docs allow up to 5000)
    num_levels=20,                # levels output per state (the book keeps all)
    max_reconnect_attempts=10,
    base_backoff_seconds=1.0,
    max_backoff_seconds=60.0,
    storage_dir="data",
    snapshot_interval_seconds=1.0, # Parquet flush interval
    max_buffer_events=10_000,     # bound on buffered/unread diffs before a resync
    rest_max_attempts=8,          # REST snapshot retries per fetch
    rest_backoff_base_seconds=0.5,
    rest_backoff_max_seconds=30.0,
)
```

---

## Feature Vector

`features.py` produces a fixed-length `float64` numpy array per book state.
Its layout is `feature_names(config)`; the Parquet columns after
`timestamp_ms` and `last_update_id` are exactly those names, and
`OrderBookReplay` rebuilds the same vector from them. With the defaults
(`num_levels=20`, `num_price_returns=10`) the vector is **139 elements**:

| Segment | Size | Description |
|---|---|---|
| Top of book | 6 | best bid/ask price and qty, mid price, spread |
| Bid depth | 2 × N | prices, then quantities, for the top N bid levels |
| Ask depth | 2 × N | prices, then quantities, for the top N ask levels |
| Derived | 3 | imbalance, weighted mid price, spread in bps |
| Cumulative volume | 2 × N | cumulative bid, then ask, volume at each level |
| Returns | R | last R mid-price returns, oldest first, zero-padded |

Size = 9 + 6N + R. Missing levels are 0.0. Imbalance is
`bid_vol / (bid_vol + ask_vol)` across the top N levels (0.5 if both are
zero); values near 1.0 mean more resting bid volume.

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
- **Slow consumers** — `stream_book` bounds unread diffs at `max_buffer_events`; past that it drops them and rebuilds the book. The example's queue is bounded (`QUEUE_MAXSIZE`) and blocks the producer when full.
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