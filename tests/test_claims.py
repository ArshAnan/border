"""Feature layout, documentation claims and the slow-consumer policy."""

from __future__ import annotations

import asyncio
import contextlib
import importlib.util
import re
from pathlib import Path

import numpy as np
import pytest

import orderbook.buffer as buffer_mod
from orderbook.book import OrderBookState, PriceLevel
from orderbook.buffer import BookSynchronizer
from orderbook.config import OrderBookConfig
from orderbook.features import FeatureExtractor, feature_names, feature_size
from orderbook.storage import OrderBookRecorder, OrderBookReplay
from orderbook.sync import stream_book
from tests.fake_exchange import FakeExchange
from tests.reference_book import ReferenceBook

ROOT = Path(__file__).resolve().parents[1]


def _states(n_levels: int, count: int) -> list[OrderBookState]:
    states = []
    for i in range(count):
        bids = [PriceLevel(100.0 - k - i * 0.25, 1.0 + k) for k in range(n_levels - 2)]
        asks = [PriceLevel(101.0 + k + i * 0.25, 2.0 + k) for k in range(n_levels)]
        mid = (bids[0].price + asks[0].price) / 2
        states.append(OrderBookState(bids, asks, 1000 + i, mid, asks[0].price - bids[0].price))
    return states


@pytest.mark.parametrize("levels,returns", [(20, 10), (5, 3)])
def test_live_and_replayed_feature_vectors_are_identical(tmp_path, levels, returns):
    config = OrderBookConfig(
        symbol="BTCUSDT", num_levels=levels, num_price_returns=returns, storage_dir=str(tmp_path)
    )
    names = feature_names(config)
    assert len(names) == len(set(names)) == feature_size(config)

    extractor = FeatureExtractor(config)
    recorder = OrderBookRecorder(config)
    live = []
    for state in _states(levels, 15):
        vector = extractor.update(state)
        assert vector.shape == (feature_size(config),)
        live.append(vector)
        recorder.record(state, vector)
    recorder.close()
    assert not list(tmp_path.glob("*.tmp"))

    frames = list(OrderBookReplay(config, str(tmp_path)).replay())
    assert [f.last_update_id for f in frames] == list(range(1000, 1015))
    for vector, frame in zip(live, frames, strict=True):
        assert frame.features.shape == vector.shape
        assert np.array_equal(frame.features, vector)


def test_readme_claims_match_the_code():
    readme = (ROOT / "README.md").read_text()
    size = feature_size(OrderBookConfig())
    assert f"**{size} elements**" in readme
    assert "VWAP" not in readme
    assert "price impact" not in readme.lower()
    assert "Binance.US" in readme
    assert "O(1)" not in readme
    for name in ("vwap", "price_impact"):
        assert not any(name in col for col in feature_names(OrderBookConfig()))
    # Every row in the README table adds up to the stated size.
    rows = re.findall(r"^\| [^|]+ \| (\d+|\d+ × N) \|", readme, flags=re.M)
    n = OrderBookConfig().num_levels
    r = OrderBookConfig().num_price_returns
    total = 0
    for cell in rows:
        total += int(cell.split()[0]) * (n if "N" in cell else 1)
    assert total + r == size or total == size


def _load_example():
    spec = importlib.util.spec_from_file_location("live_stream", ROOT / "examples" / "live_stream.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def test_example_queue_is_bounded_and_applies_backpressure():
    example = _load_example()
    queue = example.make_queue()
    assert 0 < queue.maxsize <= 10_000

    async def endless(config, sync):
        i = 0
        while True:
            i += 1
            yield i

    original = example.stream_book
    example.stream_book = endless
    try:
        task = asyncio.create_task(
            example.producer(OrderBookConfig(), BookSynchronizer(OrderBookConfig()), queue)
        )
        for _ in range(queue.maxsize + 50):
            await asyncio.sleep(0)
        assert queue.qsize() == queue.maxsize
        assert not task.done()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    finally:
        example.stream_book = original


async def test_slow_consumer_overflows_into_a_resync_and_recovers(monkeypatch):
    """Frames read but not consumed are bounded; overflow rebuilds the book."""
    exchange = FakeExchange()
    exchange.seed([(95.0, 1.0)], [(105.0, 1.0)], last_update_id=100)
    monkeypatch.setattr(buffer_mod, "stream_depth_events", exchange.stream)

    async def fetch(config):
        return exchange.snapshot()

    monkeypatch.setattr(buffer_mod, "fetch_snapshot", fetch)
    config = OrderBookConfig(symbol="BTCUSDT", max_buffer_events=8)
    config.rest_backoff_base_seconds = 0.0005
    sync = BookSynchronizer(config)
    gen = stream_book(config, sync)
    stop = asyncio.Event()

    async def feed() -> None:
        k = 0
        while not stop.is_set():
            k += 1
            exchange.emit(bids=[(94.0, float(k % 7 + 1))], asks=[(105.0, float(k % 3 + 1))])
            await asyncio.sleep(0.0005)

    feeder = asyncio.create_task(feed())
    try:
        seen = [await asyncio.wait_for(gen.__anext__(), 1)]
        # The consumer stalls while frames keep arriving.
        await asyncio.sleep(0.05)
        stalled_at = exchange.last_update_id
        assert stalled_at - seen[0].last_update_id > config.max_buffer_events
        while seen[-1].last_update_id <= stalled_at + 5:
            seen.append(await asyncio.wait_for(gen.__anext__(), 1))
    finally:
        stop.set()
        await feeder
        await gen.aclose()

    assert sync.stats.overflows >= 1
    for state in seen:
        oracle = ReferenceBook()
        oracle.initialize(exchange.snapshot(last_update_id=state.last_update_id))
        bids, asks = oracle.top(config.num_levels)
        assert [(lv.price, lv.quantity) for lv in state.bids] == bids
        assert [(lv.price, lv.quantity) for lv in state.asks] == asks
