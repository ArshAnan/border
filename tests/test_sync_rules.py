"""Sync requirements not covered by the Phase 1 reproductions."""

from __future__ import annotations

import pytest

import orderbook.buffer as buffer_mod
import orderbook.client as client_mod
from orderbook.buffer import BookSynchronizer
from orderbook.client import DepthEvent, OrderBookSnapshot, SnapshotError
from orderbook.config import OrderBookConfig
from tests.drive import InitMode, drive_quickstart
from tests.fake_exchange import FakeExchange, client_session_factory


def _event(first: int, final: int, bids=(), asks=()) -> DepthEvent:
    return DepthEvent(1, "BTCUSDT", first, final, list(bids), list(asks))


async def test_announced_reconnect_rebuilds_even_without_an_id_gap(monkeypatch):
    exchange = FakeExchange()
    exchange.push(100, 101, bids=[(100.0, 2.0)], apply=False)
    exchange.inject_reconnect(announce=True)
    exchange.push(102, 102, bids=[(99.0, 1.0)], apply=False)
    aligning = OrderBookSnapshot(100, [(100.0, 1.0)], [(101.0, 1.0)])
    rebuilt = OrderBookSnapshot(101, [(100.0, 7.0)], [(101.0, 1.0)])
    scripted = [aligning, rebuilt]
    fetched: list[OrderBookSnapshot] = []

    async def fetch(config):
        fetched.append(scripted.pop(0))
        return fetched[-1]

    monkeypatch.setattr(buffer_mod, "fetch_snapshot", fetch)
    monkeypatch.setattr(buffer_mod, "stream_depth_events", exchange.stream)
    config = OrderBookConfig(symbol="BTCUSDT")
    config.rest_backoff_base_seconds = 0.001
    result = await drive_quickstart(
        config, mode=InitMode.ALIGNING, fetch_snapshot=fetch, fetched=fetched, timeout=0.3
    )
    assert len(fetched) == 2
    assert result.view == (102, ((100.0, 7.0), (99.0, 1.0)), ((101.0, 1.0),))


def test_buffer_overflow_resyncs_and_keeps_buffering():
    config = OrderBookConfig(symbol="BTCUSDT", max_buffer_events=3)
    sync = BookSynchronizer(config)
    for update_id in range(1, 5):
        sync.on_event(_event(update_id, update_id))
    assert sync.stats.overflows == 1
    assert sync.buffered == 1
    states = sync.on_snapshot(OrderBookSnapshot(3, [(10.0, 1.0)], [(11.0, 1.0)]))
    assert [state.last_update_id for state in states] == [4]


def test_snapshot_behind_the_buffer_is_rejected_and_buffer_kept():
    sync = BookSynchronizer(OrderBookConfig(symbol="BTCUSDT"))
    sync.on_event(_event(10, 12))
    assert sync.on_snapshot(OrderBookSnapshot(8, [(10.0, 1.0)], [(11.0, 1.0)])) == []
    assert sync.stats.snapshots_rejected == 1
    assert sync.wants_snapshot and sync.buffered == 1
    assert [s.last_update_id for s in sync.on_snapshot(OrderBookSnapshot(9, [], []))] == [12]


async def test_non_retryable_rest_status_raises(monkeypatch):
    exchange = FakeExchange()
    exchange.queue_snapshot(status=400)
    monkeypatch.setattr(client_mod.aiohttp, "ClientSession", client_session_factory(exchange))
    with pytest.raises(SnapshotError) as info:
        await client_mod.fetch_snapshot(OrderBookConfig(symbol="BTCUSDT"))
    assert info.value.retryable is False
