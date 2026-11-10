"""Failing reproductions of the Phase 1 sync bugs.

The buffer currently starts a REST snapshot only after 50 diffs are queued.
Each script below pads with stale events so the interesting diff is in that
batch. Expectations follow the Binance.US rule: drop ``u <= lastUpdateId``,
accept a first event with ``U <= lastUpdateId + 1 <= u``, and resync on a
real gap. The spot document's first-event sentence disagrees with that
bridge; see the module comment in the Step 0 report. These tests encode the
bridge as accepted, which is why ``test_bridge_at_last_update_id_plus_one``
fails on the current predicate.
"""

from __future__ import annotations

import asyncio

import pytest

import orderbook.buffer as buffer_mod
import orderbook.client as client_mod
from orderbook.book import OrderBook
from orderbook.client import DepthEvent, OrderBookSnapshot
from orderbook.config import OrderBookConfig
from tests.drive import InitMode, book_view, drive_quickstart
from tests.fake_exchange import FakeExchange, client_session_factory
from tests.reference_book import ReferenceBook

# DepthEventBuffer fetches once the deque reaches this hardcoded size.
BATCH = 50


def _config() -> OrderBookConfig:
    return OrderBookConfig(symbol="BTCUSDT")


def _install_stream(monkeypatch, exchange: FakeExchange, snapshots: list):
    fetched: list[OrderBookSnapshot] = []

    async def fetch(config):
        if not snapshots:
            raise AssertionError("fetch_snapshot called with no scripted snapshot left")
        item = snapshots.pop(0)
        if asyncio.iscoroutinefunction(item):
            item = await item()
        fetched.append(item)
        return item

    monkeypatch.setattr(buffer_mod, "fetch_snapshot", fetch)
    monkeypatch.setattr(client_mod, "fetch_snapshot", fetch)
    monkeypatch.setattr(buffer_mod, "stream_depth_events", exchange.stream)
    return fetched, fetch


def _pad(exchange: FakeExchange, count: int, start: int) -> None:
    for update_id in range(start, start + count):
        exchange.push(update_id, update_id, apply=False)


def _book(last_update_id: int, bids, asks) -> OrderBookSnapshot:
    return OrderBookSnapshot(last_update_id, list(bids), list(asks))


async def _drive(monkeypatch, exchange, snapshots, **kwargs):
    fetched, fetch = _install_stream(monkeypatch, exchange, snapshots)
    return await drive_quickstart(
        _config(),
        fetch_snapshot=fetch,
        fetched=fetched,
        **kwargs,
    )


async def test_quickstart_discards_the_aligning_snapshot(monkeypatch):
    """Diffs validated against snapshot A are applied to a later snapshot B."""
    exchange = FakeExchange()
    exchange.seed([(100.0, 1.0)], [(101.0, 1.0)], last_update_id=0)
    _pad(exchange, BATCH - 1, start=1)
    bridge = exchange.push(100, 101, bids=[(100.0, 2.0)], apply=False)
    aligning = _book(100, [(100.0, 1.0)], [(101.0, 1.0)])
    later = _book(150, [(100.0, 9.0), (50.0, 4.0)], [(101.0, 3.0)])

    result = await _drive(
        monkeypatch,
        exchange,
        [aligning, later],
        mode=InitMode.SECOND,
        limit=1,
    )

    oracle = ReferenceBook()
    oracle.initialize(aligning)
    assert oracle.apply_event(bridge)
    assert result.view == oracle.view()


async def test_snapshot_ahead_of_the_buffer_still_syncs(monkeypatch):
    """Events that arrive while REST is in flight have to stay in the buffer.

    The scripted snapshot id is 60, ahead of the 50 diffs already queued.
    During the REST call the exchange emits 51..70. A reader that keeps
    buffering can bridge id 60. A reader blocked inside the REST call drops
    the batch and publishes nothing.
    """
    exchange = FakeExchange()
    exchange.seed([(100.0, 1.0)], [(101.0, 1.0)], last_update_id=0)
    _pad(exchange, BATCH, start=1)

    async def fetch_during_flight():
        for update_id in range(51, 71):
            bids = [(100.0, 3.0)] if update_id == 70 else []
            exchange.push(update_id, update_id, bids=bids, apply=False)
        await asyncio.sleep(0)
        return _book(60, [(100.0, 1.0)], [(101.0, 1.0)])

    result = await _drive(
        monkeypatch,
        exchange,
        [fetch_during_flight],
        mode=InitMode.ALIGNING,
        timeout=0.3,
    )

    oracle = ReferenceBook()
    oracle.initialize(_book(60, [(100.0, 1.0)], [(101.0, 1.0)]))
    for update_id in range(61, 71):
        bids = [(100.0, 3.0)] if update_id == 70 else []
        applied = oracle.apply_event(
            DepthEvent(1, "BTCUSDT", update_id, update_id, bids, [])
        )
        assert applied, update_id
    assert result.view == oracle.view()


async def test_event_with_u_equal_to_last_update_id_is_dropped(monkeypatch):
    exchange = FakeExchange()
    exchange.seed([(100.0, 1.0)], [(101.0, 1.0)], last_update_id=0)
    _pad(exchange, BATCH - 2, start=1)
    exchange.push(100, 100, bids=[(100.0, 5.0)], apply=False)
    bridge = exchange.push(101, 102, bids=[(100.0, 2.0)], apply=False)
    aligning = _book(100, [(100.0, 1.0)], [(101.0, 1.0)])

    result = await _drive(
        monkeypatch,
        exchange,
        [aligning],
        mode=InitMode.ALIGNING,
        limit=1,
    )

    oracle = ReferenceBook()
    oracle.initialize(aligning)
    assert oracle.apply_event(bridge)
    assert result.views
    assert result.views[0] == oracle.view()


async def test_bridge_at_last_update_id_plus_one_is_accepted(monkeypatch):
    """U == lastUpdateId + 1 does not contain lastUpdateId, and must still apply."""
    exchange = FakeExchange()
    exchange.seed([(100.0, 1.0)], [(101.0, 1.0)], last_update_id=0)
    _pad(exchange, BATCH - 1, start=1)
    bridge = exchange.push(101, 105, bids=[(100.0, 2.0)], apply=False)
    aligning = _book(100, [(100.0, 1.0)], [(101.0, 1.0)])

    result = await _drive(
        monkeypatch,
        exchange,
        [aligning],
        mode=InitMode.ALIGNING,
        timeout=0.3,
    )

    oracle = ReferenceBook()
    oracle.initialize(aligning)
    assert oracle.apply_event(bridge)
    assert result.view == oracle.view()


async def test_hole_inside_the_drained_buffer_rebuilds(monkeypatch):
    """A gap between buffered diffs is applied today. It has to resync instead.

    Update ids 105..109 never arrive. They add bid 50. The drained batch jumps
    from u=104 to U=110. The rebuilt book is the fresh snapshot, which still
    has that hidden bid.
    """
    exchange = FakeExchange()
    exchange.seed([(100.0, 1.0)], [(101.0, 1.0)], last_update_id=0)
    _pad(exchange, BATCH - 4, start=1)
    exchange.push(100, 102, bids=[(100.0, 2.0)], apply=False)
    exchange.push(103, 104, bids=[(100.0, 3.0)], apply=False)
    exchange.push(110, 111, bids=[(100.0, 7.0)], apply=False)
    exchange.push(112, 112, bids=[(100.0, 8.0)], apply=False)
    aligning = _book(100, [(100.0, 1.0)], [(101.0, 1.0)])
    rebuilt = _book(200, [(100.0, 8.0), (50.0, 4.0)], [(101.0, 1.0)])
    _pad(exchange, BATCH - 1, start=151)
    post = exchange.push(200, 201, bids=[(100.0, 8.0)], apply=False)

    result = await _drive(
        monkeypatch,
        exchange,
        [aligning, rebuilt],
        mode=InitMode.ALIGNING,
        timeout=0.5,
    )

    oracle = ReferenceBook()
    oracle.initialize(rebuilt)
    assert oracle.apply_event(post)
    assert result.view == oracle.view()


async def test_live_gap_rebuilds_the_book_from_a_fresh_snapshot(monkeypatch):
    exchange = FakeExchange()
    exchange.seed([(100.0, 1.0)], [(101.0, 1.0)], last_update_id=0)
    _pad(exchange, BATCH - 2, start=1)
    exchange.push(100, 101, bids=[(100.0, 2.0)], apply=False)
    exchange.push(102, 102, bids=[(100.0, 2.5)], apply=False)
    exchange.push(180, 180, bids=[(99.5, 1.0)], apply=False)
    aligning = _book(100, [(100.0, 1.0)], [(101.0, 1.0)])
    rebuilt = _book(200, [(100.0, 8.0), (99.0, 5.0)], [(101.0, 1.0)])
    _pad(exchange, BATCH - 1, start=151)
    post = exchange.push(200, 201, bids=[(99.5, 1.0)], apply=False)

    result = await _drive(
        monkeypatch,
        exchange,
        [aligning, rebuilt],
        mode=InitMode.ALIGNING,
        timeout=0.5,
    )

    oracle = ReferenceBook()
    oracle.initialize(rebuilt)
    assert oracle.apply_event(post)
    assert result.view == oracle.view()


async def test_reconnect_rebuilds_the_book_from_a_fresh_snapshot(monkeypatch):
    exchange = FakeExchange()
    exchange.seed([(100.0, 1.0)], [(101.0, 1.0)], last_update_id=0)
    _pad(exchange, BATCH - 1, start=1)
    exchange.push(100, 101, bids=[(100.0, 2.0)], apply=False)
    exchange.inject_reconnect()
    exchange.push(500, 500, bids=[(99.5, 1.0)], apply=False)
    aligning = _book(100, [(100.0, 1.0)], [(101.0, 1.0)])
    rebuilt = _book(600, [(100.0, 8.0), (99.0, 5.0)], [(101.0, 1.0)])
    _pad(exchange, BATCH - 1, start=551)
    post = exchange.push(600, 601, bids=[(99.5, 1.0)], apply=False)

    result = await _drive(
        monkeypatch,
        exchange,
        [aligning, rebuilt],
        mode=InitMode.ALIGNING,
        timeout=0.5,
    )

    assert exchange.reconnects == 1
    oracle = ReferenceBook()
    oracle.initialize(rebuilt)
    assert oracle.apply_event(post)
    assert result.view == oracle.view()


def test_apply_event_does_not_move_the_update_id_backward():
    book = OrderBook(_config())
    book.initialize(_book(500, [(10.0, 1.0)], [(11.0, 1.0)]))
    book.apply_event(DepthEvent(1, "BTCUSDT", 450, 450, [(10.0, 99.0)], []))
    assert book_view(book) == (500, ((10.0, 1.0),), ((11.0, 1.0),))


async def test_crossed_book_is_replaced_by_a_fresh_snapshot(monkeypatch):
    exchange = FakeExchange()
    exchange.seed([(100.0, 1.0)], [(101.0, 1.0)], last_update_id=0)
    _pad(exchange, BATCH - 2, start=1)
    exchange.push(100, 101, apply=False)
    exchange.push(102, 102, bids=[(101.0, 9.0)], apply=False)
    aligning = _book(100, [(100.0, 1.0)], [(101.0, 1.0)])
    rebuilt = _book(200, [(100.0, 4.0)], [(101.0, 1.0)])
    _pad(exchange, BATCH - 1, start=151)
    post = exchange.push(200, 201, apply=False)

    result = await _drive(
        monkeypatch,
        exchange,
        [aligning, rebuilt],
        mode=InitMode.ALIGNING,
        timeout=0.5,
    )

    oracle = ReferenceBook()
    oracle.initialize(rebuilt)
    assert oracle.apply_event(post)
    assert not oracle.crossed
    assert result.view == oracle.view()


@pytest.mark.parametrize("status", [429, 500, 503])
async def test_rest_error_does_not_end_the_stream(monkeypatch, status):
    exchange = FakeExchange()
    exchange.seed([(100.0, 1.0)], [(101.0, 1.0)], last_update_id=0)
    _pad(exchange, BATCH - 1, start=1)
    bridge = exchange.push(100, 101, bids=[(100.0, 2.0)], apply=False)
    aligning = _book(100, [(100.0, 1.0)], [(101.0, 1.0)])
    retry_after = "1.25" if status == 429 else None
    exchange.queue_snapshot(status=status, retry_after=retry_after)
    exchange.queue_snapshot(snapshot=aligning)

    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    session_cls = client_session_factory(exchange)
    monkeypatch.setattr(client_mod.aiohttp, "ClientSession", session_cls)
    monkeypatch.setattr(client_mod.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(buffer_mod, "stream_depth_events", exchange.stream)
    fetched: list[OrderBookSnapshot] = []
    real_fetch = client_mod.fetch_snapshot

    async def spy(config):
        snapshot = await real_fetch(config)
        fetched.append(snapshot)
        return snapshot

    monkeypatch.setattr(buffer_mod, "fetch_snapshot", spy)

    try:
        result = await drive_quickstart(
            _config(),
            mode=InitMode.ALIGNING,
            fetch_snapshot=spy,
            fetched=fetched,
            limit=1,
            timeout=0.5,
        )
    except RuntimeError as exc:
        pytest.fail(f"REST {status} ended the generator: {exc}")

    oracle = ReferenceBook()
    oracle.initialize(aligning)
    assert oracle.apply_event(bridge)
    assert result.view == oracle.view()
    if status == 429:
        assert 1.25 in sleeps
    else:
        assert sleeps
        assert sleeps[0] > 0


async def test_snapshot_fetches_share_one_client_session(monkeypatch):
    exchange = FakeExchange()
    exchange.seed([(100.0, 1.0)], [(101.0, 1.0)], last_update_id=0)
    _pad(exchange, BATCH - 2, start=1)
    exchange.push(100, 101, bids=[(100.0, 2.0)], apply=False)
    exchange.push(102, 102, bids=[(100.0, 2.5)], apply=False)
    exchange.push(180, 180, apply=False)
    aligning = _book(100, [(100.0, 1.0)], [(101.0, 1.0)])
    rebuilt = _book(200, [(100.0, 8.0), (99.0, 5.0)], [(101.0, 1.0)])
    _pad(exchange, BATCH - 1, start=151)
    exchange.push(200, 201, apply=False)
    exchange.queue_snapshot(snapshot=aligning)
    exchange.queue_snapshot(snapshot=rebuilt)

    session_cls = client_session_factory(exchange)
    monkeypatch.setattr(client_mod.aiohttp, "ClientSession", session_cls)
    monkeypatch.setattr(buffer_mod, "stream_depth_events", exchange.stream)
    fetched: list[OrderBookSnapshot] = []
    real_fetch = client_mod.fetch_snapshot

    async def spy(config):
        snapshot = await real_fetch(config)
        fetched.append(snapshot)
        return snapshot

    monkeypatch.setattr(buffer_mod, "fetch_snapshot", spy)
    result = await drive_quickstart(
        _config(),
        mode=InitMode.ALIGNING,
        fetch_snapshot=spy,
        fetched=fetched,
        timeout=0.5,
        until=lambda current: len(current.fetched) >= 2 and len(current.events) >= 3,
    )
    assert len(result.fetched) >= 2
    assert len(session_cls.created) == 1
