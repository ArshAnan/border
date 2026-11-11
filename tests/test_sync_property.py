"""Property test: random exchange schedules with faults against the oracle.

Every book state the library publishes carries an update id. The true book
at that id is known to the fake exchange, so each published state is
checked against a naive dict book built from it. After the schedule the
exchange keeps emitting clean diffs until the library publishes the latest
id, so a library that never recovers fails too.
"""

from __future__ import annotations

import asyncio

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

import orderbook.buffer as buffer_mod
import orderbook.client as client_mod
from orderbook.config import OrderBookConfig
from tests.drive import InitMode, drive_quickstart
from tests.fake_exchange import FakeExchange, client_session_factory
from tests.reference_book import ReferenceBook

LEVELS = 50
BID_PRICES = [float(p) for p in range(90, 100)]
ASK_PRICES = [float(p) for p in range(101, 111)]
QTY = st.sampled_from([0.0, 0.5, 1.0, 2.0, 3.5])

bid_levels = st.lists(st.tuples(st.sampled_from(BID_PRICES), QTY), max_size=3)
ask_levels = st.lists(st.tuples(st.sampled_from(ASK_PRICES), QTY), max_size=3)

op = st.one_of(
    st.tuples(st.just("emit"), bid_levels, ask_levels, st.integers(1, 3)),
    st.tuples(st.just("emit"), bid_levels, ask_levels, st.integers(1, 3)),
    st.tuples(st.just("emit"), bid_levels, ask_levels, st.integers(1, 3)),
    st.tuples(st.just("hide"), bid_levels, ask_levels),
    st.tuples(st.just("gap"), st.integers(1, 4), bid_levels, ask_levels),
    st.tuples(st.just("duplicate")),
    st.tuples(st.just("stale")),
    st.tuples(st.just("out_of_order"), bid_levels, bid_levels),
    st.tuples(st.just("reconnect")),
    st.tuples(st.just("rest_error"), st.sampled_from([429, 500, 502, 503])),
    st.tuples(st.just("pause")),
)


def _config() -> OrderBookConfig:
    config = OrderBookConfig(symbol="BTCUSDT", num_levels=LEVELS)
    # Keep REST and resync backoff short so a schedule finishes quickly.
    config.rest_backoff_base_seconds = 0.0005
    config.rest_backoff_max_seconds = 0.002
    return config


def _expected_view(exchange: FakeExchange, update_id: int) -> tuple:
    assert update_id in exchange._history, f"published id {update_id} was never a true book id"
    oracle = ReferenceBook()
    oracle.initialize(exchange.snapshot(last_update_id=update_id))
    return oracle.view(LEVELS)


async def _run(schedule) -> tuple[FakeExchange, list[tuple]]:
    exchange = FakeExchange()
    exchange.seed(
        [(95.0, 1.0), (94.0, 2.0)],
        [(105.0, 1.0), (106.0, 2.0)],
        last_update_id=1_000,
    )
    stop = asyncio.Event()
    schedule_done = asyncio.Event()

    async def produce() -> None:
        exchange.emit(bids=[(95.0, 1.5)])
        for item in schedule:
            kind = item[0]
            if kind == "emit":
                exchange.emit(item[1], item[2], n_ids=item[3])
            elif kind == "hide":
                exchange.hide(item[1], item[2])
            elif kind == "gap":
                exchange.emit_gap(item[1], item[2], item[3])
            elif kind == "duplicate":
                exchange.emit_duplicate()
            elif kind == "stale":
                exchange.emit_stale()
            elif kind == "out_of_order":
                exchange.emit_out_of_order((item[1], []), (item[2], []))
            elif kind == "reconnect":
                exchange.inject_reconnect()
            elif kind == "rest_error":
                exchange.queue_snapshot(status=item[1])
            elif kind == "pause":
                await asyncio.sleep(0.001)
            await asyncio.sleep(0)
        schedule_done.set()
        # Keep the stream alive with clean diffs until the library catches up.
        for _ in range(3_000):
            if stop.is_set():
                return
            exchange.emit(asks=[(110.0, 1.0)])
            await asyncio.sleep(0.0002)

    fetched: list = []
    real_fetch = client_mod.fetch_snapshot

    async def spy(config):
        snapshot = await real_fetch(config)
        fetched.append(snapshot)
        return snapshot

    def caught_up(result) -> bool:
        if not schedule_done.is_set():
            return False
        if result.views and result.views[-1][0] == exchange.last_update_id:
            stop.set()
            return True
        return False

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(client_mod.aiohttp, "ClientSession", client_session_factory(exchange, live=True))
        mp.setattr(buffer_mod, "stream_depth_events", exchange.stream)
        mp.setattr(buffer_mod, "fetch_snapshot", spy)
        producer = asyncio.create_task(produce())
        try:
            result = await drive_quickstart(
                _config(),
                mode=InitMode.ALIGNING,
                fetch_snapshot=spy,
                fetched=fetched,
                limit=1_000_000,
                timeout=3.0,
                until=caught_up,
            )
        finally:
            stop.set()
            await producer
    return exchange, result.views


@settings(
    max_examples=60,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
@given(st.lists(op, max_size=40))
def test_random_fault_schedules_match_the_oracle(schedule):
    try:
        exchange, views = asyncio.run(_run(schedule))
    except RuntimeError as exc:
        pytest.fail(f"stream ended: {exc}")
    assert views, "the library never published a book"
    for view in views:
        assert view == _expected_view(exchange, view[0])
    assert views[-1][0] == exchange.last_update_id
