"""The simulator's own faults. These do not touch the library sync path."""

from __future__ import annotations

from orderbook.client import DepthEvent, OrderBookSnapshot
from tests.fake_exchange import FakeExchange
from tests.reference_book import ReferenceBook


def test_emit_ids_are_contiguous_and_snapshot_matches_history():
    exchange = FakeExchange()
    exchange.seed([(100.0, 1.0)], [(101.0, 2.0)], last_update_id=10)
    first = exchange.emit(bids=[(100.0, 1.5)])
    second = exchange.emit(asks=[(101.0, 0.0)], n_ids=2)
    assert (first.first_update_id, first.final_update_id) == (11, 11)
    assert (second.first_update_id, second.final_update_id) == (12, 13)
    assert exchange.snapshot(last_update_id=11).bids == [(100.0, 1.5)]
    current = exchange.snapshot()
    assert current.last_update_id == 13
    assert current.asks == []
    assert current.bids == [(100.0, 1.5)]


def test_injections_are_visible_on_the_stream_and_the_true_book():
    exchange = FakeExchange()
    exchange.seed([(100.0, 1.0)], [(101.0, 1.0)], last_update_id=0)
    exchange.emit(bids=[(100.0, 2.0)])
    duplicate = exchange.emit_duplicate()
    stale = exchange.emit_stale()
    later, earlier = exchange.emit_out_of_order(
        ([(100.0, 3.0)], []),
        ([(100.0, 4.0)], []),
    )
    crossed = exchange.emit_crossed()
    gapped = exchange.emit_gap(4, bids=[(100.0, 5.0)])
    exchange.inject_reconnect()

    assert duplicate is exchange.emitted[0]
    assert stale is exchange.emitted[0]
    assert later.first_update_id == earlier.first_update_id + 1
    assert crossed.bids[0][0] == min(exchange.asks)
    assert gapped.first_update_id > crossed.final_update_id + 1
    assert exchange.bids[100.0] == 5.0
    assert max(exchange.bids) >= min(exchange.asks)


def test_rest_script_can_fail_and_can_lead_the_book():
    exchange = FakeExchange()
    exchange.seed([(100.0, 1.0)], [(101.0, 1.0)], last_update_id=40)
    denied = exchange.queue_snapshot(status=429, retry_after="1.25")
    failed = exchange.queue_snapshot(status=503)
    ahead = exchange.queue_snapshot(ahead_by=25)
    assert denied.status == 429 and denied.retry_after == "1.25"
    assert failed.status == 503
    assert ahead.payload is not None
    assert ahead.payload["lastUpdateId"] == 65
    assert ahead.payload["bids"]


def test_reference_sorts_on_read_and_refuses_a_gap():
    book = ReferenceBook()
    book.initialize(
        OrderBookSnapshot(
            last_update_id=5,
            bids=[(10.0, 1.0), (12.0, 1.0)],
            asks=[(14.0, 1.0), (13.0, 1.0)],
        )
    )
    assert book.top() == ([(12.0, 1.0), (10.0, 1.0)], [(13.0, 1.0), (14.0, 1.0)])
    assert book.apply_event(DepthEvent(1, "BTCUSDT", 6, 6, [(12.0, 0.0)], []))
    assert book.apply_event(DepthEvent(1, "BTCUSDT", 9, 9, [], [])) is False
    assert book.last_update_id == 6
    assert book.top()[0] == [(10.0, 1.0)]
