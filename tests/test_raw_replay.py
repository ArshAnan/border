"""Raw event log and offline replay through the live synchronizer."""

from __future__ import annotations

import asyncio
import contextlib
import gzip
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

import orderbook.buffer as buffer_mod
import orderbook.client as client_mod
from orderbook.config import OrderBookConfig
from orderbook.replay import RawRecorder, load_capture, replay_raw, state_digest
from orderbook.sync import stream_book
from tests.fake_exchange import FakeExchange, client_session_factory

ROOT = Path(__file__).resolve().parents[1]


def _config(tmp_path: Path) -> OrderBookConfig:
    config = OrderBookConfig(symbol="BTCUSDT", num_levels=10, storage_dir=str(tmp_path))
    config.rest_backoff_base_seconds = 0.0005
    config.rest_backoff_max_seconds = 0.002
    return config


async def _record_live(tmp_path: Path, monkeypatch) -> list:
    """Run stream_book against a faulty fake exchange while recording."""
    exchange = FakeExchange()
    exchange.seed([(95.0, 1.0), (94.0, 2.0)], [(105.0, 1.0)], last_update_id=500)
    monkeypatch.setattr(client_mod.aiohttp, "ClientSession", client_session_factory(exchange, live=True))
    monkeypatch.setattr(buffer_mod, "stream_depth_events", exchange.stream)

    config = _config(tmp_path)
    recorder = RawRecorder(config, tmp_path / "raw", max_records=7)
    states = []

    async def produce() -> None:
        steps = [
            lambda: exchange.emit(bids=[(95.0, 2.0)]),
            lambda: exchange.emit(asks=[(104.0, 1.0)], n_ids=2),
            lambda: exchange.emit_gap(3, bids=[(93.0, 1.0)]),
            lambda: exchange.queue_snapshot(status=429),
            lambda: exchange.emit(bids=[(95.0, 0.0)]),
            lambda: exchange.emit_duplicate(),
            lambda: exchange.inject_reconnect(announce=True),
            lambda: exchange.emit(asks=[(106.0, 3.0)]),
            lambda: exchange.hide(bids=[(92.0, 1.0)]),
        ]
        for step in steps:
            step()
            for _ in range(3):
                exchange.emit(asks=[(107.0, float(exchange.last_update_id % 5))])
                await asyncio.sleep(0.001)
        for _ in range(20):
            exchange.emit(bids=[(91.0, float(exchange.last_update_id % 3))])
            await asyncio.sleep(0.001)

    async def consume() -> None:
        async for state in stream_book(config, recorder=recorder):
            states.append(state)

    task = asyncio.create_task(consume())
    await produce()
    await asyncio.sleep(0.05)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    recorder.close()
    return states


def _replay_in_subprocess(paths: list[Path], seed: str) -> str:
    env = dict(os.environ, PYTHONHASHSEED=seed)
    out = subprocess.run(
        [sys.executable, "-m", "orderbook.replay", "--num-levels", "10", *map(str, paths)],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    return out.stdout.strip().splitlines()[-1]


async def test_replay_is_deterministic_across_processes_and_matches_live(tmp_path, monkeypatch):
    live_states = await _record_live(tmp_path, monkeypatch)
    assert live_states, "live run published nothing"
    paths = sorted((tmp_path / "raw").glob("*.jsonl.gz"))
    assert len(paths) > 1, "expected several segments"

    first = _replay_in_subprocess(paths, "1")
    second = _replay_in_subprocess(paths, "2")
    live_line = f"states={len(live_states)} sha256={state_digest(live_states)}"
    assert first == second
    assert first == live_line


async def test_segments_are_complete_and_buffer_is_bounded(tmp_path, monkeypatch):
    await _record_live(tmp_path, monkeypatch)
    raw_dir = tmp_path / "raw"
    assert not list(raw_dir.glob("*.tmp"))
    for path in raw_dir.glob("*.jsonl.gz"):
        lines = gzip.decompress(path.read_bytes()).decode().splitlines()
        records = [json.loads(line) for line in lines]
        assert records[0]["type"] == "header"
        assert len(records) - 1 <= 7
        kinds = {r["type"] for r in records[1:]}
        assert kinds <= {"depth", "snapshot", "ws_open", "overflow"}
        for record in records[1:]:
            assert isinstance(record["recv_ns"], int)


def test_failed_flush_leaves_no_partial_segment(tmp_path, monkeypatch):
    config = _config(tmp_path)
    recorder = RawRecorder(config, tmp_path / "raw", max_records=2)

    def boom(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr("orderbook.replay.os.replace", boom)
    recorder.reconnect()
    with pytest.raises(OSError):
        recorder.reconnect()
    assert not list((tmp_path / "raw").iterdir())


def test_replay_uses_no_network(tmp_path, monkeypatch):
    raw = tmp_path / "raw"
    raw.mkdir()
    lines = [
        {"type": "header", "format": "border-raw/1", "symbol": "BTCUSDT", "session": 1},
        {"type": "depth", "recv_ns": 1, "raw": json.dumps(
            {"e": "depthUpdate", "E": 1, "s": "BTCUSDT", "U": 11, "u": 12,
             "b": [["10.0", "2.0"]], "a": []})},
        {"type": "snapshot", "recv_ns": 2, "body": {
            "lastUpdateId": 10, "bids": [["10.0", "1.0"]], "asks": [["11.0", "1.0"]]}},
        {"type": "depth", "recv_ns": 3, "raw": json.dumps(
            {"e": "depthUpdate", "E": 2, "s": "BTCUSDT", "U": 13, "u": 13,
             "b": [], "a": [["11.0", "0.5"]]})},
    ]
    path = raw / "BTCUSDT_raw_x_000000.jsonl.gz"
    path.write_bytes(gzip.compress("".join(json.dumps(r) + "\n" for r in lines).encode()))

    def no_network(*args, **kwargs):
        raise AssertionError("replay touched the network")

    monkeypatch.setattr(client_mod.aiohttp, "ClientSession", no_network)
    monkeypatch.setattr(client_mod.websockets, "connect", no_network)
    states = list(replay_raw([path], OrderBookConfig(symbol="BTCUSDT")))
    assert [s.last_update_id for s in states] == [12, 13]
    assert states[-1].bids[0].quantity == 2.0
    assert states[-1].asks[0].quantity == 0.5


def test_capture_raw_files_replay(tmp_path):
    """bench/capture_raw.py layout: separate snapshot body, ws_open per connection."""
    depth = tmp_path / "BTCUSDT_depth.jsonl.gz"
    snap = tmp_path / "BTCUSDT_snapshot.json"
    meta = tmp_path / "BTCUSDT_snapshot.meta.json"
    snap.write_text(json.dumps({"lastUpdateId": 20, "bids": [["10.0", "1.0"]], "asks": [["11.0", "1.0"]]}))
    meta.write_text(json.dumps({"response_ns": 5, "status": 200}))
    frames = [
        {"type": "header", "symbol": "BTCUSDT", "started_ns": 1},
        {"type": "ws_open", "recv_ns": 6},
        {"type": "depth", "recv_ns": 7, "raw": json.dumps(
            {"e": "depthUpdate", "E": 1, "s": "BTCUSDT", "U": 19, "u": 21, "b": [["10.0", "3.0"]], "a": []})},
        {"type": "ws_open", "recv_ns": 8},
        {"type": "depth", "recv_ns": 9, "raw": json.dumps(
            {"e": "depthUpdate", "E": 2, "s": "BTCUSDT", "U": 22, "u": 22, "b": [], "a": []})},
    ]
    depth.write_bytes(gzip.compress("".join(json.dumps(r) + "\n" for r in frames).encode()))
    records = list(load_capture(depth, snap, meta))
    assert [r["type"] for r in records][:2] == ["header", "snapshot"]
    states = list(replay_raw(records, OrderBookConfig(symbol="BTCUSDT")))
    # The second ws_open is a reconnect: the book is discarded and no later
    # snapshot exists, so only the first diff is published.
    assert [s.last_update_id for s in states] == [21]


def test_frame_reader_has_no_unimplemented_speed():
    import inspect

    from orderbook.storage import OrderBookReplay

    assert "speed" not in inspect.signature(OrderBookReplay.replay).parameters
