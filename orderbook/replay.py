"""Raw event log and deterministic offline replay.

``RawRecorder`` logs exactly what the synchronizer consumes, in the order it
consumes it: raw depth frames with their local receive time, every REST
snapshot body, reconnects and overflows. ``replay_raw`` feeds such a log
through a fresh ``BookSynchronizer`` with no network, so it publishes the
same states the live run did.

Format: gzip JSONL segments, one JSON object per line, with the line types of
``bench/capture_raw.py``::

    {"type": "header", "format": "border-raw/1", "symbol": ..., "session": <ns>, ...}
    {"type": "depth", "recv_ns": <time.time_ns>, "raw": "<frame as received>"}
    {"type": "snapshot", "recv_ns": <time.time_ns>, "body": {<REST body>}}
    {"type": "ws_open", "recv_ns": ...}     # a new socket replaced a lost one
    {"type": "overflow", "recv_ns": ...}    # unread frames were dropped

Each segment holds at most ``max_records`` records after its header and is
written to a temporary file, fsynced, then renamed into place, so every
``*.jsonl.gz`` on disk is complete. A crash loses at most the unflushed
records held in memory.

Command line: ``python -m orderbook.replay [--num-levels N] SEGMENT...``
prints ``states=<n> sha256=<digest>`` over the published states.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import itertools
import json
import os
import time
from collections.abc import Iterable, Iterator
from pathlib import Path

from orderbook.book import OrderBookState
from orderbook.buffer import BookSynchronizer
from orderbook.client import DepthEvent, OrderBookSnapshot, _parse_depth_event, parse_snapshot
from orderbook.config import OrderBookConfig

FORMAT = "border-raw/1"


def _levels(levels: list[tuple[float, float]]) -> list[list[str]]:
    return [[repr(price), repr(qty)] for price, qty in levels]


class RawRecorder:
    """Bounded, atomically flushed raw input log for one symbol."""

    def __init__(
        self,
        config: OrderBookConfig,
        directory: str | Path | None = None,
        *,
        max_records: int = 5_000,
    ):
        if max_records < 1:
            raise ValueError("max_records must be positive")
        self.config = config
        self.directory = Path(directory or Path(config.storage_dir) / "raw")
        self.directory.mkdir(parents=True, exist_ok=True)
        self.max_records = max_records
        self.session = time.time_ns()
        self._stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(self.session / 1e9))
        self._segment = 0
        self._records: list[dict] = []

    # --- inputs, in the order the synchronizer sees them -------------------

    def depth(self, event: DepthEvent) -> None:
        raw = event.raw
        if raw is None:
            raw = json.dumps(
                {
                    "e": "depthUpdate",
                    "E": event.event_time,
                    "s": event.symbol,
                    "U": event.first_update_id,
                    "u": event.final_update_id,
                    "b": _levels(event.bids),
                    "a": _levels(event.asks),
                },
                separators=(",", ":"),
            )
        self._add({"type": "depth", "recv_ns": event.recv_ns or time.time_ns(), "raw": raw})

    def snapshot(self, snapshot: OrderBookSnapshot) -> None:
        body = snapshot.raw
        if body is None:
            body = {
                "lastUpdateId": snapshot.last_update_id,
                "bids": _levels(snapshot.bids),
                "asks": _levels(snapshot.asks),
            }
        self._add({"type": "snapshot", "recv_ns": snapshot.recv_ns or time.time_ns(), "body": body})

    def reconnect(self) -> None:
        self._add({"type": "ws_open", "recv_ns": time.time_ns()})

    def overflow(self) -> None:
        self._add({"type": "overflow", "recv_ns": time.time_ns()})

    # --- output ------------------------------------------------------------

    def flush(self) -> Path | None:
        if not self._records:
            return None
        header = {
            "type": "header",
            "format": FORMAT,
            "symbol": self.config.symbol,
            "session": self.session,
            "segment": self._segment,
            "ws_url": self.config.ws_stream_url,
            "rest_snapshot_url": self.config.rest_snapshot_url,
            "snapshot_depth_limit": self.config.snapshot_depth_limit,
            "depth_update_speed": self.config.depth_update_speed,
            "recv_clock": "time.time_ns",
        }
        name = f"{self.config.symbol}_raw_{self._stamp}_{self._segment:06d}.jsonl.gz"
        final = self.directory / name
        tmp = final.with_name(final.name + ".tmp")
        payload = "".join(
            json.dumps(r, separators=(",", ":"), ensure_ascii=False) + "\n"
            for r in [header, *self._records]
        ).encode()
        try:
            with open(tmp, "wb") as fh:
                fh.write(gzip.compress(payload, mtime=0))
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, final)
        finally:
            if tmp.exists():
                tmp.unlink()
        self._records = []
        self._segment += 1
        return final

    def close(self) -> None:
        self.flush()

    def _add(self, record: dict) -> None:
        self._records.append(record)
        if len(self._records) >= self.max_records:
            self.flush()


def read_segments(paths: Iterable[str | Path]) -> Iterator[dict]:
    """Yield records from gzip JSONL segments in the order given."""
    for path in paths:
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    yield json.loads(line)


def load_capture(
    depth_path: str | Path,
    snapshot_path: str | Path,
    meta_path: str | Path | None = None,
) -> Iterator[dict]:
    """Records from a ``bench/capture_raw.py`` capture.

    That script takes one REST snapshot before opening the socket, so the
    snapshot is placed right after the header. Replay can only publish states
    if the first diff bridges it (``U <= lastUpdateId + 1``) and only until
    the first reconnect, after which no snapshot exists.
    """
    body = json.loads(Path(snapshot_path).read_text())
    recv_ns = 0
    if meta_path is not None:
        recv_ns = int(json.loads(Path(meta_path).read_text()).get("response_ns") or 0)
    records = read_segments([depth_path])
    for record in records:
        yield record
        if record.get("type") == "header":
            break
    yield {"type": "snapshot", "recv_ns": recv_ns, "body": body}
    yield from records


def replay_raw(
    source: Iterable[str | Path] | Iterable[dict],
    config: OrderBookConfig | None = None,
    synchronizer: BookSynchronizer | None = None,
) -> Iterator[OrderBookState]:
    """Re-apply a raw log through a fresh synchronizer. No network is used.

    ``source`` is a list of segment paths (replayed in the order given) or an
    iterable of records. A ``ws_open`` after the first connection, or a new
    recording session, discards the book exactly as a live reconnect does.
    Timing is not reproduced; states are yielded as fast as they are computed.
    Use the same ``num_levels`` and ``max_buffer_events`` as the live run to
    get identical states.
    """
    stream = iter(source)
    first = next(stream, None)
    if first is None:
        return
    chained = itertools.chain([first], stream)
    records = chained if isinstance(first, dict) else read_segments(chained)
    sync = synchronizer or BookSynchronizer(config or OrderBookConfig())
    session = None
    seen_connection = False
    for record in records:
        kind = record.get("type")
        if kind == "header":
            new_session = record.get("session", record.get("started_ns"))
            if session is not None and new_session != session:
                sync.on_reconnect()
            session = new_session
        elif kind == "depth":
            seen_connection = True
            state = sync.on_event(_parse_depth_event(record["raw"], record.get("recv_ns", 0)))
            if state is not None:
                yield state
        elif kind == "snapshot":
            yield from sync.on_snapshot(parse_snapshot(record["body"], record.get("recv_ns", 0)))
        elif kind == "ws_open":
            if seen_connection:
                sync.on_reconnect()
            seen_connection = True
        elif kind == "overflow":
            sync.on_overflow()
        # ws_error and unknown record types carry no book input.


def state_digest(states: Iterable[OrderBookState]) -> str:
    """SHA-256 over a canonical encoding of each state (ids, levels, mid, spread)."""
    digest = hashlib.sha256()
    for state in states:
        frame = (
            state.last_update_id,
            [(level.price.hex(), level.quantity.hex()) for level in state.bids],
            [(level.price.hex(), level.quantity.hex()) for level in state.asks],
            float(state.mid_price).hex(),
            float(state.spread).hex(),
        )
        digest.update(json.dumps(frame, separators=(",", ":")).encode())
        digest.update(b"\n")
    return digest.hexdigest()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Replay raw depth segments offline.")
    parser.add_argument("segments", nargs="+")
    parser.add_argument("--num-levels", type=int, default=OrderBookConfig.num_levels)
    args = parser.parse_args(argv)
    config = OrderBookConfig(num_levels=args.num_levels)
    states = list(replay_raw([Path(p) for p in args.segments], config))
    print(f"states={len(states)} sha256={state_digest(states)}")


if __name__ == "__main__":
    main()
