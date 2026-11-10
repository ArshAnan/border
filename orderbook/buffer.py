"""Depth diff buffering, snapshot alignment and book ownership.

``BookSynchronizer`` is a pure state machine: it is fed depth events,
reconnect notices and snapshots, and it owns the buffer and the book, so the
snapshot that validates the buffer is the one that seeds the book. It does no
I/O, so live streaming and offline replay share it.

``stream_book`` is the live driver. A reader task buffers socket frames while
the REST snapshot is fetched in another task; nothing awaits REST inside the
read loop.

Sync rule (Binance.US / Binance spot "Managing a Local Order Book"):

* buffer diffs, fetch a snapshot with id ``L``;
* if ``L + 1`` is before the first buffered ``U``, fetch a new snapshot;
* drop buffered events with ``u <= L``;
* every event, buffered or live, is applied only if ``U <= book_id + 1``;
  ``u <= book_id`` is ignored and ``U > book_id + 1`` is a gap.

The first applied event therefore satisfies ``U <= L + 1 <= u``. The docs'
sentence that ``L`` should lie inside ``[U;u]`` is stricter by one id; an
event starting at exactly ``L + 1`` misses nothing and is accepted, which is
what the docs' own update procedure does.

On a gap, reconnect, crossed book or buffer overflow the book is discarded
and rebuilt from a fresh snapshot.
"""

import asyncio
import contextlib
import contextvars
import logging
from collections import deque
from dataclasses import dataclass
from enum import Enum, auto
from typing import AsyncGenerator

from orderbook import client as _client
from orderbook.book import OrderBook, OrderBookState
from orderbook.client import (
    RECONNECTED,
    DepthEvent,
    OrderBookSnapshot,
    SnapshotError,
    fetch_snapshot,
    stream_depth_events,
)
from orderbook.config import OrderBookConfig

logger = logging.getLogger(__name__)


class SyncState(Enum):
    BUFFERING = auto()
    SYNCED = auto()


@dataclass
class SyncStats:
    snapshots_applied: int = 0
    snapshots_rejected: int = 0
    events_applied: int = 0
    events_ignored: int = 0
    resyncs: int = 0
    gaps: int = 0
    reconnects: int = 0
    crossed: int = 0
    overflows: int = 0


class BookSynchronizer:
    """Owns the diff buffer, the aligning snapshot and the book."""

    def __init__(self, config: OrderBookConfig):
        self.config = config
        self.book: OrderBook | None = None
        self.state = SyncState.BUFFERING
        self.stats = SyncStats()
        self._buffer: deque[DepthEvent] = deque()
        self._failures = 0

    # --- inputs -----------------------------------------------------------

    @property
    def wants_snapshot(self) -> bool:
        """True while buffering with at least one event to align against."""
        return self.state is SyncState.BUFFERING and bool(self._buffer)

    @property
    def buffered(self) -> int:
        return len(self._buffer)

    def on_event(self, event: DepthEvent) -> OrderBookState | None:
        """Feed one diff. Return the new book state if it was applied."""
        if self.state is SyncState.BUFFERING:
            self._buffer_event(event)
            return None
        return self._apply(event)

    def on_reconnect(self) -> None:
        self.stats.reconnects += 1
        self.reset("reconnect")

    def on_overflow(self) -> None:
        self.stats.overflows += 1
        self.reset("buffer overflow")

    def on_snapshot(self, snapshot: OrderBookSnapshot) -> list[OrderBookState]:
        """Seed the book from ``snapshot`` and drain the buffer through it.

        Returns one state per applied buffered event. If the snapshot cannot
        align with the buffer (it is behind the first buffered event, or it is
        crossed) it is rejected and ``wants_snapshot`` stays True.
        """
        if self.state is not SyncState.BUFFERING:
            return []
        last_id = snapshot.last_update_id
        if self._buffer and last_id + 1 < self._buffer[0].first_update_id:
            self._reject(
                f"snapshot {last_id} is behind the first buffered event "
                f"U={self._buffer[0].first_update_id}"
            )
            return []

        book = OrderBook(self.config)
        book.initialize(snapshot)
        if book.is_crossed():
            self._reject(f"snapshot {last_id} is crossed")
            return []

        self.book = book
        self.state = SyncState.SYNCED
        self.stats.snapshots_applied += 1
        logger.info(
            f"Synced on snapshot {last_id} with {len(self._buffer)} buffered events"
        )

        pending = list(self._buffer)
        self._buffer.clear()
        states: list[OrderBookState] = []
        for event in pending:
            state = self.on_event(event)
            if state is not None:
                states.append(state)
        return states

    def note_snapshot_failure(self) -> None:
        self._failures += 1

    def reset(self, reason: str) -> None:
        """Discard the book and the buffer; the next event starts a new sync."""
        logger.warning(f"Resync: {reason}")
        self.book = None
        self.state = SyncState.BUFFERING
        self._buffer.clear()
        self.stats.resyncs += 1
        self._failures += 1

    def backoff_delay(self) -> float:
        """Delay before the next snapshot request. Zero for the first sync and
        the first resync; doubles while consecutive attempts keep failing."""
        if self._failures <= 1:
            return 0.0
        return min(
            self.config.rest_backoff_base_seconds * (2 ** (self._failures - 2)),
            self.config.rest_backoff_max_seconds,
        )

    # --- internals --------------------------------------------------------

    def _reject(self, reason: str) -> None:
        logger.warning(f"Snapshot rejected: {reason}")
        self.stats.snapshots_rejected += 1
        self._failures += 1

    def _buffer_event(self, event: DepthEvent) -> None:
        if len(self._buffer) >= self.config.max_buffer_events:
            self.on_overflow()
        self._buffer.append(event)

    def _apply(self, event: DepthEvent) -> OrderBookState | None:
        book = self.book
        assert book is not None and book.last_update_id is not None
        if event.final_update_id <= book.last_update_id:
            self.stats.events_ignored += 1
            return None
        if event.first_update_id > book.last_update_id + 1:
            self.stats.gaps += 1
            self.reset(
                f"gap: book at {book.last_update_id}, event U={event.first_update_id}"
            )
            self._buffer_event(event)
            return None

        book.apply_event(event)
        if book.is_crossed():
            self.stats.crossed += 1
            self.reset(f"crossed book after u={event.final_update_id}")
            return None

        self.stats.events_applied += 1
        self._failures = 0
        return book.top_levels()


_OVERFLOW = object()


async def _fetch_after(delay: float, config: OrderBookConfig) -> OrderBookSnapshot:
    if delay > 0:
        await asyncio.sleep(delay)
    # Looked up on this module at call time so tests can substitute it.
    return await fetch_snapshot(config)


async def stream_book(
    config: OrderBookConfig,
    synchronizer: BookSynchronizer | None = None,
) -> AsyncGenerator[OrderBookState, None]:
    """Yield the book state after every applied diff, forever.

    The book is rebuilt from a fresh snapshot after any gap, reconnect,
    crossed book or overflow. REST errors are retried with backoff and never
    end the stream. Pass ``synchronizer`` to inspect ``.book`` and ``.stats``.

    Slow consumers: frames read but not yet consumed are bounded by
    ``config.max_buffer_events``. When that bound is hit the unread frames are
    dropped and the book is rebuilt, so a consumer that cannot keep up sees a
    resync instead of unbounded memory growth.
    """
    sync = synchronizer or BookSynchronizer(config)
    pending: deque = deque()
    wake = asyncio.Event()

    holder = _client._SessionHolder()
    ctx = contextvars.copy_context()
    ctx.run(_client._session_holder.set, holder)

    async def reader() -> None:
        async for item in stream_depth_events(config):
            if len(pending) >= config.max_buffer_events:
                pending.clear()
                pending.append(_OVERFLOW)
            else:
                pending.append(item)
            wake.set()

    reader_task = asyncio.create_task(reader())
    reader_task.add_done_callback(lambda _: wake.set())
    snapshot_task: asyncio.Task | None = None

    try:
        while True:
            if snapshot_task is None and sync.wants_snapshot:
                snapshot_task = asyncio.create_task(
                    _fetch_after(sync.backoff_delay(), config), context=ctx
                )
                snapshot_task.add_done_callback(lambda _: wake.set())

            if snapshot_task is not None and snapshot_task.done():
                task, snapshot_task = snapshot_task, None
                try:
                    snapshot = task.result()
                except SnapshotError as exc:
                    if not exc.retryable:
                        raise
                    logger.warning(f"{exc}; will retry")
                    sync.note_snapshot_failure()
                    continue
                for state in sync.on_snapshot(snapshot):
                    yield state
                continue

            if pending:
                item = pending.popleft()
                if item is RECONNECTED:
                    sync.on_reconnect()
                elif item is _OVERFLOW:
                    sync.on_overflow()
                else:
                    state = sync.on_event(item)
                    if state is not None:
                        yield state
                continue

            if reader_task.done():
                reader_task.result()
                return

            wake.clear()
            await wake.wait()
    finally:
        for task in (reader_task, snapshot_task):
            if task is not None and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        await holder.close()
