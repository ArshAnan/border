"""Drive local-book sync the way a caller can drive it today.

The README quickstart keeps the book outside the buffer, so the snapshot that
aligned the diffs is not the snapshot that seeds the book. When
``orderbook.sync.stream_book`` exists, this helper uses it and the same
assertions cover that lifecycle instead.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib
from dataclasses import dataclass, field
from enum import Enum, auto

from orderbook.book import OrderBook, OrderBookState
from orderbook.client import DepthEvent, OrderBookSnapshot
from orderbook.config import OrderBookConfig


class InitMode(Enum):
    """Which snapshot the legacy quickstart is allowed to seed from."""

    ALIGNING = auto()
    SECOND = auto()


@dataclass
class DriveResult:
    book: OrderBook
    events: list[DepthEvent] = field(default_factory=list)
    fetched: list[OrderBookSnapshot] = field(default_factory=list)
    views: list[tuple] = field(default_factory=list)

    @property
    def view(self) -> tuple | None:
        return self.views[-1] if self.views else None


def state_view(state: OrderBookState) -> tuple:
    return (
        state.last_update_id,
        tuple((level.price, level.quantity) for level in state.bids),
        tuple((level.price, level.quantity) for level in state.asks),
    )


def book_view(book: OrderBook, n: int | None = None) -> tuple:
    return state_view(book.top_levels(n))


def _stream_book():
    try:
        module = importlib.import_module("orderbook.sync")
    except ImportError:
        return None
    return getattr(module, "stream_book", None)


async def drive_quickstart(
    config: OrderBookConfig,
    *,
    mode: InitMode,
    fetch_snapshot,
    fetched: list[OrderBookSnapshot] | None = None,
    limit: int = 20,
    timeout: float = 0.5,
    until=None,
) -> DriveResult:
    book = OrderBook(config)
    result = DriveResult(book=book)
    if fetched is not None:
        result.fetched = fetched
    stream_book = _stream_book()

    async def consume() -> None:
        if stream_book is not None:
            async for state in stream_book(config):
                result.views.append(state_view(state))
                if len(result.views) >= limit:
                    return
                if until is not None and until(result):
                    return
            return

        from orderbook.buffer import DepthEventBuffer  # legacy API only

        initialized = False
        async for event in DepthEventBuffer(config).stream_validated_events():
            if not initialized:
                if mode is InitMode.SECOND:
                    snapshot = await fetch_snapshot(config)
                else:
                    if not result.fetched:
                        raise AssertionError("buffer yielded an event without a snapshot")
                    snapshot = result.fetched[0]
                book.initialize(snapshot)
                initialized = True
            book.apply_event(event)
            result.events.append(event)
            result.views.append(book_view(book))
            if len(result.events) >= limit:
                return
            if until is not None and until(result):
                return

    task = asyncio.create_task(consume())
    try:
        await asyncio.wait_for(task, timeout)
    except TimeoutError:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    return result
