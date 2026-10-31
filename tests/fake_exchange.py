"""In-process Binance.US depth exchange for sync tests.

The exchange keeps a true book, emits ``depthUpdate`` diffs with consistent
``U``/``u`` ids, and can serve a REST snapshot of that book. Faults are
explicit so a test can force the case it cares about.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from orderbook.client import DepthEvent, OrderBookSnapshot

_RECONNECT = object()
_CLOSE = object()


@dataclass
class RestScript:
    status: int
    payload: dict | None
    retry_after: str | None = None


@dataclass
class FakeExchange:
    """True book plus a scripted depth stream and REST snapshot queue."""

    symbol: str = "BTCUSDT"
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)
    rest_script: list[RestScript] = field(default_factory=list)
    emitted: list[DepthEvent] = field(default_factory=list)
    reconnects: int = 0

    def __post_init__(self) -> None:
        self._next_id = 1
        self._history: dict[int, tuple[dict[float, float], dict[float, float]]] = {}
        self._inbox: asyncio.Queue = asyncio.Queue()
        self._remember(0)

    @property
    def last_update_id(self) -> int:
        return self._next_id - 1

    def seed(
        self,
        bids: list[tuple[float, float]],
        asks: list[tuple[float, float]],
        last_update_id: int = 0,
    ) -> None:
        self.bids = {float(price): float(qty) for price, qty in bids if qty != 0.0}
        self.asks = {float(price): float(qty) for price, qty in asks if qty != 0.0}
        self._next_id = last_update_id + 1
        self._history.clear()
        self._remember(last_update_id)

    def emit(
        self,
        bids: list[tuple[float, float]] = (),
        asks: list[tuple[float, float]] = (),
        *,
        n_ids: int = 1,
    ) -> DepthEvent:
        """Apply a diff and emit it with the next ``n_ids`` update ids."""
        if n_ids < 1:
            raise ValueError("n_ids must be positive")
        first = self._next_id
        final = first + n_ids - 1
        return self.push(first, final, bids, asks, apply=True)

    def push(
        self,
        first_id: int,
        final_id: int,
        bids: list[tuple[float, float]] = (),
        asks: list[tuple[float, float]] = (),
        *,
        apply: bool = True,
    ) -> DepthEvent:
        """Enqueue a diff. ``apply=False`` leaves the true book unchanged."""
        if final_id < first_id:
            raise ValueError("final update id is before the first update id")
        if apply:
            self._apply(bids, asks)
            self._next_id = max(self._next_id, final_id + 1)
            self._remember(final_id)
        event = DepthEvent(
            event_time=1_700_000_000_000 + final_id,
            symbol=self.symbol,
            first_update_id=first_id,
            final_update_id=final_id,
            bids=list(bids),
            asks=list(asks),
        )
        self.emitted.append(event)
        self._inbox.put_nowait(event)
        return event

    def hide(
        self,
        bids: list[tuple[float, float]] = (),
        asks: list[tuple[float, float]] = (),
        *,
        n_ids: int = 1,
    ) -> int:
        """Change the true book and consume ids without emitting a diff."""
        if n_ids < 1:
            raise ValueError("n_ids must be positive")
        self._apply(bids, asks)
        final = self._next_id + n_ids - 1
        self._next_id = final + 1
        self._remember(final)
        return final

    def emit_gap(
        self,
        missed: int,
        bids: list[tuple[float, float]] = (),
        asks: list[tuple[float, float]] = (),
    ) -> DepthEvent:
        """Skip ``missed`` ids, then emit the next diff. The skipped ids are a hole."""
        if missed < 1:
            raise ValueError("missed must be positive")
        self._next_id += missed
        return self.emit(bids, asks)

    def emit_duplicate(self) -> DepthEvent:
        if not self.emitted:
            raise RuntimeError("no event to duplicate")
        event = self.emitted[-1]
        self._inbox.put_nowait(event)
        return event

    def emit_stale(self) -> DepthEvent:
        if not self.emitted:
            raise RuntimeError("no event to replay")
        event = self.emitted[0]
        self._inbox.put_nowait(event)
        return event

    def emit_out_of_order(
        self,
        first: tuple[list[tuple[float, float]], list[tuple[float, float]]] = ((), ()),
        second: tuple[list[tuple[float, float]], list[tuple[float, float]]] = ((), ()),
    ) -> tuple[DepthEvent, DepthEvent]:
        """Queue the later id before the earlier id."""
        earlier = self._take_next(first[0], first[1])
        later = self._take_next(second[0], second[1])
        self._inbox.put_nowait(later)
        self._inbox.put_nowait(earlier)
        self.emitted.append(later)
        self.emitted.append(earlier)
        return later, earlier

    def emit_crossed(self) -> DepthEvent:
        """Emit a diff that lifts the best bid to the best ask or above it."""
        if not self.asks:
            self._apply((), [(101.0, 1.0)])
        crossed_price = min(self.asks)
        return self.emit(bids=[(crossed_price, 1.0)])

    def inject_reconnect(self) -> None:
        """Drop the socket the way the real client hides a reconnect.

        The depth generator stays open. Events queued after this sentinel are
        the first frames of the new connection.
        """
        self._inbox.put_nowait(_RECONNECT)

    def close_stream(self) -> None:
        self._inbox.put_nowait(_CLOSE)

    def snapshot(
        self,
        last_update_id: int | None = None,
        *,
        ahead_by: int = 0,
    ) -> OrderBookSnapshot:
        """Return a depth snapshot.

        ``ahead_by`` publishes an id past every buffered event while keeping
        the latest known levels. That is the "snapshot ahead of the buffer"
        fault. A historical id returns the book remembered at that id.
        """
        if last_update_id is None:
            uid = self.last_update_id + ahead_by
        else:
            uid = last_update_id + ahead_by
        if uid in self._history and ahead_by == 0:
            bids, asks = self._history[uid]
        else:
            bids, asks = self.bids, self.asks
        return OrderBookSnapshot(
            last_update_id=uid,
            bids=sorted(bids.items(), key=lambda level: -level[0]),
            asks=sorted(asks.items()),
        )

    def queue_snapshot(
        self,
        snapshot: OrderBookSnapshot | None = None,
        *,
        status: int = 200,
        retry_after: str | None = None,
        ahead_by: int = 0,
        last_update_id: int | None = None,
    ) -> RestScript:
        if status == 200:
            snap = snapshot or self.snapshot(
                last_update_id=last_update_id, ahead_by=ahead_by
            )
            payload = {
                "lastUpdateId": snap.last_update_id,
                "bids": [[repr(price), repr(qty)] for price, qty in snap.bids],
                "asks": [[repr(price), repr(qty)] for price, qty in snap.asks],
            }
        else:
            payload = {"code": status, "msg": "scripted rest error"}
        script = RestScript(status=status, payload=payload, retry_after=retry_after)
        self.rest_script.append(script)
        return script

    async def stream(self, config) -> object:
        """Stand-in for ``orderbook.client.stream_depth_events``."""
        while True:
            item = await self._inbox.get()
            if item is _RECONNECT:
                self.reconnects += 1
                continue
            if item is _CLOSE:
                return
            yield item

    def _take_next(
        self,
        bids: list[tuple[float, float]],
        asks: list[tuple[float, float]],
    ) -> DepthEvent:
        first = self._next_id
        final = first
        self._apply(bids, asks)
        self._next_id = final + 1
        self._remember(final)
        return DepthEvent(
            event_time=1_700_000_000_000 + final,
            symbol=self.symbol,
            first_update_id=first,
            final_update_id=final,
            bids=list(bids),
            asks=list(asks),
        )

    def _apply(
        self,
        bids: list[tuple[float, float]],
        asks: list[tuple[float, float]],
    ) -> None:
        for price, qty in bids:
            if qty == 0.0:
                self.bids.pop(float(price), None)
            else:
                self.bids[float(price)] = float(qty)
        for price, qty in asks:
            if qty == 0.0:
                self.asks.pop(float(price), None)
            else:
                self.asks[float(price)] = float(qty)

    def _remember(self, update_id: int) -> None:
        self._history[update_id] = (dict(self.bids), dict(self.asks))


class _RestResponse:
    def __init__(self, status: int, payload: dict | None, headers: dict[str, str]):
        self.status = status
        self._payload = payload
        self.headers = headers

    async def json(self) -> dict | None:
        return self._payload

    async def __aenter__(self) -> _RestResponse:
        return self

    async def __aexit__(self, *exc) -> bool:
        return False


def client_session_factory(exchange: FakeExchange):
    """``aiohttp.ClientSession`` replacement that serves ``exchange.rest_script``."""

    created: list[object] = []

    class ScriptedSession:
        def __init__(self, *args, **kwargs) -> None:
            created.append(self)

        async def __aenter__(self) -> ScriptedSession:
            return self

        async def __aexit__(self, *exc) -> bool:
            return False

        def get(self, url, params=None, timeout=None) -> _RestResponse:
            if not exchange.rest_script:
                raise AssertionError("REST snapshot requested with an empty script")
            script = exchange.rest_script.pop(0)
            headers = {}
            if script.retry_after is not None:
                headers["Retry-After"] = script.retry_after
            return _RestResponse(script.status, script.payload, headers)

    ScriptedSession.created = created
    return ScriptedSession
