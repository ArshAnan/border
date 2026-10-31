"""Naive order book used as the sync oracle.

Prices live in plain dicts. The top of book is produced by sorting on read.
"""

from __future__ import annotations

from orderbook.client import DepthEvent, OrderBookSnapshot


class ReferenceBook:
    """Absolute-quantity book with the Binance.US continuity rules.

    An event whose final id is already at or behind ``last_update_id`` is
    ignored. An event whose first id is more than one past the book id is a
    gap and is refused. Every other event replaces the touched levels.
    """

    def __init__(self) -> None:
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.last_update_id: int | None = None

    def initialize(self, snapshot: OrderBookSnapshot) -> None:
        self.bids = {price: qty for price, qty in snapshot.bids if qty != 0.0}
        self.asks = {price: qty for price, qty in snapshot.asks if qty != 0.0}
        self.last_update_id = snapshot.last_update_id

    def apply_event(self, event: DepthEvent) -> bool:
        """Apply one diff. Return False when the event is a real gap."""
        if self.last_update_id is None:
            raise RuntimeError("reference book has no snapshot")
        if event.final_update_id <= self.last_update_id:
            return True
        if event.first_update_id > self.last_update_id + 1:
            return False
        self._apply(event.bids, event.asks)
        self.last_update_id = event.final_update_id
        return True

    def _apply(
        self,
        bids: list[tuple[float, float]],
        asks: list[tuple[float, float]],
    ) -> None:
        for price, qty in bids:
            if qty == 0.0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = qty
        for price, qty in asks:
            if qty == 0.0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = qty

    def top(self, n: int | None = None) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
        bids = sorted(self.bids.items(), key=lambda level: -level[0])
        asks = sorted(self.asks.items(), key=lambda level: level[0])
        if n is not None:
            bids = bids[:n]
            asks = asks[:n]
        return bids, asks

    @property
    def crossed(self) -> bool:
        if not self.bids or not self.asks:
            return False
        return max(self.bids) >= min(self.asks)

    def view(self, n: int | None = None) -> tuple:
        bids, asks = self.top(n)
        return (self.last_update_id, tuple(bids), tuple(asks))
