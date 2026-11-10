import logging
from dataclasses import dataclass

from sortedcontainers import SortedDict

from orderbook.client import DepthEvent, OrderBookSnapshot
from orderbook.config import OrderBookConfig

logger = logging.getLogger(__name__)


class SequenceGapError(RuntimeError):
    """An event starts after ``last_update_id + 1``; updates were missed."""


@dataclass
class PriceLevel:
    price: float
    quantity: float


@dataclass
class OrderBookState:
    bids: list[PriceLevel]
    asks: list[PriceLevel]
    last_update_id: int
    mid_price: float
    spread: float


class OrderBook:
    def __init__(self, config: OrderBookConfig):
        self.config = config
        self._bids = SortedDict()
        self._asks = SortedDict()
        self._last_update_id: int | None = None

    def initialize(self, snapshot: OrderBookSnapshot) -> None:
        self._bids.clear()
        self._asks.clear()

        for price, quantity in snapshot.bids:
            if quantity != 0.0:
                self._bids[-price] = quantity

        for price, quantity in snapshot.asks:
            if quantity != 0.0:
                self._asks[price] = quantity

        self._last_update_id = snapshot.last_update_id
        logger.info(
            f"Order book initialized - "
            f"last_update_id={self._last_update_id}, "
            f"bids={len(self._bids)}, asks={len(self._asks)}"
        )

    @property
    def last_update_id(self) -> int | None:
        return self._last_update_id

    def apply_event(self, event: DepthEvent) -> bool:
        """Apply one diff. Return False if it is already reflected in the book.

        An event with ``u <= last_update_id`` is ignored, so the update id
        never moves backward. An event with ``U > last_update_id + 1`` raises
        ``SequenceGapError`` and leaves the book unchanged.
        """
        if self._last_update_id is None:
            raise RuntimeError("apply_event called before initialize")
        if event.final_update_id <= self._last_update_id:
            return False
        if event.first_update_id > self._last_update_id + 1:
            raise SequenceGapError(
                f"gap: book at {self._last_update_id}, event starts at "
                f"{event.first_update_id}"
            )

        for price, quantity in event.bids:
            if quantity == 0.0:
                self._bids.pop(-price, None)
            else:
                self._bids[-price] = quantity

        for price, quantity in event.asks:
            if quantity == 0.0:
                self._asks.pop(price, None)
            else:
                self._asks[price] = quantity

        self._last_update_id = event.final_update_id
        return True

    def is_crossed(self) -> bool:
        """True when both sides exist and best bid >= best ask."""
        if not self._bids or not self._asks:
            return False
        return -self._bids.peekitem(0)[0] >= self._asks.peekitem(0)[0]

    def best_bid(self) -> PriceLevel | None:
        if not self._bids:
            return None
        neg_price, quantity = self._bids.peekitem(0)
        return PriceLevel(price=-neg_price, quantity=quantity)

    def best_ask(self) -> PriceLevel | None:
        if not self._asks:
            return None
        price, quantity = self._asks.peekitem(0)
        return PriceLevel(price=price, quantity=quantity)

    def mid_price(self) -> float | None:
        bid = self.best_bid()
        ask = self.best_ask()
        if bid is None or ask is None:
            return None
        return (bid.price + ask.price) / 2

    def spread(self) -> float | None:
        bid = self.best_bid()
        ask = self.best_ask()
        if bid is None or ask is None:
            return None
        return ask.price - bid.price

    def top_levels(self, n: int | None = None) -> OrderBookState:
        n = n or self.config.num_levels
        bids = [
            PriceLevel(price=-k, quantity=v)
            for k, v in self._bids.items()[:n]
        ]
        asks = [
            PriceLevel(price=k, quantity=v)
            for k, v in self._asks.items()[:n]
        ]

        mid = self.mid_price()
        sprd = self.spread()

        return OrderBookState(
            bids=bids,
            asks=asks,
            last_update_id=self._last_update_id or 0,
            mid_price=mid or 0.0,
            spread=sprd or 0.0,
        )

    def is_valid(self) -> bool:
        bid = self.best_bid()
        ask = self.best_ask()

        if bid is None or ask is None:
            return False

        if bid.price >= ask.price:
            logger.error(
                f"Invalid book state - best bid {bid.price} >= best ask {ask.price}"
            )
            return False

        return True