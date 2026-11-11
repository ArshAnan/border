import logging
from collections import deque

import numpy as np

from orderbook.book import OrderBookState
from orderbook.config import OrderBookConfig

logger = logging.getLogger(__name__)


class FeatureExtractor:
    def __init__(self, config: OrderBookConfig):
        self.config = config
        self._mid_price_history: deque[float] = deque(
            maxlen=config.num_price_returns + 1
        )

    def update(self, state: OrderBookState) -> np.ndarray:
        self._mid_price_history.append(state.mid_price)
        return self._extract(state)

    def _extract(self, state: OrderBookState) -> np.ndarray:
        n = self.config.num_price_returns
        config = self.config
        num_levels = config.num_levels

        bids = state.bids[:num_levels]
        asks = state.asks[:num_levels]

        # --- Level 1: top of book ---
        best_bid_price = bids[0].price if bids else 0.0
        best_bid_qty   = bids[0].quantity if bids else 0.0
        best_ask_price = asks[0].price if asks else 0.0
        best_ask_qty   = asks[0].quantity if asks else 0.0
        mid_price      = state.mid_price
        spread         = state.spread

        # --- Level 2: depth ---
        bid_prices = np.array([b.price    for b in bids] + [0.0] * (num_levels - len(bids)))
        bid_qtys   = np.array([b.quantity for b in bids] + [0.0] * (num_levels - len(bids)))
        ask_prices = np.array([a.price    for a in asks] + [0.0] * (num_levels - len(asks)))
        ask_qtys   = np.array([a.quantity for a in asks] + [0.0] * (num_levels - len(asks)))

        # --- Level 3: derived ---
        total_bid_vol = float(np.sum(bid_qtys))
        total_ask_vol = float(np.sum(ask_qtys))
        total_vol     = total_bid_vol + total_ask_vol
        imbalance     = total_bid_vol / total_vol if total_vol > 0 else 0.5

        denom        = best_bid_qty + best_ask_qty
        weighted_mid = (
            (best_bid_price * best_ask_qty + best_ask_price * best_bid_qty) / denom
            if denom > 0 else mid_price
        )

        spread_bps  = (spread / mid_price * 10000) if mid_price > 0 else 0.0
        cum_bid_vol = np.cumsum(bid_qtys)
        cum_ask_vol = np.cumsum(ask_qtys)

        # --- Level 4: recent price returns ---
        history = list(self._mid_price_history)
        if len(history) < 2:
            returns = np.zeros(n, dtype=np.float64)
        else:
            raw_returns = [
                (history[i] - history[i - 1]) / history[i - 1]
                if history[i - 1] != 0 else 0.0
                for i in range(1, len(history))
            ]
            # pad with zeros if not enough history yet
            padding = n - len(raw_returns)
            returns = np.array([0.0] * padding + raw_returns, dtype=np.float64)

        # --- Assemble ---
        features = np.concatenate([
            [best_bid_price, best_bid_qty,
             best_ask_price, best_ask_qty,
             mid_price, spread],
            bid_prices, bid_qtys,
            ask_prices, ask_qtys,
            [imbalance, weighted_mid, spread_bps],
            cum_bid_vol,
            cum_ask_vol,
            returns,
        ], dtype=np.float64)

        if features.shape[0] != feature_size(config):
            raise AssertionError(
                f"feature vector has {features.shape[0]} values, layout has {feature_size(config)}"
            )
        return features

    def reset(self) -> None:
        self._mid_price_history.clear()


def feature_names(config: OrderBookConfig) -> list[str]:
    """The feature vector layout. This list is the single source of truth:
    ``FeatureExtractor`` fills it in this order, the Parquet recorder writes
    one column per name, and ``OrderBookReplay`` rebuilds the vector from
    those columns in this order."""
    n = config.num_levels
    return [
        "best_bid_price", "best_bid_qty", "best_ask_price", "best_ask_qty",
        "mid_price", "spread",
        *(f"bid_price_{i}" for i in range(n)),
        *(f"bid_qty_{i}" for i in range(n)),
        *(f"ask_price_{i}" for i in range(n)),
        *(f"ask_qty_{i}" for i in range(n)),
        "imbalance", "weighted_mid", "spread_bps",
        *(f"cum_bid_vol_{i}" for i in range(n)),
        *(f"cum_ask_vol_{i}" for i in range(n)),
        *(f"return_{i}" for i in range(config.num_price_returns)),
    ]


def feature_size(config: OrderBookConfig) -> int:
    return len(feature_names(config))