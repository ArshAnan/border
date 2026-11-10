import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from orderbook.book import OrderBookState
from orderbook.config import OrderBookConfig

logger = logging.getLogger(__name__)


# --- Schema ---

def _make_schema(config: OrderBookConfig) -> pa.Schema:
    n = config.num_levels
    fields = [
        pa.field("timestamp_ms", pa.int64()),
        pa.field("last_update_id", pa.int64()),
        pa.field("mid_price", pa.float64()),
        pa.field("spread", pa.float64()),
    ]

    for i in range(n):
        fields.append(pa.field(f"bid_price_{i}", pa.float64()))
        fields.append(pa.field(f"bid_qty_{i}", pa.float64()))
        fields.append(pa.field(f"ask_price_{i}", pa.float64()))
        fields.append(pa.field(f"ask_qty_{i}", pa.float64()))

    fields.append(pa.field("imbalance", pa.float64()))
    fields.append(pa.field("weighted_mid", pa.float64()))
    fields.append(pa.field("spread_bps", pa.float64()))

    for i in range(n):
        fields.append(pa.field(f"cum_bid_vol_{i}", pa.float64()))
        fields.append(pa.field(f"cum_ask_vol_{i}", pa.float64()))

    return pa.schema(fields)


# --- Recorder ---

class OrderBookRecorder:
    def __init__(self, config: OrderBookConfig):
        self.config = config
        self._schema = _make_schema(config)
        self._rows: list[dict] = []
        self._last_write_time = time.time()
        self._file_count = 0

        Path(config.storage_dir).mkdir(parents=True, exist_ok=True)
        logger.info(f"Recorder initialized — writing to {config.storage_dir}/")

    def record(self, state: OrderBookState, features: np.ndarray) -> None:
        n = self.config.num_levels
        derived_start = 6 + 4 * n

        row = {
            "timestamp_ms": int(time.time() * 1000),
            "last_update_id": state.last_update_id,
            "mid_price": state.mid_price,
            "spread": state.spread,
        }

        for i in range(n):
            row[f"bid_price_{i}"] = state.bids[i].price if i < len(state.bids) else 0.0
            row[f"bid_qty_{i}"] = state.bids[i].quantity if i < len(state.bids) else 0.0
            row[f"ask_price_{i}"] = state.asks[i].price if i < len(state.asks) else 0.0
            row[f"ask_qty_{i}"] = state.asks[i].quantity if i < len(state.asks) else 0.0

        row["imbalance"] = float(features[derived_start])
        row["weighted_mid"] = float(features[derived_start + 1])
        row["spread_bps"] = float(features[derived_start + 2])

        for i in range(n):
            row[f"cum_bid_vol_{i}"] = float(features[derived_start + 3 + i])
            row[f"cum_ask_vol_{i}"] = float(features[derived_start + 3 + n + i])

        self._rows.append(row)

        now = time.time()
        if (now - self._last_write_time >= self.config.snapshot_interval_seconds
                or len(self._rows) >= self.config.max_rows_per_file):
            self._flush()
            self._last_write_time = now

    def _flush(self) -> None:
        if not self._rows:
            return

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"{self.config.symbol}_{timestamp}_{self._file_count:04d}.parquet"
        filepath = Path(self.config.storage_dir) / filename

        table = pa.Table.from_pylist(self._rows, schema=self._schema)
        pq.write_table(table, filepath, compression="snappy")

        logger.info(f"Flushed {len(self._rows)} rows to {filepath}")
        self._rows = []
        self._file_count += 1

    def close(self) -> None:
        self._flush()
        logger.info("Recorder closed")


# --- Replay Engine ---

@dataclass
class ReplayFrame:
    timestamp_ms: int
    last_update_id: int
    features: np.ndarray


class OrderBookReplay:
    """Reads recorded top-N state frames back in file order.

    This re-reads what ``OrderBookRecorder`` wrote; it does not rebuild a
    book. For deterministic replay from raw diffs use ``orderbook.replay``.
    """

    def __init__(self, config: OrderBookConfig, data_dir: str | None = None):
        self.config = config
        self._data_dir = Path(data_dir or config.storage_dir)
        self._schema = _make_schema(config)

    def list_files(self, symbol: str | None = None) -> list[Path]:
        symbol = symbol or self.config.symbol
        files = sorted(self._data_dir.glob(f"{symbol}_*.parquet"))
        logger.info(f"Found {len(files)} Parquet files for {symbol}")
        return files

    def replay(
        self,
        files: list[Path] | None = None,
        loop: bool = False,
    ):
        files = files or self.list_files()

        if not files:
            raise RuntimeError(f"No Parquet files found in {self._data_dir}")

        while True:
            for filepath in files:
                logger.info(f"Replaying {filepath}")
                table = pq.read_table(filepath)
                df = table.to_pydict()
                n_rows = len(df["timestamp_ms"])

                for i in range(n_rows):
                    feature_cols = [
                        col for col in df
                        if col not in ("timestamp_ms", "last_update_id")
                    ]
                    features = np.array(
                        [df[col][i] for col in feature_cols],
                        dtype=np.float64
                    )

                    yield ReplayFrame(
                        timestamp_ms=df["timestamp_ms"][i],
                        last_update_id=df["last_update_id"][i],
                        features=features,
                    )

            if not loop:
                break

            logger.info("Replay complete — looping...")