import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from orderbook.book import OrderBookState
from orderbook.config import OrderBookConfig
from orderbook.features import feature_names

logger = logging.getLogger(__name__)


# --- Schema ---

META_COLUMNS = ("timestamp_ms", "last_update_id")


def _make_schema(config: OrderBookConfig) -> pa.Schema:
    """``timestamp_ms`` (local wall clock at record time), ``last_update_id``,
    then one float64 column per entry of ``feature_names(config)``."""
    fields = [
        pa.field("timestamp_ms", pa.int64()),
        pa.field("last_update_id", pa.int64()),
    ]
    fields += [pa.field(name, pa.float64()) for name in feature_names(config)]
    return pa.schema(fields)


# --- Recorder ---

class OrderBookRecorder:
    """Buffers top-N state frames and writes them to Parquet.

    Each row is one published state: its feature vector, its update id and
    the local wall clock at record time. These are derived frames, not raw
    diffs; use ``orderbook.replay.RawRecorder`` to keep the raw input.
    Files are written to a temp name and renamed, so a file on disk is
    complete. At most ``max_rows_per_file`` rows are held in memory.
    """

    def __init__(self, config: OrderBookConfig):
        self.config = config
        self._schema = _make_schema(config)
        self._names = feature_names(config)
        self._rows: list[dict] = []
        self._last_write_time = time.time()
        self._file_count = 0

        Path(config.storage_dir).mkdir(parents=True, exist_ok=True)
        logger.info(f"Recorder initialized — writing to {config.storage_dir}/")

    def record(self, state: OrderBookState, features: np.ndarray) -> None:
        if features.shape != (len(self._names),):
            raise ValueError(
                f"feature vector shape {features.shape} does not match layout of {len(self._names)}"
            )
        row = {
            "timestamp_ms": int(time.time() * 1000),
            "last_update_id": state.last_update_id,
        }
        row.update(zip(self._names, features.tolist()))
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
        tmp = filepath.with_name(filepath.name + ".tmp")

        table = pa.Table.from_pylist(self._rows, schema=self._schema)
        try:
            pq.write_table(table, tmp, compression="snappy")
            os.replace(tmp, filepath)
        finally:
            if tmp.exists():
                tmp.unlink()

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
        self._names = feature_names(config)

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
                missing = [n for n in self._names if n not in table.column_names]
                if missing:
                    raise ValueError(
                        f"{filepath} does not match the feature layout for "
                        f"num_levels={self.config.num_levels}, "
                        f"num_price_returns={self.config.num_price_returns}; "
                        f"missing {len(missing)} columns, e.g. {missing[:3]}"
                    )
                matrix = np.column_stack(
                    [table.column(n).to_numpy().astype(np.float64) for n in self._names]
                )
                timestamps = table.column("timestamp_ms").to_pylist()
                update_ids = table.column("last_update_id").to_pylist()

                for i in range(table.num_rows):
                    yield ReplayFrame(
                        timestamp_ms=timestamps[i],
                        last_update_id=update_ids[i],
                        features=matrix[i].copy(),
                    )

            if not loop:
                break

            logger.info("Replay complete — looping...")