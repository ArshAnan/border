from dataclasses import dataclass, field


@dataclass

class OrderBookConfig:
    symbol: str = "BTCUSDT"

    # Binance URLs
    ws_base_url: str = "wss://stream.binance.us:9443/ws"
    rest_base_url: str = "https://api.binance.us"

    #Stream Settings
    depth_update_speed: str = "100ms"
    snapshot_depth_limit: int = 1000

    # Orderbook Settings
    num_levels: int = 20 # Number of price levels to maintain

    # Reconnection Settings
    max_reconnect_attempts: int = 10
    base_backoff_seconds: float = 1.0
    max_backoff_seconds: float = 60.0

    # Sync Settings
    # Events held while waiting for a snapshot, and events read from the
    # socket but not yet consumed. Exceeding either discards the book and
    # starts a fresh sync.
    max_buffer_events: int = 10_000
    # REST snapshot retries (429/418/5xx/network). Retry-After is honored
    # when present; otherwise the delay doubles up to the max.
    rest_max_attempts: int = 8
    rest_backoff_base_seconds: float = 0.5
    rest_backoff_max_seconds: float = 30.0

    # Storage Settings
    storage_dir: str = "data"
    snapshot_interval_seconds: float = 3600.00
    max_rows_per_file: int = 50000

    # Feature Settings
    num_price_returns: int = 10

    # Derived Properties
    @property
    def ws_stream_url(self) -> str:
        symbol = self.symbol.lower()
        return f"{self.ws_base_url}/{symbol}@depth@{self.depth_update_speed}"

    @property
    def rest_snapshot_url(self) -> str:
        return f"{self.rest_base_url}/api/v3/depth"