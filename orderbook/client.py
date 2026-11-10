import asyncio
import contextvars
import json
import logging
import time
from dataclasses import dataclass, field

import aiohttp
import websockets

from orderbook.config import OrderBookConfig

logger = logging.getLogger(__name__)


@dataclass
class DepthEvent:
    event_time: int
    symbol: str
    first_update_id: int
    final_update_id: int
    bids: list[tuple[float, float]]
    asks: list[tuple[float, float]]
    # Local receive time (time.time_ns, taken before parsing) and the frame
    # exactly as received. Set by stream_depth_events; used by the raw log.
    recv_ns: int = field(default=0, compare=False)
    raw: str | None = field(default=None, repr=False, compare=False)


@dataclass
class OrderBookSnapshot:
    last_update_id: int
    bids: list[tuple[float, float]]
    asks: list[tuple[float, float]]
    # Local receive time and the decoded REST body with its original strings.
    recv_ns: int = field(default=0, compare=False)
    raw: dict | None = field(default=None, repr=False, compare=False)


class _Reconnected:
    """Marker yielded by ``stream_depth_events`` when a new socket replaces a lost one."""

    def __repr__(self) -> str:
        return "RECONNECTED"


RECONNECTED = _Reconnected()


class SnapshotError(RuntimeError):
    """A REST snapshot could not be fetched.

    ``retryable`` is True when the failure was transient (429/418/5xx or a
    network error) and every attempt was used up.
    """

    def __init__(self, message: str, *, retryable: bool):
        super().__init__(message)
        self.retryable = retryable


def _parse_depth_event(raw: str, recv_ns: int = 0) -> DepthEvent:
    data = json.loads(raw)
    return DepthEvent(
        event_time=data["E"],
        symbol=data["s"],
        first_update_id=data["U"],
        final_update_id=data["u"],
        bids=[(float(p), float(q)) for p, q in data["b"]],
        asks=[(float(p), float(q)) for p, q in data["a"]],
        recv_ns=recv_ns,
        raw=raw,
    )


def parse_snapshot(data: dict, recv_ns: int = 0) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        last_update_id=data["lastUpdateId"],
        bids=[(float(p), float(q)) for p, q in data["bids"]],
        asks=[(float(p), float(q)) for p, q in data["asks"]],
        recv_ns=recv_ns,
        raw=data,
    )


async def stream_depth_events(config: OrderBookConfig):
    """Yield parsed depth diffs. Yield ``RECONNECTED`` before the first diff
    of every connection after the first, so the caller can rebuild its book."""
    attempt = 0
    connected_before = False

    while attempt < config.max_reconnect_attempts:
        try:
            logger.info(f"Connecting to {config.ws_stream_url} (attempt {attempt + 1})")

            async with websockets.connect(config.ws_stream_url) as ws:
                logger.info("Connected successfully")
                attempt = 0
                if connected_before:
                    yield RECONNECTED
                connected_before = True

                async for raw_message in ws:
                    recv_ns = time.time_ns()
                    if isinstance(raw_message, bytes):
                        raw_message = raw_message.decode()
                    yield _parse_depth_event(raw_message, recv_ns)

        except websockets.ConnectionClosed as e:
            logger.warning(f"Connection closed: {e}")

        except Exception as e:
            logger.error(f"Unexpected error: {e}")

        attempt += 1
        wait = min(
            config.base_backoff_seconds * (2 ** attempt),
            config.max_backoff_seconds
        )
        logger.info(f"Reconnecting in {wait:.1f} seconds...")
        await asyncio.sleep(wait)

    raise RuntimeError(f"Failed to connect after {config.max_reconnect_attempts} attempts")


class _SessionHolder:
    """One lazily created ``aiohttp.ClientSession`` shared by every snapshot fetch
    of a stream."""

    def __init__(self) -> None:
        self._session = None

    def get(self):
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        session, self._session = self._session, None
        close = getattr(session, "close", None)
        if close is not None:
            await close()


# Set by ``stream_book`` for the tasks it starts, so ``fetch_snapshot(config)``
# reuses the stream's session without changing its signature.
_session_holder: contextvars.ContextVar[_SessionHolder | None] = contextvars.ContextVar(
    "orderbook_session_holder", default=None
)

_RETRYABLE_STATUS = {418, 429}


def _retry_after_seconds(headers) -> float | None:
    value = headers.get("Retry-After") if headers is not None else None
    if value is None:
        return None
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return None
    return max(seconds, 0.0)


def _backoff(config: OrderBookConfig, attempt: int) -> float:
    return min(
        config.rest_backoff_base_seconds * (2 ** attempt),
        config.rest_backoff_max_seconds,
    )


async def fetch_snapshot(config: OrderBookConfig, session=None) -> OrderBookSnapshot:
    """Fetch a REST depth snapshot.

    429, 418 and 5xx responses and network errors are retried with bounded
    exponential backoff; ``Retry-After`` is honored when present. Other
    statuses raise ``SnapshotError(retryable=False)`` at once.
    """
    params = {
        "symbol": config.symbol,
        "limit": config.snapshot_depth_limit,
    }
    holder = _session_holder.get()
    own_session = None
    last_error = "no attempt made"

    try:
        for attempt in range(config.rest_max_attempts):
            if session is not None:
                current = session
            elif holder is not None:
                current = holder.get()
            else:
                if own_session is None:
                    own_session = aiohttp.ClientSession()
                current = own_session

            delay: float | None = None
            try:
                async with current.get(
                    config.rest_snapshot_url,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as response:
                    status = response.status
                    if status == 200:
                        data = await response.json()
                        return parse_snapshot(data, time.time_ns())
                    if status in _RETRYABLE_STATUS:
                        delay = _retry_after_seconds(response.headers)
                    elif status < 500:
                        raise SnapshotError(
                            f"REST snapshot failed with status {status}", retryable=False
                        )
                    last_error = f"status {status}"
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_error = f"{type(exc).__name__}: {exc}"

            if attempt + 1 >= config.rest_max_attempts:
                break
            wait = delay if delay is not None else _backoff(config, attempt)
            logger.warning(f"Snapshot fetch failed ({last_error}); retrying in {wait:.2f}s")
            await asyncio.sleep(wait)
    finally:
        if own_session is not None:
            close = getattr(own_session, "close", None)
            if close is not None:
                await close()

    raise SnapshotError(
        f"REST snapshot failed after {config.rest_max_attempts} attempts ({last_error})",
        retryable=True,
    )
