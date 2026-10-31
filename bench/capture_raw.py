#!/usr/bin/env python3
"""Record raw Binance.US depth messages and the opening REST snapshot.

Endpoints come from ``orderbook.config.OrderBookConfig``. This script does not
fall back to Binance.com or any other host. If a snapshot is not HTTP 200, it
exits before opening the WebSocket.

Output (gitignored):
  bench/data/{SYMBOL}_snapshot.json       raw REST body
  bench/data/{SYMBOL}_snapshot.meta.json  status and timestamps
  bench/data/{SYMBOL}_depth.jsonl.gz      compressed JSONL of raw WS messages

Each depth line is ``{"type":"depth","recv_ns":<time.time_ns>,"raw":"<frame>"}``.
``recv_ns`` is taken immediately after the frame is received, before parsing.
"""

from __future__ import annotations

import argparse
import asyncio
import ctypes
import ctypes.util
import gzip
import json
import logging
import os
import platform
import signal
import socket
import struct
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import websockets

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from orderbook.config import OrderBookConfig  # noqa: E402

DATA_DIR = ROOT / "bench" / "data"
RESULTS_DIR = ROOT / "bench" / "results"
ET = ZoneInfo("America/New_York")
NTP_HOSTS = ("time.google.com", "time.cloudflare.com", "pool.ntp.org")

logger = logging.getLogger("capture_raw")


class GzipJsonl:
    """Append-only gzip JSONL.

    The member is closed every ``rotate_every`` records (and after the first)
    so a reader can decompress the file while the capture is still running.
    """

    def __init__(self, path: Path, rotate_every: int = 200):
        self.path = path
        self.rotate_every = rotate_every
        self.count = 0
        path.parent.mkdir(parents=True, exist_ok=True)
        self._fp = gzip.open(path, "wb")

    def write(self, obj: dict) -> None:
        line = json.dumps(obj, separators=(",", ":"), ensure_ascii=False) + "\n"
        self._fp.write(line.encode())
        self.count += 1
        # Close early members so the snapshot of the first depth frame is a
        # valid gzip stream while the process is still running.
        if self.count <= 3 or self.count % self.rotate_every == 0:
            self._fp.close()
            self._fp = gzip.open(self.path, "ab")
        else:
            self._fp.flush()

    def close(self) -> None:
        if not self._fp.closed:
            self._fp.close()


def _adjtimex() -> dict:
    libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)

    class Timex(ctypes.Structure):
        _fields_ = [
            ("modes", ctypes.c_uint),
            ("offset", ctypes.c_long),
            ("freq", ctypes.c_long),
            ("maxerror", ctypes.c_long),
            ("esterror", ctypes.c_long),
            ("status", ctypes.c_int),
            ("constant", ctypes.c_long),
            ("precision", ctypes.c_long),
            ("tolerance", ctypes.c_long),
            ("time_tv_sec", ctypes.c_long),
            ("time_tv_usec", ctypes.c_long),
            ("tick", ctypes.c_long),
            ("ppsfreq", ctypes.c_long),
            ("jitter", ctypes.c_long),
            ("shift", ctypes.c_int),
            ("stabil", ctypes.c_long),
            ("jitcnt", ctypes.c_long),
            ("calcnt", ctypes.c_long),
            ("errcnt", ctypes.c_long),
            ("stbcnt", ctypes.c_long),
            ("tai", ctypes.c_int),
            ("__pad", ctypes.c_int * 11),
        ]

    buf = Timex()
    rc = libc.adjtimex(ctypes.byref(buf))
    sta_nano = bool(buf.status & 0x2000)
    offset = buf.offset
    return {
        "adjtimex_return": rc,
        "adjtimex_return_name": {
            0: "TIME_OK",
            1: "TIME_INS",
            2: "TIME_DEL",
            3: "TIME_OOP",
            4: "TIME_WAIT",
            5: "TIME_ERROR",
        }.get(rc, "UNKNOWN"),
        "status": buf.status,
        "status_hex": hex(buf.status),
        "sta_pll": bool(buf.status & 0x1),
        "sta_nano": sta_nano,
        "offset_raw": offset,
        "offset_unit": "ns" if sta_nano else "us",
        "maxerror_us": buf.maxerror,
        "esterror_us": buf.esterror,
        "tai": buf.tai,
    }


def _ntp_offset(host: str, timeout: float = 3.0) -> dict:
    packet = b"\x1b" + 47 * b"\0"
    t1 = time.time()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout)
    try:
        sock.sendto(packet, (host, 123))
        data, _addr = sock.recvfrom(48)
    finally:
        sock.close()
    t4 = time.time()
    if len(data) < 48:
        raise RuntimeError(f"short NTP response from {host}: {len(data)} bytes")

    def ntp64(offset: int) -> float:
        sec, frac = struct.unpack("!II", data[offset : offset + 8])
        return sec + frac / 2**32 - 2_208_988_800

    t2 = ntp64(32)
    t3 = ntp64(40)
    return {
        "host": host,
        "offset_s": ((t2 - t1) + (t3 - t4)) / 2,
        "rtt_s": (t4 - t1) - (t3 - t2),
        "bytes": len(data),
    }


def _clocksource() -> str:
    path = Path("/sys/devices/system/clocksource/clocksource0/current_clocksource")
    try:
        return path.read_text().strip()
    except OSError as exc:
        return f"unavailable ({exc})"


def write_env(path: Path, symbols: list[str], duration_s: float) -> None:
    now_utc = datetime.now(timezone.utc)
    now_et = now_utc.astimezone(ET)
    sample = OrderBookConfig(symbol=symbols[0])
    lines = [
        f"written_utc: {now_utc.isoformat()}",
        f"written_et: {now_et.isoformat()}",
        f"hostname: {socket.gethostname()}",
        f"platform: {platform.platform()}",
        f"python: {sys.version.replace(chr(10), ' ')}",
        f"websockets: {websockets.__version__}",
        f"clocksource: {_clocksource()}",
        "receive_clock: time.time_ns (CLOCK_REALTIME)",
        "duration_clock: time.monotonic",
        f"timezone_local: {time.tzname!r}",
        f"symbols: {' '.join(symbols)}",
        f"duration_s: {duration_s}",
        f"rest_snapshot_url: {sample.rest_snapshot_url}",
        f"ws_base_url: {sample.ws_base_url}",
        f"depth_update_speed: {sample.depth_update_speed}",
        f"snapshot_depth_limit: {sample.snapshot_depth_limit}",
        f"ws_stream_url_example: {sample.ws_stream_url}",
        "note: no fallback host is configured",
    ]
    try:
        adj = _adjtimex()
    except Exception as exc:
        lines.append(f"adjtimex_error: {type(exc).__name__}: {exc}")
    else:
        for key, value in adj.items():
            lines.append(f"{key}: {value}")

    for host in NTP_HOSTS:
        try:
            sample_ntp = _ntp_offset(host)
        except Exception as exc:
            lines.append(f"ntp_{host}: FAIL {type(exc).__name__}: {exc}")
        else:
            lines.append(
                f"ntp_{sample_ntp['host']}: offset_s={sample_ntp['offset_s']:.9f} "
                f"rtt_s={sample_ntp['rtt_s']:.9f} bytes={sample_ntp['bytes']}"
            )

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")
    logger.info("wrote %s", path)


def fetch_snapshot(symbol: str) -> None:
    cfg = OrderBookConfig(symbol=symbol)
    params = {"symbol": cfg.symbol, "limit": str(cfg.snapshot_depth_limit)}
    query = urllib.parse.urlencode(params)
    url = f"{cfg.rest_snapshot_url}?{query}"
    request = urllib.request.Request(url, headers={"User-Agent": "border-capture/0.1"})
    request_ns = time.time_ns()
    status: int
    body: bytes
    headers: dict[str, str]
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            status = response.status
            body = response.read()
            headers = dict(response.headers.items())
    except urllib.error.HTTPError as exc:
        status = exc.code
        body = exc.read()
        headers = dict(exc.headers.items()) if exc.headers else {}
    response_ns = time.time_ns()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    body_path = DATA_DIR / f"{symbol}_snapshot.json"
    meta_path = DATA_DIR / f"{symbol}_snapshot.meta.json"
    body_path.write_bytes(body)

    last_update_id = None
    n_bids = None
    n_asks = None
    parse_error = None
    try:
        payload = json.loads(body)
        if isinstance(payload, dict):
            last_update_id = payload.get("lastUpdateId")
            bids = payload.get("bids")
            asks = payload.get("asks")
            n_bids = len(bids) if isinstance(bids, list) else None
            n_asks = len(asks) if isinstance(asks, list) else None
    except json.JSONDecodeError as exc:
        parse_error = str(exc)

    meta = {
        "symbol": symbol,
        "url": url,
        "library_rest_snapshot_url": cfg.rest_snapshot_url,
        "status": status,
        "request_ns": request_ns,
        "response_ns": response_ns,
        "bytes": len(body),
        "last_update_id": last_update_id,
        "n_bids": n_bids,
        "n_asks": n_asks,
        "parse_error": parse_error,
        "date_header": headers.get("Date"),
    }
    meta_path.write_text(json.dumps(meta, indent=2) + "\n")

    if status != 200 or last_update_id is None:
        preview = body[:300].decode("utf-8", errors="replace")
        logger.error(
            "REST snapshot failed for %s status=%s url=%s library_target=%s body=%s",
            symbol,
            status,
            url,
            cfg.rest_snapshot_url,
            preview,
        )
        raise SystemExit(1)

    logger.info(
        "REST snapshot succeeded symbol=%s status=%s lastUpdateId=%s bids=%s asks=%s bytes=%s url=%s",
        symbol,
        status,
        last_update_id,
        n_bids,
        n_asks,
        len(body),
        url,
    )


async def stream_symbol(
    symbol: str,
    deadline: float,
    stop: asyncio.Event,
) -> None:
    cfg = OrderBookConfig(symbol=symbol)
    url = cfg.ws_stream_url
    writer = GzipJsonl(DATA_DIR / f"{symbol}_depth.jsonl.gz")
    writer.write(
        {
            "type": "header",
            "symbol": symbol,
            "ws_url": url,
            "rest_snapshot_url": cfg.rest_snapshot_url,
            "snapshot_depth_limit": cfg.snapshot_depth_limit,
            "depth_update_speed": cfg.depth_update_speed,
            "started_ns": time.time_ns(),
            "recv_clock": "time.time_ns",
        }
    )
    logged_first = False
    try:
        while time.monotonic() < deadline and not stop.is_set():
            try:
                logger.info("connecting %s", url)
                async with websockets.connect(url, open_timeout=15, close_timeout=5) as ws:
                    writer.write(
                        {"type": "ws_open", "recv_ns": time.time_ns(), "url": url}
                    )
                    while time.monotonic() < deadline and not stop.is_set():
                        timeout = min(30.0, max(0.1, deadline - time.monotonic()))
                        try:
                            frame = await asyncio.wait_for(ws.recv(), timeout=timeout)
                        except asyncio.TimeoutError:
                            logger.warning("no frame for %.0fs on %s; still connected to %s", timeout, symbol, url)
                            continue
                        recv_ns = time.time_ns()
                        if isinstance(frame, bytes):
                            raw = frame.decode()
                        else:
                            raw = frame
                        writer.write({"type": "depth", "recv_ns": recv_ns, "raw": raw})
                        if not logged_first:
                            logged_first = True
                            brief = _brief(raw)
                            logger.info(
                                "receiving data symbol=%s bytes=%s %s url=%s",
                                symbol,
                                len(raw),
                                brief,
                                url,
                            )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(
                    "WebSocket error symbol=%s url=%s error=%s: %s",
                    symbol,
                    url,
                    type(exc).__name__,
                    exc,
                )
                writer.write(
                    {
                        "type": "ws_error",
                        "recv_ns": time.time_ns(),
                        "url": url,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
                await asyncio.sleep(1.0)
    finally:
        writer.close()
        logger.info("closed %s records_including_headers=%s", symbol, writer.count)


def _brief(raw: str) -> str:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        return f"json_error={exc}"
    if not isinstance(payload, dict):
        return f"type={type(payload).__name__}"
    return "e={e} s={s} U={U} u={u} E={E} n_b={nb} n_a={na}".format(
        e=payload.get("e"),
        s=payload.get("s"),
        U=payload.get("U"),
        u=payload.get("u"),
        E=payload.get("E"),
        nb=len(payload.get("b") or []),
        na=len(payload.get("a") or []),
    )


async def _amain(symbols: list[str], duration_s: float) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _stop() -> None:
        logger.info("stop signal received")
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            pass

    deadline = time.monotonic() + duration_s
    stop_et = datetime.now(ET).timestamp() + duration_s
    logger.info(
        "streaming %s for %.0fs until about %s ET",
        " ".join(symbols),
        duration_s,
        datetime.fromtimestamp(stop_et, ET).isoformat(),
    )
    tasks = [
        asyncio.create_task(stream_symbol(symbol, deadline, stop), name=symbol)
        for symbol in symbols
    ]
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", nargs="+", default=["BTCUSDT", "ETHUSDT"])
    parser.add_argument("--seconds", type=float, default=7200.0)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    (RESULTS_DIR / "capture.pid").write_text(str(os.getpid()) + "\n")

    write_env(RESULTS_DIR / "env.txt", args.symbols, args.seconds)
    for symbol in args.symbols:
        fetch_snapshot(symbol)

    asyncio.run(_amain(args.symbols, args.seconds))


if __name__ == "__main__":
    main()
