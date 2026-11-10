import asyncio
import logging

from orderbook.book import OrderBookState
from orderbook.config import OrderBookConfig
from orderbook.features import FeatureExtractor, feature_size
from orderbook.storage import OrderBookRecorder
from orderbook.sync import BookSynchronizer, stream_book

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

logger = logging.getLogger(__name__)


async def producer(
    config: OrderBookConfig, sync: BookSynchronizer, queue: asyncio.Queue
) -> None:
    """
    Only job: keep the synchronized book up to date and hand each new state
    to the queue. ``stream_book`` owns the buffer, the snapshot and the book,
    and rebuilds the book on any gap, reconnect or crossed state.
    """
    async for state in stream_book(config, sync):
        await queue.put(state)


async def consumer(
    queue: asyncio.Queue,
    sync: BookSynchronizer,
    extractor: FeatureExtractor,
    recorder: OrderBookRecorder,
    config: OrderBookConfig,
) -> None:
    """
    Pulls book states off the queue and does the heavier work: extracting
    features, recording to disk, printing.
    """
    event_count = 0

    while True:
        state: OrderBookState = await queue.get()
        event_count += 1

        features = extractor.update(state)
        recorder.record(state, features)

        if event_count % 10 == 0:
            print(f"\n--- Order Book ({config.symbol}) | update #{event_count} ---")
            print(f"Mid price : ${state.mid_price:,.2f}")
            print(f"Spread    : ${state.spread:.2f}")
            print(f"Queue size: {queue.qsize()}")  # watch this — should stay near 0

            print(f"Resyncs   : {sync.stats.resyncs}")

        queue.task_done()


async def main():
    config = OrderBookConfig(symbol="BTCUSDT")
    sync = BookSynchronizer(config)
    extractor = FeatureExtractor(config)
    recorder = OrderBookRecorder(config)

    logger.info(f"Starting order book for {config.symbol}")
    logger.info(f"Feature vector size: {feature_size(config)}")

    queue: asyncio.Queue = asyncio.Queue()

    producer_task = asyncio.create_task(producer(config, sync, queue))
    consumer_task = asyncio.create_task(
        consumer(queue, sync, extractor, recorder, config)
    )

    try:
        await asyncio.gather(producer_task, consumer_task)
    except asyncio.CancelledError:
        pass
    finally:
        recorder.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down...")