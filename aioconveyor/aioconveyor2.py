#!/bin/env python3

"""
long running, async producer consumer class
"""

import asyncio
import logging
from asyncio.exceptions import CancelledError
from collections.abc import AsyncGenerator, Callable, Coroutine, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Thread
from time import time
from typing import Any  # noqa: TYP001

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@dataclass
class Event:
    # timezone aware datetime
    event_time: datetime
    loop_counter: int


TProducer = Callable[[Event], Coroutine[Any, Any, str]]
TConsumer = Callable[[Event, str], Coroutine[Any, Any, int]]


class AioGenConveyor:
    """Async producer/consumer class

    Async loop is run in daemon thread so main thread can do other work.

    This variation changes up:
        - producer is an async generator. This means the producer can run some startup
          code then drop into data reader loop.
        - data is passed via asyncio queue
    """

    def __init__(
        self,
        produce: TProducer,
        consumers: Sequence[TConsumer],
        loop_interval: float,
        loop_offset: float = 0,
    ) -> None:
        """Async produce consumer class

        Produce data and feed it to 1 or more consumers asynchronously.
        Async loop is kicked off in separate thread.

        After instantiating, call start() which returns immediately.

        Args:
            produce: coroutine that returns data when called.
            consumers: 1 or more coroutines that are called with the data returned
            by produce.
            loop_interval: seconds between loop trigger.
            loop_offset: offset from loop_interval. The producer is normally kicked off
            when the wall clock hits mod(time(), loop_interval) == 0. This can be used to
            offset the kick off time. Eg Amber data is nominally every 5 minutes but
            apparently we need to wait for two minutes for the data from the end of
            the period.
        """
        self.produce: TProducer = produce
        self.consumers = consumers
        self.loop_interval = loop_interval
        self.loop_offset = loop_offset
        self.running = False
        self.stopped = False

    def loop_time(
        self,
        loop_interval: int,
        loop_offset: int = 0,
        t_now: float | None = None,
    ) -> float:
        """calculate next absolute time for a loop timer.

        The time is in the future wrt to t_now and is quantized to loop_interval seconds.

        Args:
            loop_interval (int): seconds between timer events.
            loop_offset: seconds offset to loop_interval.
            t_now (float, optional): If you want to start at some time in the
                future (or past) then override this. Defaults to time().

        Returns:
            float: time for next timer event
        """
        if t_now is None:
            t_now = time()
        t0: float = (t_now // loop_interval) * loop_interval + loop_offset
        if t0 <= t_now:
            t0 += loop_interval
        return t0

    async def loop_generator(self):
        """underlying timer for async loop

        Yields:
            absolute time for next loop event (ie kick off producer)
        """
        while True:
            t_event = self.loop_time(
                loop_interval=self.loop_interval,
                loop_offset=self.loop_offset,
            )
            log.debug(f"t_event: {t_event}")
            yield t_event

    async def event_generator(self) -> AsyncGenerator[Event, None]:
        """Generator that yields events.

        Schedules the producer
        """
        loop_counter = 0
        async for t_event in self.loop_generator():
            # non naive datetime for timestamp field in result data
            event_time: datetime = datetime.fromtimestamp(t_event, tz=timezone.utc)
            log.info(
                f"scheduler: next event: {event_time}, "
                f"{t_event - time():.2f} sec from now",
            )
            delta = t_event - time()
            if delta < 0:
                log.warning(
                    f"skipping loop event at {t_event} because it is in the past",
                )
                await asyncio.sleep(0.2)
                continue
            event = Event(event_time, loop_counter)
            await asyncio.sleep(delta)
            yield event
            loop_counter += 1

    async def production_scheduler(
        self,
        queue: asyncio.Queue,
        event_generator: AsyncGenerator[Event, None],
    ):
        """skeleton producer

        Long running task producing scheduled data and put on queue.

        Args:
            queue: [description]
            event_generator: [description]
        """

        log.info("production_scheduler: startup code goes here")

        async for event in event_generator:
            log.debug(f">>> production_scheduler: event: {(event)}")
            print()
            payload = await self.produce(event)
            await queue.put((event, payload))

    async def consumer_loop(self, queue: asyncio.Queue) -> None:
        while True:
            event, payload = await queue.get()
            log.debug(f"consumer_loop de queued: {event.loop_counter}")
            cons_tasks = [asyncio.create_task(c(event, payload)) for c in self.consumers]
            results = await asyncio.gather(*cons_tasks)
            log.info(f"scheduler: consumers completed with: {results}")

    async def launcher(self):
        """launch launch long running coro(s)

        This is the async entry point for the thread.
        Kick off long running coros (scheduler and watchdog).
        """
        queue = asyncio.Queue()
        try:
            producer = asyncio.create_task(
                self.production_scheduler(queue, self.event_generator()),
                name="producer",
            )
            consumer = asyncio.create_task(self.consumer_loop(queue), name="consumer")
            watchdog = asyncio.create_task(self.watchdog(), name="watchdog")
            results = await asyncio.gather(producer, consumer, watchdog)
            log.info(f"launcher: all tasks ended: {results}")
        except Exception as ex:
            log.error(f"launcher: received exception {ex}")
        finally:
            log.info("launcher: quitting")
            self.running = False

    async def watchdog(self) -> None:
        """detect anomalies and trigger clean exit

        watchdog was originally intended to clean up after either consumer
        or producer quit and trigger the end of the thread.

        Override as appropriate.
        """
        watchdog_loop_counter = 0
        while True:
            await asyncio.sleep(1.5)
            log.debug("watchdog: awoke")
            if self.stopped:
                raise Exception("Watchdog raising hell")
            watchdog_loop_counter += 1
        log.info("watchdog: quitting")

    def start(self):
        """start the thread, called from main thread.

        Start the thread to kick off the async producer/consumer logic.
        Returns immediately.
        """
        self.loop = asyncio.new_event_loop()
        self.thread = Thread(target=self.run_loop, daemon=True)
        self.thread.start()
        self.running = True
        log.info("start: thread started, returning to caller")

    def run_loop(self) -> None:
        """Thread to run coroutine loop in.

        Run launcher and wait for it to finish.
        """
        log.info("run loop started")
        try:
            asyncio.set_event_loop(self.loop)
            self.loop.run_until_complete(self.launcher())
        except CancelledError:
            log.error("run_loop: launcher has been cancelled.")
        finally:
            log.info("run_loop: asyncio loop in thread completed, thread terminating")

    def stop(self):
        self.stopped = True


async def toy_producer(event: Event) -> str:
    """toy producer"""
    payload = f"producing toy event: {event}, wall clock: {time()}"
    print("<<< producer:", payload)
    # if event.loop_counter > 3:
    #     raise Exception(f"produce_arg: injected exception @ {event.loop_counter}")
    return payload


async def toy_consumer(event: Event, payload: str) -> int:
    """toy consumer"""
    print(f">>> consumer: {event} {payload}>>>")
    # if event.loop_counter > 3:
    #     raise Exception(f"consume: injected exception @ {event.loop_counter}")
    return 0


async def main():
    conv = AioGenConveyor(
        produce=toy_producer,
        consumers=[toy_consumer],
        loop_interval=5.0,
    )

    conv.start()
    log.info("main: thread started")

    while conv.running:
        log.debug("main loop")
        await asyncio.sleep(2)
    log.info("main: conveyor thread no longer running, terminating")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.error("aioproc exiting")
