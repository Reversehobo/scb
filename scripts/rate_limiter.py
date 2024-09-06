import asyncio
from contextlib import asynccontextmanager


class RateLimiter:
    def __init__(self, interval: float, concurrency_limit: int):
        """
        Rate limiter for API requests.

        Args:
            interval (float): The minimum interval between requests in seconds.
            concurrency_limit (int): The maximum number of concurrent/in-progress requests.
        """
        assert interval > 0, "Interval must be a positive number."
        assert concurrency_limit >= 1, "Max concurrency must be at least 1."

        self.interval = interval
        self.semaphore = asyncio.Semaphore(concurrency_limit)
        self.last_request_time = None
        self.creation_time = asyncio.get_event_loop().time()
        self.lock = asyncio.Lock()

    # Define a property for the interval attribute

    async def wait_for_interval(self):
        async with self.lock:
            now = asyncio.get_event_loop().time()
            if self.last_request_time is not None:
                elapsed = now - self.last_request_time
                await asyncio.sleep(max(0, self.interval - elapsed))
            self.last_request_time = asyncio.get_event_loop().time()

    @asynccontextmanager
    async def throttle(self):
        await self.semaphore.acquire()
        try:
            await self.wait_for_interval()
            yield
        finally:
            self.semaphore.release()
