import asyncio
import collections
import logging
import time
from functools import partial

from kombu.exceptions import OperationalError

logger = logging.getLogger(__name__)


#: Every inspect method Flower knows how to poll, and the default set.
#: ``scheduled`` returns one entry per task on the worker's ETA timer, so its
#: reply grows with the backlog rather than with the worker's configuration --
#: tens of megabytes on a worker holding a large ETA fan-out. Deployments that
#: don't need that table can drop it via --inspect-methods.
DEFAULT_INSPECT_METHODS = ('stats', 'active_queues', 'registered', 'scheduled',
                           'active', 'reserved', 'revoked', 'conf')


class Inspector:
    #: Kept as a class attribute for backwards compatibility. The instance
    #: attribute assigned in __init__ is what actually gets polled.
    methods = DEFAULT_INSPECT_METHODS
    max_concurrency = len(methods)

    # pylint: disable=too-many-arguments
    def __init__(self, io_loop, capp, timeout, max_concurrency=None,
                 methods=None):
        self.io_loop = io_loop
        self.capp = capp
        self.timeout = timeout
        if methods:
            unknown = sorted(set(methods) - set(DEFAULT_INSPECT_METHODS))
            if unknown:
                raise ValueError(
                    f"Unknown inspect method(s): {', '.join(unknown)}. "
                    f"Valid methods are: {', '.join(DEFAULT_INSPECT_METHODS)}")
            self.methods = tuple(methods)
        self.workers = collections.defaultdict(dict)
        self._inspect_tasks = {}
        self._inspect_max_concurrency = (
            max_concurrency or len(self.methods))
        self._inspect_semaphore = None

    def purge_worker(self, worker_name):
        """Remove a worker from the inspector's cached data."""
        self.workers.pop(worker_name, None)

    def inspect(self, workername=None):
        task = self._inspect_tasks.get(workername)
        if task is None and workername is not None:
            task = self._inspect_tasks.get(None)
        if task is None:
            task = asyncio.ensure_future(self._inspect_all(workername))
            self._inspect_tasks[workername] = task
            task.add_done_callback(
                partial(self._on_inspect_done, workername))
        return task

    async def _inspect_all(self, workername):
        results = await asyncio.gather(*(
            self._inspect_method(method, workername)
            for method in self.methods
        ), return_exceptions=True)
        for method, result in zip(self.methods, results):
            if isinstance(result, Exception):
                logger.error("Inspect method %s failed: %s", method, result)

    async def _inspect_method(self, method, workername):
        if self._inspect_semaphore is None:
            self._inspect_semaphore = asyncio.Semaphore(
                self._inspect_max_concurrency)
        async with self._inspect_semaphore:
            await self.io_loop.run_in_executor(
                None, partial(self._inspect, method, workername))

    def _on_inspect_done(self, workername, task):
        if self._inspect_tasks.get(workername) is task:
            self._inspect_tasks.pop(workername)
        if not task.cancelled() and task.exception() is not None:
            logger.error("Worker inspection failed: %s", task.exception())

    def _on_update(self, workername, method, response):
        if method == 'stats':
            consumer = response.get('consumer') or response
            broker = consumer.get('broker', {})
            # Temporary fix for issue #1512, until Celery sanitizes broker statistics.
            broker.pop('alternates', None)

        info = self.workers[workername]
        info[method] = response
        info['timestamp'] = time.time()

    def _inspect(self, method, workername):
        destination = [workername] if workername else None

        logger.debug('Sending %s inspect command', method)
        start = time.time()
        try:
            # Use a dedicated connection instead of the app's pooled one. A
            # pooled connection can go stale without raising (e.g. after an
            # ElastiCache failover), and every subsequent inspect then returns
            # no replies at all -- silently freezing the worker list until
            # flower is restarted.
            with self.capp.connection() as conn:
                inspect = self.capp.control.inspect(
                    timeout=self.timeout, destination=destination,
                    connection=conn)
                result = (
                    getattr(inspect, method)()
                    if method != 'active'
                    else getattr(inspect, method)(safe=True)
                )
        except Exception as exc:
            if not self._is_connection_error(exc):
                raise
            logger.warning("Inspect method %s failed: %s", method, exc)
            return
        logger.debug("Inspect command %s took %.2fs to complete", method, time.time() - start)

        if result is None or 'error' in result:
            logger.warning("Inspect method %s failed (no replies from any worker)", method)
            return
        for worker, response in result.items():
            if response is not None:
                self.io_loop.add_callback(partial(self._on_update, worker, method, response))

    def _is_connection_error(self, exc):
        if isinstance(exc, OperationalError):
            return True

        connection = self.capp.connection_for_read()
        try:
            return isinstance(exc, connection.recoverable_connection_errors)
        finally:
            connection.close()
