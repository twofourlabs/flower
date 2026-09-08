import collections
import glob
import logging
import os
import queue
import shelve
import threading
import time
from collections import Counter
from functools import partial

from celery.events import EventReceiver
from celery.events.state import State
from kombu.exceptions import OperationalError
from prometheus_client import Counter as PrometheusCounter
from prometheus_client import Gauge, Histogram
from tornado.ioloop import PeriodicCallback

from .options import options
from .utils.search import TaskSearchEngine

logger = logging.getLogger(__name__)

PROMETHEUS_METRICS = None

MAX_RETRY_INTERVAL = 60


def get_prometheus_metrics():
    global PROMETHEUS_METRICS  # pylint: disable=global-statement
    if PROMETHEUS_METRICS is None:
        PROMETHEUS_METRICS = PrometheusMetrics()

    return PROMETHEUS_METRICS


class PrometheusMetrics:
    def __init__(self):
        self.events = PrometheusCounter('flower_events_total', "Number of events", ['worker', 'type', 'task'])

        self.runtime = Histogram(
            'flower_task_runtime_seconds',
            "Task runtime",
            ['worker', 'task'],
            buckets=options.task_runtime_metric_buckets
        )
        self.prefetch_time = Gauge(
            'flower_task_prefetch_time_seconds',
            "The time the task spent waiting at the celery worker to be executed.",
            ['worker', 'task']
        )
        self.number_of_prefetched_tasks = Gauge(
            'flower_worker_prefetched_tasks',
            'Number of tasks of given type prefetched at a worker',
            ['worker', 'task']
        )
        self.worker_online = Gauge('flower_worker_online', "Worker online status", ['worker'])
        self.worker_number_of_currently_executing_tasks = Gauge(
            'flower_worker_number_of_currently_executing_tasks',
            "Number of tasks currently executing at a worker",
            ['worker']
        )

    def observe_task(self, worker, event, task):
        event_type = event['type']
        name = event.get('name') or task.name or ''
        self.events.labels(worker, event_type, name).inc()
        if event.get('runtime'):
            self.runtime.labels(worker, name).observe(event['runtime'])

        # Prefetch metrics only make sense for tasks without a scheduled time
        if task.eta or not task.received:
            return
        if event_type == 'task-received':
            self.number_of_prefetched_tasks.labels(worker, name).inc()
        elif event_type == 'task-started' and task.started:
            self.prefetch_time.labels(worker, name).set(task.started - task.received)
            self.number_of_prefetched_tasks.labels(worker, name).dec()
        elif event_type in ('task-succeeded', 'task-failed') and task.started:
            self.prefetch_time.labels(worker, name).set(0)

    def observe_worker(self, worker, event):
        online = {'worker-online': 1, 'worker-heartbeat': 1, 'worker-offline': 0}
        if event['type'] in online:
            self.worker_online.labels(worker).set(online[event['type']])
        if event['type'] == 'worker-heartbeat' and event.get('active') is not None:
            self.worker_number_of_currently_executing_tasks.labels(worker).set(event['active'])

    def remove_workers(self, worker_names):
        metrics = (
            self.events,
            self.runtime,
            self.prefetch_time,
            self.number_of_prefetched_tasks,
            self.worker_online,
            self.worker_number_of_currently_executing_tasks,
        )
        removed_workers = set()
        for metric in metrics:
            labels_to_remove = [
                labels for labels in metric._metrics  # pylint: disable=protected-access
                if labels and labels[0] in worker_names
            ]
            for labels in labels_to_remove:
                removed_workers.add(labels[0])
                metric.remove(*labels)
        return len(removed_workers)


class EventsState(State):
    # EventsState object is created and accessed only from ioloop thread

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.counter = collections.defaultdict(Counter)
        self.metrics = get_prometheus_metrics()
        self.search_engine = TaskSearchEngine()
        self.search_engine.rebuild(self.tasks.items())

    def _clear_tasks(self, ready=True):
        super()._clear_tasks(ready)
        self.search_engine.rebuild(self.tasks.items())

    def _eviction_candidate(self, task_id):
        # Celery discards the least recently used task when a new one
        # arrives at the limit, so remember which one may go
        limit = self.tasks.limit
        if not limit or task_id in self.tasks or len(self.tasks) < limit:
            return None
        return next(iter(self.tasks), None)

    def _index_task(self, task, evicted):
        if evicted is not None and evicted not in self.tasks:
            self.search_engine.remove(evicted)
        self.search_engine.upsert(task)

    def event(self, event):
        event_type, worker = event['type'], event['hostname']
        is_task = event_type.startswith('task-')
        evicted = self._eviction_candidate(event.get('uuid')) if is_task else None

        super().event(event)
        self.counter[worker][event_type] += 1

        if is_task:
            task = self.tasks[event['uuid']]
            self._index_task(task, evicted)
            self.metrics.observe_task(worker, event, task)
        else:
            self.metrics.observe_worker(worker, event)


class Events(threading.Thread):
    events_enable_interval = 5000
    _BACKPRESSURE_MAXSIZE = 10000
    _DRAIN_INTERVAL_MS = 100
    _DRAIN_BATCH_SIZE = 500

    # pylint: disable=too-many-arguments
    def __init__(self, capp, io_loop, db=None, persistent=False,
                 enable_events=True, state_save_interval=0,
                 *, max_tasks_in_memory, **kwargs):
        threading.Thread.__init__(self)
        self.daemon = True

        self.io_loop = io_loop
        self.capp = capp

        self.db = db
        self.persistent = persistent
        self.enable_events = enable_events
        self.state = None
        self.state_save_timer = None
        self.state_save_interval = state_save_interval
        self._drain_timer = None
        self._event_queue = queue.Queue(maxsize=self._BACKPRESSURE_MAXSIZE)
        self._drop_count = 0
        self._last_drop_log_time = 0.0

        if self.persistent:
            self.state = self.load_state()
            if self.state:
                # A restored state keeps the limit it was saved with
                self.state.max_tasks_in_memory = self.state.tasks.limit = max_tasks_in_memory
                self.state.tasks.update()

            if state_save_interval:
                self.state_save_timer = PeriodicCallback(self.save_state,
                                                         state_save_interval)

        if not self.state:
            self.state = EventsState(max_tasks_in_memory=max_tasks_in_memory, **kwargs)

        self.timer = PeriodicCallback(self.on_enable_events,
                                      self.events_enable_interval)

    def start(self):
        threading.Thread.start(self)
        if self.enable_events:
            logger.debug("Starting enable events timer...")
            self.timer.start()

        if self.state_save_timer:
            logger.debug("Starting state save timer...")
            self.state_save_timer.start()

        self._drain_timer = PeriodicCallback(self._drain_events,
                                             self._DRAIN_INTERVAL_MS)
        self._drain_timer.start()

    def stop(self):
        try:
            if self.enable_events:
                logger.debug("Stopping enable events timer...")
                try:
                    self.timer.stop()
                except Exception:
                    logger.debug("Error stopping enable events timer", exc_info=True)

            if self.state_save_timer:
                logger.debug("Stopping state save timer...")
                try:
                    self.state_save_timer.stop()
                except Exception:
                    logger.debug("Error stopping state save timer", exc_info=True)

            if self._drain_timer:
                try:
                    self._drain_timer.stop()
                except Exception:
                    logger.debug("Error stopping drain timer", exc_info=True)
        finally:
            if self.persistent:
                self.save_state()

    def run(self):
        try_interval = 1
        while True:
            try:
                try_interval *= 2
                if try_interval > MAX_RETRY_INTERVAL:
                    try_interval = MAX_RETRY_INTERVAL

                with self.capp.connection() as conn:
                    recv = EventReceiver(conn,
                                         handlers={"*": self.on_event},
                                         app=self.capp)
                    try_interval = 1
                    logger.debug("Capturing events...")
                    # Use a timeout to periodically reconnect and prevent
                    # stale pub/sub connections from blocking forever
                    # (e.g. after ElastiCache maintenance or failover).
                    recv.capture(limit=None, timeout=300, wakeup=True)
            except (KeyboardInterrupt, SystemExit):
                try:
                    import _thread as thread
                except ImportError:
                    import thread
                thread.interrupt_main()
            except Exception as e:
                logger.error("Failed to capture events: '%s', "
                             "trying again in %s seconds.",
                             e, try_interval)
                logger.debug(e, exc_info=True)
                time.sleep(try_interval)

    def load_state(self):
        logger.debug("Loading state from '%s'...", self.db)
        try:
            state = shelve.open(self.db)
            try:
                if not state:
                    return None
                events = state['events']
                events.counter.update(state.get('counter', {}))
                return events
            finally:
                state.close()
        except Exception as e:
            logger.error("Failed to load state from '%s', moving it aside "
                         "and starting fresh: %s", self.db, e)
            # dbm backends may add suffixes like .db or .dat to the actual files
            for suffix in ('', '.db', '.dat', '.dir', '.bak'):
                name = self.db + suffix
                if os.path.exists(name):
                    os.replace(name, f'{name}.corrupt')
            return None

    def save_state(self):
        logger.debug("Saving state to '%s'...", self.db)
        started = time.monotonic()
        tmp = f'{self.db}.tmp'
        state = shelve.open(tmp, flag='n')
        try:
            state['events'] = self.state
            state['counter'] = dict(self.state.counter)
        finally:
            state.close()
        # dbm backends may add suffixes like .db or .dat to the actual files
        for name in glob.glob(glob.escape(tmp) + '*'):
            os.replace(name, self.db + name[len(tmp):])

        elapsed = time.monotonic() - started
        interval_seconds = self.state_save_interval / 1000
        if self.state_save_timer and elapsed > interval_seconds / 10:
            logger.warning(
                "Saving state took %.1fs, consider increasing "
                "--state-save-interval or decreasing --max-tasks", elapsed)

    async def on_enable_events(self):
        # Periodically enable events for workers
        # launched after flower
        try:
            await self.io_loop.run_in_executor(
                None, self.capp.control.enable_events)
        except OperationalError as exc:
            logger.warning("Failed to enable events: %s", exc)

    def on_event(self, event):
        # Enqueue event with backpressure — drop if queue is full.
        # Rate-limit drop warnings to avoid flooding logs under sustained load.
        try:
            self._event_queue.put_nowait(event)
        except queue.Full:
            self._drop_count += 1
            now = time.monotonic()
            if now - self._last_drop_log_time >= 5.0:
                logger.warning(
                    "Event queue full (%d), dropped %d event(s) in last %.0fs",
                    self._BACKPRESSURE_MAXSIZE, self._drop_count,
                    now - self._last_drop_log_time if self._last_drop_log_time else 0)
                self._drop_count = 0
                self._last_drop_log_time = now

    def _drain_events(self):
        """Process up to _DRAIN_BATCH_SIZE events from the backpressure queue."""
        for _ in range(self._DRAIN_BATCH_SIZE):
            try:
                event = self._event_queue.get_nowait()
            except queue.Empty:
                break
            try:
                self.state.event(event)
            except Exception:
                logger.error("Error processing event", exc_info=True)
