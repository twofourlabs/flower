import time
import unittest
from unittest.mock import MagicMock, patch, PropertyMock

import celery
from celery.events import Event
from celery.events.state import Worker
from tornado.ioloop import IOLoop
from tornado.options import options

from flower import command  # noqa: F401 side effect - define options
from flower.app import Flower
from flower.events import Events, EventsState, get_prometheus_metrics
from flower.urls import handlers, settings


class TestQueueCache(unittest.TestCase):
    def setUp(self):
        capp = celery.Celery()
        events = Events(capp, IOLoop.current())
        self.app = Flower(capp=capp, events=events,
                          options=options, handlers=handlers, **settings)
        self.app._queue_cache_ttl = 5.0

    def test_cache_miss_returns_none(self):
        result = self.app.get_cached_queue_stats(frozenset(['q1', 'q2']))
        self.assertIsNone(result)

    def test_cache_hit_returns_copy(self):
        names_key = frozenset(['q1', 'q2'])
        data = [{'name': 'q1', 'messages': 5}, {'name': 'q2', 'messages': 10}]
        self.app.set_queue_cache(names_key, data)

        result = self.app.get_cached_queue_stats(names_key)
        self.assertEqual(result, data)
        self.assertIsNot(result, data)

    def test_cache_returns_copy_to_prevent_mutation(self):
        names_key = frozenset(['q1'])
        data = [{'name': 'q1', 'messages': 5}]
        self.app.set_queue_cache(names_key, data)

        result = self.app.get_cached_queue_stats(names_key)
        result.append({'name': 'q2', 'messages': 99})

        result2 = self.app.get_cached_queue_stats(names_key)
        self.assertEqual(len(result2), 1)

    def test_cache_expires_after_ttl(self):
        names_key = frozenset(['q1'])
        data = [{'name': 'q1', 'messages': 5}]
        self.app.set_queue_cache(names_key, data)

        ts, key, result = self.app._queue_cache
        self.app._queue_cache = (ts - 10.0, key, result)

        self.assertIsNone(self.app.get_cached_queue_stats(names_key))

    def test_cache_miss_on_different_names(self):
        names_key = frozenset(['q1'])
        data = [{'name': 'q1', 'messages': 5}]
        self.app.set_queue_cache(names_key, data)

        different_key = frozenset(['q1', 'q2'])
        self.assertIsNone(self.app.get_cached_queue_stats(different_key))

    def test_cache_disabled_when_ttl_zero(self):
        self.app._queue_cache_ttl = 0
        names_key = frozenset(['q1'])
        data = [{'name': 'q1', 'messages': 5}]
        self.app.set_queue_cache(names_key, data)

        self.assertIsNone(self.app.get_cached_queue_stats(names_key))



class TestFlowerStopSafety(unittest.TestCase):
    def test_stop_continues_if_purge_timer_fails(self):
        capp = celery.Celery()
        events = Events(capp, IOLoop.current())
        app = Flower(capp=capp, events=events,
                     options=options, handlers=handlers, **settings)
        app.started = True
        app._purge_timer = MagicMock()
        app._purge_timer.stop.side_effect = RuntimeError("timer error")
        app.events = MagicMock()
        app.executor = MagicMock()
        app.io_loop = MagicMock()

        app.stop()

        app.executor.shutdown.assert_called_once()
        app.io_loop.stop.assert_called_once()
        self.assertFalse(app.started)


class TestTransportCaching(unittest.TestCase):
    def test_transport_is_cached(self):
        capp = celery.Celery()
        events = Events(capp, IOLoop.current())
        app = Flower(capp=capp, events=events,
                     options=options, handlers=handlers, **settings)

        app._transport = 'amqp'
        self.assertEqual(app.transport, 'amqp')


if __name__ == '__main__':
    unittest.main()
