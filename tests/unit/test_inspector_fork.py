import unittest
from unittest.mock import MagicMock

from flower.inspector import DEFAULT_INSPECT_METHODS, Inspector


class TestInspectorPurgeWorker(unittest.TestCase):
    def test_purge_existing_worker(self):
        io_loop = MagicMock()
        capp = MagicMock()
        inspector = Inspector(io_loop, capp, timeout=1.0)

        inspector.workers['w1'] = {'stats': {}, 'timestamp': 1000}
        inspector.workers['w2'] = {'stats': {}, 'timestamp': 1000}

        inspector.purge_worker('w1')

        self.assertNotIn('w1', inspector.workers)
        self.assertIn('w2', inspector.workers)

    def test_purge_nonexistent_worker_is_noop(self):
        io_loop = MagicMock()
        capp = MagicMock()
        inspector = Inspector(io_loop, capp, timeout=1.0)

        # Should not raise
        inspector.purge_worker('nonexistent')
        self.assertEqual(len(inspector.workers), 0)



class TestInspectorMethodSelection(unittest.TestCase):
    """--inspect-methods lets deployments drop expensive inspect calls.

    On a worker holding a large ETA backlog, `scheduled` and `reserved`
    serialize one entry per held task, producing replies tens of megabytes
    wide that the requester cannot read inside its inspect timeout. The
    orphaned reply then leaks in the broker, so operators need a way to stop
    Flower asking for them.
    """

    def test_defaults_poll_every_known_method(self):
        inspector = Inspector(MagicMock(), MagicMock(), timeout=1.0)

        self.assertEqual(inspector.methods, DEFAULT_INSPECT_METHODS)
        self.assertIn('scheduled', inspector.methods)

    def test_explicit_methods_replace_the_default_set(self):
        inspector = Inspector(MagicMock(), MagicMock(), timeout=1.0,
                              methods=['stats', 'active'])

        self.assertEqual(inspector.methods, ('stats', 'active'))
        self.assertNotIn('scheduled', inspector.methods)
        self.assertNotIn('reserved', inspector.methods)

    def test_concurrency_follows_the_selected_methods(self):
        inspector = Inspector(MagicMock(), MagicMock(), timeout=1.0,
                              methods=['stats', 'active'])

        self.assertEqual(inspector._inspect_max_concurrency, 2)

    def test_explicit_max_concurrency_still_wins(self):
        inspector = Inspector(MagicMock(), MagicMock(), timeout=1.0,
                              max_concurrency=5, methods=['stats', 'active'])

        self.assertEqual(inspector._inspect_max_concurrency, 5)

    def test_empty_methods_falls_back_to_defaults(self):
        inspector = Inspector(MagicMock(), MagicMock(), timeout=1.0,
                              methods=[])

        self.assertEqual(inspector.methods, DEFAULT_INSPECT_METHODS)

    def test_unknown_method_is_rejected(self):
        with self.assertRaises(ValueError) as ctx:
            Inspector(MagicMock(), MagicMock(), timeout=1.0,
                      methods=['stats', 'schedulez'])

        self.assertIn('schedulez', str(ctx.exception))

    def test_selected_methods_do_not_leak_onto_other_instances(self):
        Inspector(MagicMock(), MagicMock(), timeout=1.0, methods=['stats'])
        other = Inspector(MagicMock(), MagicMock(), timeout=1.0)

        self.assertEqual(other.methods, DEFAULT_INSPECT_METHODS)


if __name__ == '__main__':
    unittest.main()
