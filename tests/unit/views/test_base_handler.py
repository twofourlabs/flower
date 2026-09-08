import inspect
import unittest

from flower.views import BaseHandler


class TestMutableDefaultFix(unittest.TestCase):
    """Guard against a mutable default argument in get_argument.

    Upstream reached the same goal with ``default=None`` rather than the
    ``_UNSET`` sentinel this fork used, so assert the property instead of
    the implementation.
    """

    def test_default_is_immutable(self):
        default = inspect.signature(BaseHandler.get_argument) \
            .parameters['default'].default
        self.assertNotIsInstance(default, (list, dict, set))

    def test_default_is_not_shared_mutable_state(self):
        first = inspect.signature(BaseHandler.get_argument) \
            .parameters['default'].default
        if isinstance(first, (list, dict, set)):
            first.append('leaked')
        second = inspect.signature(BaseHandler.get_argument) \
            .parameters['default'].default
        self.assertEqual(first, second)
        self.assertNotIsInstance(second, (list, dict, set))


if __name__ == '__main__':
    unittest.main()
