from tornado import web
from tornado.ioloop import IOLoop

from ..views.beat import get_beat_schedules
from . import BaseApiHandler


class ListSchedules(BaseApiHandler):
    @web.authenticated
    async def get(self):
        schedules = await IOLoop.current().run_in_executor(
            None, get_beat_schedules, self.capp)
        self.write(dict(schedules=schedules))
