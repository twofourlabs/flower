from tornado import web

from ..views.beat import get_beat_schedules
from . import BaseApiHandler


class ListSchedules(BaseApiHandler):
    @web.authenticated
    def get(self):
        schedules = get_beat_schedules(self.capp)
        self.write(dict(schedules=schedules))
