import logging

from celery.schedules import crontab, schedule, solar
from tornado import web

from ..views import BaseHandler

logger = logging.getLogger(__name__)


def format_schedule(s):
    if isinstance(s, crontab):
        return 'crontab({0} {1} {2} {3} {4})'.format(
            s._orig_minute, s._orig_hour, s._orig_day_of_week,
            s._orig_day_of_month, s._orig_month_of_year,
        )
    if isinstance(s, schedule):
        return '{0}s'.format(s.seconds)
    if isinstance(s, solar):
        return 'solar({0}, {1}, {2})'.format(s.event, s.lat, s.lon)
    if isinstance(s, (int, float)):
        return '{0}s'.format(s)
    return str(s)


def get_beat_schedules(capp):
    beat_schedule = getattr(capp.conf, 'beat_schedule', None) or {}
    result = []
    for name, entry in sorted(beat_schedule.items()):
        result.append({
            'name': name,
            'task': entry.get('task', ''),
            'schedule': format_schedule(entry.get('schedule', '')),
            'args': str(entry.get('args', '()')),
            'kwargs': str(entry.get('kwargs', '{}')),
            'options': str(entry.get('options', '{}')),
        })
    return result


class BeatView(BaseHandler):
    @web.authenticated
    def get(self):
        json = self.get_argument('json', default=False, type=bool)
        schedules = get_beat_schedules(self.capp)

        if json:
            self.write(dict(data=schedules))
        else:
            self.render("beat.html", schedules=schedules)
