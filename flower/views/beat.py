import logging
import time

from celery.schedules import crontab, schedule, solar
from tornado import web
from tornado.ioloop import IOLoop

from ..views import BaseHandler

logger = logging.getLogger(__name__)

_beat_cache = None
_beat_cache_time = 0
_BEAT_CACHE_TTL = 300  # 5 minutes


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


def _fetch_beat_schedule_from_workers(capp):
    """Fetch beat_schedule from a running worker via inspect."""
    try:
        i = capp.control.inspect(timeout=5.0, limit=1)
        conf = i.conf()
        if conf:
            for worker_name, worker_conf in conf.items():
                beat = worker_conf.get('beat_schedule')
                if beat:
                    logger.debug("Fetched beat_schedule from worker %s", worker_name)
                    return beat
    except Exception:
        logger.exception("Failed to fetch beat_schedule from workers")
    return {}


def get_beat_schedules(capp):
    global _beat_cache, _beat_cache_time

    now = time.monotonic()
    if _beat_cache is not None and (now - _beat_cache_time) < _BEAT_CACHE_TTL:
        return _beat_cache

    beat_schedule = getattr(capp.conf, 'beat_schedule', None) or {}

    if not beat_schedule:
        beat_schedule = _fetch_beat_schedule_from_workers(capp)

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

    _beat_cache = result
    _beat_cache_time = now
    return result


class BeatView(BaseHandler):
    @web.authenticated
    async def get(self):
        json = self.get_argument('json', default=False, type=bool)
        schedules = await IOLoop.current().run_in_executor(
            None, get_beat_schedules, self.capp)

        if json:
            self.write(dict(data=schedules))
        else:
            self.render("beat.html", schedules=schedules)
