import logging

from tornado import web

from ..views import BaseHandler

logger = logging.getLogger(__name__)


class BrokerView(BaseHandler):
    @web.authenticated
    async def get(self):
        app = self.application

        queue_names = self.get_active_queue_names()
        names_key = frozenset(queue_names)

        # Serve from cache when fresh; with many queues this avoids re-fetching
        # every length from the broker on each page load. See --queue_cache_ttl.
        queues = app.get_cached_queue_stats(names_key)
        if queues is None:
            broker = self.get_broker()
            queues = []
            try:
                queues = await broker.queues(queue_names)
                app.set_queue_cache(names_key, queues)
            except Exception as e:
                logger.error("Unable to get queues: '%s'", e)

        self.render("broker.html",
                    broker_url=app.broker_uri,
                    queues=queues)
