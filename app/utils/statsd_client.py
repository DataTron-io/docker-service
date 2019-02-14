from app.settings import settings
import statsd


class ShivaStatsdClient(object):
    def __init__(
            self,
            host=settings.STATSD_HOST,
            port=settings.STATSD_PORT,
            prefix=settings.STATSD_PREFIX
    ):
        self.client = statsd.StatsClient(host, port, prefix=prefix)

    def get_client(self):
        return self.client
