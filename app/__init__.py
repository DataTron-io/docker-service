import logging
from flask import Flask
from prometheus_flask_exporter import PrometheusMetrics

from app.settings import settings  # noqa

logging.basicConfig(format=settings.DEFAULT_LOG_FORMAT, level=logging.INFO)


def create_application():
    application = Flask(__name__)
    application.config.from_object(settings)

    metrics = PrometheusMetrics(application)
    metrics.info("app_info", "Datatron Docker Service", version="1.0.0")

    from app.routes.api import api  # noqa
    api.init_app(application)

    return application
