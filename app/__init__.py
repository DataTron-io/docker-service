import logging
from flask import Flask
from app.settings import settings  # noqa

logging.basicConfig(format=settings.DEFAULT_LOG_FORMAT, level=logging.INFO)


def create_application():
    application = Flask(__name__)
    application.config.from_object(settings)

    from app.routes.api import api  # noqa
    api.init_app(application)

    return application
