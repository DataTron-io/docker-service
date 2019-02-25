from flask_restful import Api
api = Api()

from app.resources.publisher_healthcheck import PublisherHealthcheck  # noqa
from app.resources.serve_publisher_request import ServePublisherRequest  # noqa

api.add_resource(ServePublisherRequest, '/predict')
api.add_resource(PublisherHealthcheck, '/healthcheck')
