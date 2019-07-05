from flask_restful import Api
api = Api()

from app.resources.healthcheck import Healthcheck  # noqa
from app.resources.serve_request import ServePredictRequest, ServePredictProbaRequest # noqa

api.add_resource(ServePredictRequest, '/predict')
api.add_resource(ServePredictProbaRequest, '/predictproba')
api.add_resource(Healthcheck, '/healthcheck')
