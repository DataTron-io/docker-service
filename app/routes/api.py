from flask_restful import Api
api = Api()


from app.resources.serve_publisher_request import ServePublisherRequest  # noqa

api.add_resource(ServePublisherRequest, '/predict')

