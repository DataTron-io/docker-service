from app.utils.settings import str_env, int_env, bool_env
import os

basedir = os.path.abspath(os.path.dirname(__file__))

APPLICATION_ENV = str_env('APPLICATION_ENV', 'development')


class BaseConfig(object):
    DEFAULT_LOG_LEVEL = str_env('LOG_LEVEL', 'logging.INFO')
    DEFAULT_LOG_FORMAT = '%(asctime)s [%(levelname)s] %(message)s'
    DEBUG = True

    PORT = int_env('PUBLISHER_PORT', 6821)

    DATATRON_ROOT_LOCATION = str_env('DATATRON_ROOT_LOCATION', '/home/datatron/shiva')

    WORKSPACE_SLUG = str_env('WORKSPACE_SLUG', 'dt-sample-workspace-slug')
    PUBLISHER_SLUG = str_env('PUBLISHER_SLUG', 'dt-publisher-sample')
    MODEL_LEARN_TYPE = str_env('MODEL_LEARN_TYPE', 'model_learn_type_sample')
    MODEL_TYPE = str_env('MODEL_TYPE', 'tensorflow')
    MODEL_PATH = str_env('MODEL_PATH', 's3://staging-shivastg/test_workspace/dnn_new_input_format.zip')
    MODEL_NAME = str_env('MODEL_NAME', 'dnn_model')
    MODEL_VERSION = str_env('MODEL_VERSION', 'model_version_sample')
    MODEL_MINOR_VERSION = str_env('MODEL_MINOR_VERSION', 'model_version_sample')
    MODEL_SLUG = str_env('MODEL_SLUG', 'model_slug_sample')
    MODEL_FEATURES = str_env('MODEL_FEATURES', '"[model_features_sample]"')
    DEPLOYMENT_ID = str_env('DEPLOYMENT_ID', 'deployment_id_sample')

    DISCOVERY_TYPE = str_env('DISCOVERY_TYPE', 'zk')
    SHIVA_ZOOKEEPER_HOSTS = str_env('SHIVA_ZOOKEEPER_HOSTS',
                                    '52.229.16.150:2181, 52.191.128.40:2181, 51.141.166.253:2181')
    SHIVA_SERVICE_TYPE = str_env('SHIVA_SERVICE_TYPE', 'workitem')
    ORG = str_env('ORG', 'datatron')

    STATSD_HOST = str_env('STATSD_HOST', '52.191.143.140')
    STATSD_PORT = str_env('STATSD_PORT', 8125)
    STATSD_PREFIX = str_env('STATSD_PREFIX', 'publisher.local.{}.'.format(PUBLISHER_SLUG))

    PUBLISHER_MODEL_PREDICT = 'model.predict'

    PUBLISHER_HEALTHCHECK_TOTAL = 'healthcheck.total'
    PUBLISHER_HEALTHCHECK_SUCCESS = 'healthcheck.success'
    PUBLISHER_HEALTHCHECK_FAILURE = 'healthcheck.failure'

    PUBLISHER_REQUEST_COUNT = 'predict_request.total'
    PUBLISHER_REQUEST_SUCCESS = 'predict_request.success'
    PUBLISHER_REQUEST_FAILURE = 'predict_request.failure'

    PUBLISHER_REQUEST_LATENCY = 'predict_request.latency'


class DevConfig(BaseConfig):
    STATSD_PREFIX = str_env('STATSD_PREFIX', 'publisher.dev.{}.'.format(BaseConfig.PUBLISHER_SLUG))


class StagingConfig(BaseConfig):
    DEBUG = False
    STATSD_HOST = str_env('STATSD_HOST', '10.0.1.11')
    STATSD_PREFIX = str_env('STATSD_PREFIX', 'publisher.staging.{}.'.format(BaseConfig.PUBLISHER_SLUG))
    SHIVA_ZOOKEEPER_HOSTS = str_env('SHIVA_ZOOKEEPER_HOSTS',
                                    '10.0.1.5:2181, 10.0.1.6:2181, 10.0.1.7:2181')


class ProdConfig(BaseConfig):
    DEBUG = False
    STATSD_HOST = str_env('STATSD_HOST', '10.0.1.11')
    STATSD_PREFIX = str_env('STATSD_PREFIX', 'publisher.prod.{}.'.format(BaseConfig.PUBLISHER_SLUG))
    SHIVA_ZOOKEEPER_HOSTS = str_env('SHIVA_ZOOKEEPER_HOSTS',
                                    '10.0.1.5:2181, 10.0.1.6:2181, 10.0.1.7:2181')


settings = None

if APPLICATION_ENV == 'development':
    settings = DevConfig()
elif APPLICATION_ENV == 'staging':
    settings = StagingConfig()
elif APPLICATION_ENV == 'production':
    settings = ProdConfig()

