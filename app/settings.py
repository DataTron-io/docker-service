from app.utils.settings import str_env, int_env
import os

basedir = os.path.abspath(os.path.dirname(__file__))

APPLICATION_ENV = str_env('APPLICATION_ENV', 'development')


class BaseConfig(object):
    DEFAULT_LOG_LEVEL = str_env('LOG_LEVEL', 'logging.INFO')
    DEFAULT_LOG_FORMAT = '%(asctime)s [%(levelname)s] %(message)s'
    DEBUG = True

    ORG = str_env('ORG', 'datatron')
    PORT = int_env('PUBLISHER_PORT', 6821)
    SHIVA_HADOOP_USER = str_env('SHIVA_HADOOP_USER', 'datatron')
    DATATRON_ROOT_LOCATION = str_env('DATATRON_ROOT_LOCATION', '/home/datatron')
    PUBLISHER_SLUG = str_env('PUBLISHER_SLUG', 'dt-publisher-sample')
    MODEL_TYPE = str_env('MODEL_TYPE', 'Docker')
    MODEL_VERSION_SLUG = str_env('MODEL_VERSION_SLUG', 'model_slug_sample')
    WORKSPACE_SLUG = str_env('WORKSPACE_SLUG', 'workspace_slug_sample')
    DEPLOYMENT_ID = str_env('DEPLOYMENT_ID', 'deployment_id_sample')
    BATCH_ID = str_env('BATCH_ID', 'batch_id_sample')
    REMOTE_INPUT_FILEPATH = str_env('REMOTE_INPUT_FILEPATH', '/home/datatron/shiva')
    REMOTE_OUTPUT_FILEPATH = str_env('REMOTE_OUTPUT_FILEPATH', '/home/datatron/shiva')
    LEARN_TYPE = str_env('LEARN_TYPE', 'regression')
    CHUNK_SIZE = str_env('CHUNK_SIZE', 5000)
    DELIMITER = str_env('DELIMITER', ',')


class DevConfig(BaseConfig):
    DEBUG = True


class StagingConfig(BaseConfig):
    DEBUG = False


class ProdConfig(BaseConfig):
    DEBUG = False


settings = None

if APPLICATION_ENV == 'development':
    settings = DevConfig()
elif APPLICATION_ENV == 'staging':
    settings = StagingConfig()
elif APPLICATION_ENV == 'production':
    settings = ProdConfig()

