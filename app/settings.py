from app.utils.settings import str_env, int_env, bool_env
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
    MODEL_LEARN_TYPE = str_env('MODEL_LEARN_TYPE', 'model_learn_type_sample')
    MODEL_NAME = str_env('MODEL_NAME', 'dnn_model')
    MODEL_VERSION = str_env('MODEL_VERSION', 'model_version_sample')
    MODEL_VERSION_SLUG = str_env('MODEL_VERSION_SLUG', 'model_version_slug_sample')
    MODEL_SLUG = str_env('MODEL_SLUG', 'model_slug_sample')
    MODEL_TYPE = str_env('MODEL_TYPE', 'Docker')
    WORKSPACE_SLUG = str_env('WORKSPACE_SLUG', 'workspace_slug_sample')
    DEPLOYMENT_ID = str_env('DEPLOYMENT_ID', 'deployment_id_sample')
    BATCH_ID = str_env('BATCH_ID', 'batch_id_sample')
    REMOTE_INPUT_FILEPATH = str_env('REMOTE_INPUT_FILEPATH', '/home/datatron/shiva')
    REMOTE_OUTPUT_FILEPATH = str_env('REMOTE_OUTPUT_FILEPATH', '/home/datatron/shiva')
    LEARN_TYPE = str_env('LEARN_TYPE', 'regression')
    CHUNK_SIZE = str_env('CHUNK_SIZE', 20000)
    DELIMITER = str_env('DELIMITER', ',')

    INPUT_CONNECTOR = str_env('INPUT_CONNECTOR')
    OUTPUT_CONNECTOR = str_env('OUTPUT_CONNECTOR')

    DATATRON_INTERNAL_STORAGE_USER = str_env('DATATRON_INTERNAL_STORAGE_USER', 'datatron')

    DATATRON_INTERNAL_STORAGE_TYPE = str_env('DATATRON_INTERNAL_STORAGE_TYPE', 'S3')

    USE_KERBEROS = bool_env('USE_KERBEROS', False)

    XML_HOST_PATH = str_env('XML_HOST_PATH')
    XML_MOUNT_POINT = str_env('XML_MOUNT_POINT')
    HADOOP_XML_FILES = str_env('HADOOP_XML_FILES')
    KEYTAB_LOCATION = str_env('KEYTAB_LOCATION')
    KERBEROS_USER = str_env('KERBEROS_USER')
    AWS_ACCESS_KEY_ID = str_env('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = str_env('AWS_SECRET_ACCESS_KEY')
    S3_ENDPOINT_URL = str_env('S3_ENDPOINT_URL', None)
    S3_SSL = bool_env('S3_SSL', True)
    USE_WEBHDFS = bool_env('USE_WEBHDFS', True)

    JAVA_GATEWAY_JAR_LOCATION = os.path.dirname(os.path.abspath(__file__)) \
                                + '/../java-gateway-1.0-SNAPSHOT-jar-with-dependencies.jar'
    JAVA_GATEWAY_PORT = 25333


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

