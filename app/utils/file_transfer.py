import json
from app.settings import settings


def generate_credentials_for_internal_storage():
    if settings.DATATRON_INTERNAL_STORAGE_TYPE == 'HDFS':
        if not settings.USE_KERBEROS:
            return {
                'user': settings.DATATRON_INTERNAL_STORAGE_USER
            }
        creds = {
            'keytab': settings.KEYTAB_LOCATION,
            'principal': settings.KERBEROS_USER,
            'xml_files': settings.HADOOP_XML_FILES,
            'user': settings.DATATRON_INTERNAL_STORAGE_USER
        }
        replace_host_path(creds)
    else:
        creds = {
            'accessKeyId': settings.AWS_ACCESS_KEY_ID,
            'secretAccessKey': settings.AWS_SECRET_ACCESS_KEY
        }
        if settings.S3_ENDPOINT_URL:
            creds['use_ssl'] = settings.S3_SSL
    return creds


def generate_credentials(connector):
    connector_details = json.loads(connector)
    credentials = connector_details["connector"]["configurations"].get("credentials", None)
    if credentials is None or "principal" not in credentials:
        return {
            'user': connector_details["connector"]["configurations"].get("user", "datatron")
        }
    creds = {
        'keytab': connector_details["connector"]["configurations"]["credentials"]["keytab"],
        'principal': connector_details["connector"]["configurations"]["credentials"]["principal"],
        'xml_files': connector_details["connector"]["configurations"]["credentials"].get("xmls", ""),
        'user': connector_details["connector"]["configurations"]["user"]
    }
    replace_host_path(creds)
    return creds

def replace_host_path(credentials):
    if "keytab" in credentials:
        credentials["keytab"] = credentials["keytab"].replace(settings.XML_HOST_PATH, settings.XML_MOUNT_POINT, 1)
    if "xml_files" in credentials:
        xmls = credentials["xml_files"].split(',')
        for i in range(len(xmls)):
            xmls[i] = xmls[i].replace(settings.XML_HOST_PATH, settings.XML_MOUNT_POINT, 1)
        credentials["xml_files"] = ','.join(xmls)
