import logging
from app.settings import settings
from py4j.java_gateway import JavaGateway


'''
credentials: {
        'keytab': Kerberos keytab,
        'principal': Kerberos principle,
        'xml_files': Hadoop config files, comma separated string,
        'user': hadoop user
    }
'''



class Gateway:
    __instance = None

    def __init__(self):
        if Gateway.__instance is None:
            self.gateway = JavaGateway()
            self.gateway.launch_gateway(classpath=settings.JAVA_GATEWAY_JAR_LOCATION, die_on_exit=True, port=settings.JAVA_GATEWAY_PORT)


            logging.info('Successfully launched java gateway for classpath {} at port {}.'
                         .format(settings.JAVA_GATEWAY_JAR_LOCATION, settings.JAVA_GATEWAY_PORT))

            self.uri = self.gateway.jvm.java.net.URI
            self.path = self.gateway.jvm.org.apache.hadoop.fs.Path
            self.filesystem = self.gateway.jvm.org.apache.hadoop.fs.FileSystem
            self.configuration = self.gateway.jvm.org.apache.hadoop.conf.Configuration()
            self.configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")

            Gateway.__instance = self

    @staticmethod
    def get_instance():
        if Gateway.__instance is None:
            Gateway()
        return Gateway.__instance

    @staticmethod
    def generate_ticket(url, principal, keytab, xmls):
        gateway_instance = Gateway.get_instance()
        configuration = gateway_instance.configuration
        logging.info('Using kerberos for hdfs authentication')
        configuration.set("fs.defaultFS", url)
        configuration.set("hadoop.security.authentication", "kerberos")

        if xmls:
            logging.info('Adding xml resources {}'.format(xmls))
            xml_list = xmls.split(',')
            for xml in xml_list:
                configuration.addResource(gateway_instance.path(xml))
            # configuration.set("hadoop.rpc.protection", "privacy")
        provider_array = gateway_instance.gateway.new_array(
            gateway_instance.gateway.jvm.org.apache.hadoop.security.SecurityInfo, 1)
        provider_array[0] = gateway_instance.gateway.jvm.org.apache.hadoop.security.AnnotatedSecurityInfo()
        gateway_instance.gateway.jvm.org.apache.hadoop.security.SecurityUtil.setSecurityInfoProviders(provider_array)
        gateway_instance.gateway.jvm.org.apache.hadoop.security.UserGroupInformation.setConfiguration(configuration)
        logging.info('Generating new ticket')
        gateway_instance.gateway.jvm.org.apache.hadoop.security.UserGroupInformation.loginUserFromKeytab(principal,
                                                                                                         keytab)
        logging.info('Kerberos authentication successful')


class SecureClient:
    """InsecureClient object
    """

    def __init__(self, url, user, credentials):

        try:
            gateway_instance = Gateway.get_instance()
            configuration = gateway_instance.configuration
            logging.info('Starting HDFS session for {}'.format(url))

            if credentials:
                Gateway.generate_ticket(url, credentials.get("principal"), credentials.get("keytab"),
                                        credentials.get("xml_files"))


                self.fs = gateway_instance.filesystem.get(configuration)
            else:
                self.fs = gateway_instance.filesystem.get(gateway_instance.uri(url), gateway_instance.configuration,
                                                          user)

        except Exception as e:
            logging.info('Cannot initiate Hdfs session for url {}: {}.'.format(url, str(e)))
            raise

    def download(self, hdfs_path, local_path, overwrite):
        try:
            gateway_instance = Gateway.get_instance()
            self.fs.copyToLocalFile(gateway_instance.path(hdfs_path), gateway_instance.path(local_path))
        except Exception as e:
            logging.info('Error in downloading file from {} to {}: {}.'.format(hdfs_path, local_path, str(e)))
            raise

    def upload(self, hdfs_path, local_path):
        try:
            gateway_instance = Gateway.get_instance()
            self.fs.copyFromLocalFile(gateway_instance.path(local_path), gateway_instance.path(hdfs_path))
        except Exception as e:
            logging.info('Error in uploading file from {} to {}: {}.'.format(local_path, hdfs_path, str(e)))
            raise

    def makedirs(self, hdfs_parent_dir):
        try:
            gateway_instance = Gateway.get_instance()
            return self.fs.mkdirs(gateway_instance.path(hdfs_parent_dir))
        except Exception as e:
            logging.info('Error in creating directory for hdfs path {}: {}.'.format(hdfs_parent_dir, str(e)))
            raise

    def content(self, hdfs_path, strict=True):
        try:
            gateway_instance = Gateway.get_instance()

            return self.fs.getContentSummary(gateway_instance.path(hdfs_path))
        except Exception as e:
            logging.info('Exception in getting content for hdfs path {}: {}.'.format(hdfs_path, str(e)))
            if not strict:
                return None
            raise
