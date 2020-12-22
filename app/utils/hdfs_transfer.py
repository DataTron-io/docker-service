import time
import logging
import os
import uuid
from app.settings import settings

try:
    from urlparse import urlsplit
except ImportError:
    from urllib.parse import urlsplit


from hdfs.client import InsecureClient
from .hdfs_client import SecureClient


# TODO - Refactor and add kerberos support

def _download(hdfs_path, local_path, credentials):
    start = time.time()
    hdfs_parsed_uri = urlsplit(hdfs_path)
    user = credentials.pop('user', 'root')
    if hdfs_parsed_uri.scheme == 'webhdfs':
        logging.info('Starting the webhdfs python client at: {}'.format(hdfs_parsed_uri.netloc))
        scheme = 'http://'
        hdfs_client = InsecureClient(url=scheme + hdfs_parsed_uri.netloc, user=user)
    else:
        logging.info('Starting the hdfs python client at: {}'.format(hdfs_parsed_uri.netloc))
        scheme = 'hdfs://'
        hdfs_client = SecureClient(scheme + hdfs_parsed_uri.netloc, user, credentials)

    logging.info('Started downloading file from HDFS location: {} to local: {}'.format(hdfs_path, local_path))
    local_filepath = hdfs_client.download(hdfs_parsed_uri.path, local_path, overwrite=True)
    duration = str(time.time() - start)
    logging.info('Finished downloading at local file path: {} in duration : {}'.format(local_filepath, duration))


def _upload(local_path, hdfs_path, credentials):
    start = time.time()

    hdfs_parsed_uri = urlsplit(hdfs_path)
    hdfs_parent_dir = hdfs_parsed_uri.path.rpartition('/')[0]

    user = credentials.pop('user', 'root')
    if hdfs_parsed_uri.scheme == 'webhdfs':
        logging.info('Starting the webhdfs python client at: {}'.format(hdfs_parsed_uri.netloc))
        scheme = 'http://'
        hdfs_client = InsecureClient(url=scheme + hdfs_parsed_uri.netloc, user=user)
    else:
        logging.info('Starting the hdfs python client at: {}'.format(hdfs_parsed_uri.netloc))
        scheme = 'hdfs://'
        hdfs_client = SecureClient(scheme + hdfs_parsed_uri.netloc, user, credentials)

    logging.info('Started uploading file to HDFS location: {} , from local: {}'.format(hdfs_path, local_path))
    logging.info('Creating directory if needed for remote hdfs file upload as :{}'.format(hdfs_parent_dir))
    hdfs_client.makedirs(hdfs_parent_dir)
    remote_filepath = hdfs_client.upload(hdfs_parsed_uri.path, local_path)
    duration = str(time.time() - start)
    logging.info('Finished uploading to remote file path: {} in duration : {}'.format(remote_filepath, duration))


def copy_file(src, dest, copy_type, credentials):


    logging.info('Initiating HDFS copy file API with copy type: {}'.format(copy_type))
    if copy_type == 'upload':
        _upload(src, dest, credentials)
    elif copy_type == 'download':
        _download(src, dest, credentials)
    else:
        raise ValueError('Invalid src + dest combination supplied for HDFS')

def create_local_dest_path(source_path):
    tmp_dir = '/tmp'
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)
    file_name = str(uuid.uuid4())[:8] + '_' + source_path.rpartition('/')[2]
    local_path = tmp_dir + os.sep + file_name
    return local_path