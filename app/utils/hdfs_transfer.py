import time
import logging
import os
import uuid
try:
    from urlparse import urlsplit
except ImportError:
    from urllib.parse import urlsplit
from hdfs.client import InsecureClient
from app.settings import settings


def _download(hdfs_path, local_path):
    start = time.time()
    hdfs_parsed_uri = urlsplit(hdfs_path)
    logging.info('Starting the hdfs python client at: {}'.format(hdfs_parsed_uri.netloc))
    hdfs_client = InsecureClient(url='http://' + hdfs_parsed_uri.netloc)
    logging.info('Started downloading file from HDFS location: {} to local: {}'.format(hdfs_path, local_path))
    local_filepath = hdfs_client.download(hdfs_parsed_uri.path, local_path, overwrite=True)
    duration = str(time.time() - start)
    logging.info('Finished downloading at local file path: {} in duration : {}'.format(local_filepath, duration))


def _upload(local_path, hdfs_path, user=settings.SHIVA_HADOOP_USER):
    start = time.time()

    hdfs_parsed_uri = urlsplit(hdfs_path)
    hdfs_parent_dir = hdfs_parsed_uri.path.rpartition('/')[0]
    logging.info('Starting the hdfs python client at: {}'.format(hdfs_parsed_uri.netloc))
    hdfs_client = InsecureClient(url='http://' + hdfs_parsed_uri.netloc, user=user)
    logging.info('Started uploading file to HDFS location: {} , from local: {}'.format(hdfs_path, local_path))
    logging.info('Creating directory if needed for remote hdfs file upload as :{}'.format(hdfs_parent_dir))
    hdfs_client.makedirs(hdfs_parent_dir)
    remote_filepath = hdfs_client.upload(hdfs_parsed_uri.path, local_path)
    duration = str(time.time() - start)
    logging.info('Finished uploading to remote file path: {} in duration : {}'.format(remote_filepath, duration))


def copy_file(src, dest, **kwargs):
    try:
        parsed_src_uri = urlsplit(src)
        parsed_dest_uri = urlsplit(dest)

        logging.info('Source URI is: {} and Destination URI is: {}'.format(parsed_src_uri.scheme,
                                                                           parsed_dest_uri.scheme))

        if parsed_src_uri.scheme == parsed_dest_uri.scheme == 'webhdfs':
            local_path = create_local_dest_path(src)
            _download(src, local_path)
            _upload(local_path, dest, **kwargs)
        elif parsed_src_uri.scheme == '' and parsed_dest_uri.scheme == 'webhdfs':
            _upload(src, dest, **kwargs)
        elif parsed_src_uri.scheme == 'webhdfs' and parsed_dest_uri.scheme == '':
            _download(src, dest)
        else:
            raise NotImplementedError('URI combination is not supported for copy transfer')

        logging.info('Finished transferring file from source: {} to destination: {}'.format(src, dest))
    except Exception as e:
        logging.error('Unable to complete copy file task, Error: {}'.format(str(e)))
        raise RuntimeError('Failed to transfer file from src: {} to dest: {}'.format(src, dest))


def create_local_dest_path(source_path):
    tmp_dir = '/tmp'
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)
    file_name = str(uuid.uuid4())[:8] + '_' + source_path.rpartition('/')[2]
    local_path = tmp_dir + os.sep + file_name
    return local_path
