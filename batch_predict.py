import os
import uuid
import time
import logging
import pandas as pd
from app.utils import hdfs_transfer as ht
import gzip
from app.ml_model import batch_predictor, predictor
from app.settings import settings
from app.utils.file_transfer import generate_credentials_for_internal_storage, generate_credentials

logging.basicConfig(format=settings.DEFAULT_LOG_FORMAT, level=logging.INFO)


class BatchPredictionJob:

    def __init__(self):
        self.batch_id = settings.BATCH_ID
        self.workspace_slug = settings.WORKSPACE_SLUG
        self.model_version_slug = settings.MODEL_VERSION_SLUG
        self.learn_type = settings.LEARN_TYPE
        self.delimiter = chr(int(settings.DELIMITER, 16))
        self.remote_input_filepath = settings.REMOTE_INPUT_FILEPATH
        self.remote_output_filepath = settings.REMOTE_OUTPUT_FILEPATH
        self.chunk_size = settings.CHUNK_SIZE

    @staticmethod
    def _generate_trace_id(trace_type='req', prefix='batch'):
        return 'dt{}-{}-'.format(trace_type, prefix) + str(uuid.uuid4())[:13]

    @staticmethod
    def calculate_duration(start_time):
        return time.time() - start_time

    def _create_local_path(self, remote_path, local_prefix):
        logging.info(
            'Creating local filepath for the remote file: {} with prefix: {}'.format(remote_path, local_prefix))
        remote_filename = remote_path.rpartition('/')[2]

        local_dir = os.path.join(settings.DATATRON_ROOT_LOCATION, self.workspace_slug, local_prefix)

        if not os.path.exists(local_dir):
            os.makedirs(local_dir, exist_ok=True)

        local_filepath = os.path.join(local_dir, remote_filename)

        logging.info('Successfully created local filepath as: {}'.format(local_filepath))

        return local_filepath

    def fetch_remote_file(self, remote_path, local_prefix):
        logging.info(
            'Getting the remote file for batch job from : {}, locally at: {}'.format(remote_path, local_prefix))
        local_filepath = self._create_local_path(remote_path, local_prefix)
        if not settings.INPUT_CONNECTOR:
            credentials = generate_credentials_for_internal_storage()
        else:
            credentials = generate_credentials(settings.INPUT_CONNECTOR)
            logging.info("credentials to use: {}".format(credentials))
        ht.copy_file(remote_path, local_filepath, 'download', credentials)
        return local_filepath

    def add_request_id(self, result):
        output = self._generate_trace_id() + ',' + str(result) + "\n"
        return output

    def process_batch(self):
        logging.info('Starting the batch process for the batch id: {}'.format(self.batch_id))
        try:

            file_process_start = time.time()

            model_key = str(self.model_version_slug) + '__' + str(self.learn_type)

            local_input_filepath = self.fetch_remote_file(remote_path=self.remote_input_filepath, local_prefix='input')
            input_filename = self.remote_input_filepath.rpartition('/')[2]
            local_output_filepath = self._create_local_path(remote_path=self.remote_output_filepath, local_prefix='output')

            compress = True if '.gz' in local_output_filepath.rpartition('/')[2] else False
            is_first_frame = True
            is_first_line = True

            current_chunk_size = 0
            output_list = []

            with open(local_input_filepath) as f:
                for line in f:


                    # Handle if request ID already exists

                    model_input = batch_predictor.transform(line)
                    output = predictor.predict(model_input)
                    result = self.add_request_id(output)
                    current_chunk_size += len(result)
                    output_list.append(result)

                    if current_chunk_size >= self.chunk_size:
                        if compress:
                            with gzip.open(local_output_filepath, "a") as out_file:
                                out_file.writelines(output_list)
                        else:
                            with open(local_output_filepath, "a") as out_file:
                                out_file.writelines(output_list)
                        output_list = []
                        current_chunk_size = 0

            if not settings.OUTPUT_CONNECTOR:
                credentials = generate_credentials_for_internal_storage()
            else:
                credentials = generate_credentials(settings.INPUT_CONNECTOR)
                logging.info("credentials to use: {}".format(credentials))
            ht.copy_file(local_output_filepath, self.remote_output_filepath, 'upload', credentials)

            logging.info('Batch Processing succeeded for filename: {} in {} duration'
                         .format(input_filename, self.calculate_duration(file_process_start)))

        except Exception as e:
            logging.error('Batch Processing for batch id {} failed due to error: {}'.format(self.batch_id, str(e)))
            raise


if __name__ == '__main__':
    batch_job_obj = BatchPredictionJob()
    batch_job_obj.process_batch()