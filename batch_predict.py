import os
import uuid
import time
import logging
import pandas as pd
from app.utils import hdfs_transfer as ht
from app.ml_model import predictor
from app.settings import settings

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
        ht.copy_file(remote_path, local_filepath)
        return local_filepath

    def add_request_ids(self, dframe):
        logging.info('Starting to update the current frame with the datatron requests id')
        if 'datatron_request_id' in dframe:
            logging.info('Datatron requests id already present, skipping generation of new ids')
            return dframe
        dframe['datatron_request_id'] = [self._generate_trace_id() for _ in range(len(dframe))]
        logging.info('Finished updating the current frame with the datatron requests id')
        return dframe

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

            for each_chunk in pd.read_csv(filepath_or_buffer=local_input_filepath,
                                          chunksize=self.chunk_size,
                                          delimiter=self.delimiter):

                logging.info('Starting to process new chunk for the batch file')

                chunk_process_start = time.time()
                requestid_add_start = time.time()
                each_chunk = self.add_request_ids(each_chunk)
                each_chunk = each_chunk.set_index('datatron_request_id')

                logging.info("Finished adding trace ids for current frame in : {}".
                             format(self.calculate_duration(requestid_add_start)))

                model_predict_start = time.time()
                logging.info('Calling predict batch on the model: {} , for current frame'.format(model_key))

                feature_list = predictor.feature_list()
                x_list = each_chunk[feature_list].values
                output = predictor.predict(x_list)
                predict_df = pd.DataFrame(output, columns=['outputs'])
                predict_df.index = each_chunk.index.values

                logging.info('Model: {} finished the prediction for the current frame in: {}'
                             .format(model_key, self.calculate_duration(model_predict_start)))

                predict_merge_start = time.time()
                predict_df = predict_df.add_prefix(model_key)
                each_chunk = each_chunk.merge(predict_df, how='outer', left_index=True, right_index=True)

                logging.info('Finished merging the prediction result with existing base frame in: {}'
                             .format(self.calculate_duration(predict_merge_start)))
                chunk_append_start = time.time()

                if compress:
                    each_chunk.to_csv(path_or_buf=local_output_filepath, mode='a', compression='gzip',
                                      encoding='utf-8', header=is_first_frame, sep=self.delimiter)
                else:
                    each_chunk.to_csv(path_or_buf=local_output_filepath, mode='a', encoding='utf-8',
                                      header=is_first_frame, sep=self.delimiter)

                is_first_frame = False

                logging.info('Finished appending results to output file in: {}'
                             .format(self.calculate_duration(chunk_append_start)))

                logging.info("Finished processing current frame in : {}"
                             .format(self.calculate_duration(chunk_process_start)))

            ht.copy_file(local_output_filepath, self.remote_output_filepath)

            logging.info('Batch Processing succeeded for filename: {} in {} duration'
                         .format(input_filename, self.calculate_duration(file_process_start)))
        except Exception as e:
            logging.error('Batch Processing for batch id {} failed due to error: {}'.format(self.batch_id, str(e)))


if __name__ == '__main__':
    batch_job_obj = BatchPredictionJob()
    batch_job_obj.process_batch()