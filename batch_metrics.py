import os
import uuid
import time
import json
import logging
import argparse
import pandas as pd
from app.utils import hdfs_transfer as ht
from app.utils.file_transfer import generate_credentials_for_internal_storage, generate_credentials
from app.settings import settings
from app.governor.datatron_metrics.metrics_factory import MetricsManager

logging.basicConfig(format=settings.DEFAULT_LOG_FORMAT, level=logging.INFO)

def log_time_taken(msg):
    def timer(method):
        def calc_timer(*args, **kwargs):
            start = time.time()
            res = method(*args, **kwargs)
            duration = (time.time() - start)
            print(f"{msg} took {duration} seconds.")
            return res
        return calc_timer
    return timer

class BatchMetricsJob:

    def __init__(self):
        self.batch_id = settings.BATCH_ID
        self.job_id = settings.JOB_ID
        self.workspace_slug = settings.WORKSPACE_SLUG
        self.metric_args = settings.METRIC_ARGS
        self.delimiter = chr(int(settings.DELIMITER, 16))
        self.prediction_filepath = settings.REMOTE_INPUT_FILEPATH
        self.feedback_filepaths = settings.FEEDBACK_FILEPATH_LIST
        self.chunk_size = min(self._calculate_byte_row(self.prediction_filepath), settings.CHUNK_SIZE)
        self.prediction_filename = self.prediction_file.rpartition('/')[2]
        metrics_intermediate_dir = os.path.join(settings.METRICS_DIR, self.job_id)
        os.mkdir(metrics_intermediate_dir)
        self.metrics_manager = MetricsManager(self.metric_args, self.metrics_intermediate_dir)


    @staticmethod
    def _calculate_byte_row(filepath):
        partial_pd = pd.read_csv(filepath_or_buffer=filepath, nrows=100)
        avg_mem_usage = (partial_pd.memory_usage(index=False, deep=True) / partial_pd.shape[0]).sum()
        avg_mem_usage *= 8
        return avg_mem_usage
    
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

    def delete_local_file(self, local_filepath):
        logging.info(
            'Deleting the following local file after finishing metric calculation: {}'.format(local_filepath))
        if os.path.exists(local_filepath):
            os.remove(local_filepath)
            logging.info("Successfully deleted file: {}".format(local_filepath))
        else:
            logging.info("The file at {} doesn't exist. Nothing was removed.".format(local_filepath))

    def find_optimal_chunksize(self, filepath):
        filename = filepath.rpartition('/')[2]
        logging.info('Calculating the optimal chunksize for the provided file: {}'.format(filename))
        bytes_per_row = self._calculate_byte_row(filepath)
        logging.info('Average mem usage per row: {} byte'.format(str(bytes_per_row)))
        optimal_rows_for_mem = int(self.ALLOWED_MAX_BYTES / (2*bytes_per_row)) # Multiply by 2 for feedback files
        logging.info('Optimal chuck size for memory of {} byte: {}'.format(str(self.ALLOWED_MAX_BYTES), str(optimal_rows_for_mem)))
        _chunksize = self.DEFAULT_CHUNK if (bytes_per_row * self.DEFAULT_CHUNK) < self.ALLOWED_MAX_BYTES else optimal_rows_for_mem
        logging.info('Estimated optimal chunksize is {}'.format(str(_chunksize)))
        return _chunksize

    @classmethod
    def save_metrics(cls, file_name, metric_vals, job_id):
        file_path = os.path.join(os.path.join(settings.METRICS_DIR, job_id), file_name)
        try:
            with open(file_path, "w") as fopen:
                json.dump(metric_vals, fopen)
        except FileNotFoundError as e:
            logging.info(f"Metrics for matadata calculated for job-id {job_id} could not be saved due to file {file_name} not being found.")

    @log_time_taken(f"Calculating batch metrics for prediction file: {self.prediction_filepath} for job-id: {self.job_id}")
    def process_batch(self):
        chunksize = 1e6 # Make this a variable in the settings or determine dynamically
        try:
            local_prediction_filepath = self.fetch_remote_file(remote_path=self.remote_input_filepath, local_prefix='input')
            logging.info("Successfully received prediction file from {}".format(local_prediction_filepath))
            for prediction_chunk in pd.read_csv(local_prediction_filepath, 
                                                chunksize=chunksize, 
                                                delimiter=self.delimiter):
                for feedback_filepath in self.feedback_filepaths:
                    local_feedback_filepath = self.fetch_remote_file(remote_path=feedback_filepath)
                    logging.info("Successfully received feedback file from {}".format(local_prediction_filepath))
                    for feedback_chunk in pd.read_csv(local_feedback_filepath, 
                                                    chunksize=chunksize,
                                                    delimiter=self.delimiter):
                        try:
                            joined_df = pd.merge(prediction_chunk, feedback_chunk, left_on = "datatron_request_id", right_on = "feedback_id", how="inner")
                            self.metrics_manager.update_batch(joined_df["actual_value"].to_numpy(), joined_df["prediction"].to_numpy()) # VERIFY COLUMN NAMES
                        except KeyError as e:
                            logging.info(f"Could not join datatron_id column in either the prediction file: {local_prediction_filepath} or feedback file: {local_feedback_filepath} due to error: {str(e)}.")
                    self.delete_local_file(local_feedback_filepath)
            metrics_file = os.path.join(self.metrics_dir, self.job_id, self.prediction_filepath)
            metric_values = self.metrics_manager.fetch_metric_values()
            self.save_metrics(metrics_file, metric_values, self.job_id)
        except FileNotFoundError as e:
            logging.error(f"Metrics processing for batch id {self.job_id} failed due to error: {str(e)}.")

if __name__ == '__main__':
    batch_metrics_job = BatchMetricsJob()
    batch_metrics_job.process_batch()