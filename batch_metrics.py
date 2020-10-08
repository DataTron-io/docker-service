import os
import uuid
import time
import json
import logging
import argparse
import pandas as pd
from app.settings import settings
from datatron.governor import MetricsManager, MetricsAggregator

logging.basicConfig(format=settings.DEFAULT_LOG_FORMAT, level=logging.INFO)

# Change this to a mounted folder path & add this to settings potentially
METRICS_DIR = "/tmp/metrics/"

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
        self.feedback_files = settings.FEEDBACK_FILEPATH_LIST
        self.chunk_size = min(self._calculate_byte_row(self.prediction_filepath), settings.CHUNK_SIZE)
        self.prediction_filename = self.prediction_file.rpartition('/')[2]
        self.metrics_manager = MetricsManager(self.metric_args, METRICS_DIR)

    @staticmethod
    def _calculate_byte_row(filepath):
        partial_pd = pd.read_csv(filepath_or_buffer=filepath, nrows=100)
        avg_mem_usage = (partial_pd.memory_usage(index=False, deep=True) / partial_pd.shape[0]).sum()
        avg_mem_usage *= 8
        return avg_mem_usage

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
        try:
            with open(file_name, "w") as fopen:
                json.dump(metric_vals, fopen)
        except FileNotFoundError as e:
            logging.info(f"Metrics for matadata calculated for job-id {job_id} could not be saved due to file {file_name} not being found.")

    @log_time_taken(f"Calculating batch metrics for prediction file: {self.prediction_filepath} for job-id: {self.job_id}")
    def process_batch(self):
        chunksize = 1e6 # Make this a variable in the settings or determine dynamically
        try:
            for prediction_chunk in pd.read_csv(self.prediction_file, chunksize=chunksize):
                for feedback_file in self.feedback_files:
                    for feedback_chunk in pd.read_csv(feedback_file, chunksize=chunksize):
                        try:
                            joined_df = pd.merge(prediction_chunk, feedback_chunk, left_on = "datatron_request_id", right_on = "feedback_id", how="inner")
                            self.metrics_manager.update_batch(joined_df["feedback"].to_numpy(), joined_df["predictions"].to_numpy()) # VERIFY COLUMN NAMES
                        except KeyError as e:
                            logging.info(f"Could not join datatron_id column in either the prediction file: {predictions_file} or feedback file: {feedback_file} due to error: {str(e)}.")
            metrics_file = os.path.join(self.metrics_dir, self.job_id, self.prediction_filepath)
            metric_values = self.metrics_manager.fetch_metric_values()
            self.save_metrics(metrics_file, metric_values, self.job_id)
        except FileNotFoundError as e:
            logging.error(f"Metrics processing for batch id {self.job_id} failed due to error: {str(e)}.")


class AggregateMetricsJob:
    
    def __init__(self):
        self.job_id = settings.JOB_ID
        self.metadata_dir = os.path.join(METRICS_DIR, self.job_id)
        self.metrics_param = settings.METRIC_ARGS
        self.metrics_final_file = settings.METRICS_FILE
        self.metrics_aggregator = MetricsAggregator(self.metrics_param, self.metadata_dir)

    def aggregate_metrics(self):
        try:
            for metadata_filename in os.listdir(self.metadata_dir):
                metadata_filepath = os.path.join(self.metadata_dir, metadata_filename)
                try:
                    with open(metadata_filepath, "r") as fopen:
                        metric_metadata = json.load(fopen)
                    self.metrics_aggregator.aggregate_metadata(metric_metadata)
                except FileNotFoundError as e:
                    logging.info(f"Couldn't find file: {metadata_filename} for job-id: {self.job_id} to read metadata. Skipping this file.")
            
        except FileNotFoundError as e:
            logging.info(f"Could not access metadata directory at {self.metadata_dir}. Metrics weren't aggregated for job-id: {self.job_id}.")

    def save_metrics(self):
        metric_vals = self.metrics_aggregator.fetch_metric_values()
        BatchMetricsJob.save_metrics(self.metrics_final_file, metric_vals)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Commandline for batch metrics')
    parser.add_argument('-c', '--calc', action='store_true')
    args = parser.parse_args()

    if args.c:
        batch_metrics_job = BatchMetricsJob()
        batch_metrics_job.process_batch()
    else:
        agg_metrics_job = AggregateMetricsJob()
        agg_metrics_job.aggregate_metrics()