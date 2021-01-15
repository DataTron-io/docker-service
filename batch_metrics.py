import os
import uuid
import time
import json
import yaml
import logging
import argparse
import traceback
import numpy as np
import pandas as pd
from app.utils import hdfs_transfer as ht
from app.utils.file_transfer import generate_credentials_for_internal_storage, generate_credentials
from app.settings import settings
from utils.retry_apis import _retry_call
import requests
from datatron.common.discovery import DatatronDiscovery
from app.governor.datatron_metrics import MetricsManager

logging.basicConfig(format=settings.DEFAULT_LOG_FORMAT, level=logging.INFO)

class BatchMetricsJob:

    def __init__(self):
        logging.info("Received batch metrics job for batch-id: {} with metric-args: {}".format(settings.BATCH_ID, settings.METRIC_ARGS))
        self.batch_id = settings.BATCH_ID
        self.job_id = settings.JOB_ID
        self.workspace_slug = settings.WORKSPACE_SLUG
        self.metric_args = settings.METRIC_ARGS
        self.delimiter = chr(int(settings.DELIMITER, 16))
        self.prediction_filepath = settings.REMOTE_INPUT_FILEPATH
        self.feedback_filepaths = settings.REMOTE_FEEDBACK_FILEPATH_LIST
        self.prediction_filename = self.prediction_filepath.rpartition('/')[2]
        self.metrics_intermediate_dir = os.path.join(settings.METRICS_DIR, self.job_id)
        os.makedirs(self.metrics_intermediate_dir, exist_ok=True)
    
    @staticmethod
    def _get_service_discovery_client():
        dsd_discovery_client = DatatronDiscovery(discovery_type=settings.DISCOVERY_TYPE,
                                                 services_type='infrastructure',
                                                 hosts=settings.SHIVA_ZOOKEEPER_HOSTS,
                                                 caching=False)
        return dsd_discovery_client

    def _request_to_dictator(self, request_type, subroute, payload=None):
        logging.info('Requesting dictator for request: {}, subroute: {}, payload: {}'.format(request_type,
                                                                                             subroute,
                                                                                             str(payload)))
        dsd_client = self._get_service_discovery_client()
        dictator_url = dsd_client.get_single_instance(service_path='dictator', pick_random=True)
        header = {'Content-Type': 'application/json'}
        full_url = dictator_url + subroute

        if request_type == 'get':
            result = _retry_call(requests.get, url=full_url, headers=header)
        elif request_type == 'put':
            result = _retry_call(requests.put, url=full_url, headers=header, data=json.dumps(payload))
        else:
            raise ValueError('Inavlid request type initiated for dictator')

        dsd_client.stop()

        if result.status_code != 200:
            # TODO: In future, map the status code to check against from env or dictator constants
            raise Exception('Failed to process metadata with dictator service')

        result_data = json.loads(result.content.decode('utf-8'))
        logging.info('Received result data for subroute: {} as : {}'.format(subroute, str(result_data)))

        return result_data

    def _update_status_to_dictator(self, status, status_meta):
        update_status_payload = {
            'status': status,
            'status_meta': status_meta
        }
        self._request_to_dictator('put',
                                  subroute='/api/scorer_job/{}'.format(self.batch_id),
                                  payload=update_status_payload)
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

    def fetch_remote_file(self, remote_path, local_prefix, connector=None):
        logging.info(
            'Getting the remote file for batch job from : {}, locally at: {}'.format(remote_path, local_prefix))
        local_filepath = self._create_local_path(remote_path, local_prefix)
        if not connector:
            credentials = generate_credentials_for_internal_storage()
        else:
            credentials = generate_credentials(connector)
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
        logging.debug('Calculating the optimal chunksize for the provided file: {}'.format(filename))
        bytes_per_row = self._calculate_byte_row(filepath)
        logging.debug('Average mem usage per row: {} byte'.format(str(bytes_per_row)))
        optimal_rows_for_mem = int(self.ALLOWED_MAX_BYTES / (2*bytes_per_row)) # Multiply by 2 for feedback files
        logging.debug('Optimal chuck size for memory of {} byte: {}'.format(str(self.ALLOWED_MAX_BYTES), str(optimal_rows_for_mem)))
        _chunksize = self.DEFAULT_CHUNK if (bytes_per_row * self.DEFAULT_CHUNK) < self.ALLOWED_MAX_BYTES else optimal_rows_for_mem
        logging.debug('Estimated optimal chunksize is {}'.format(str(_chunksize)))
        return _chunksize

    def save_metrics(self, file_name, metric_vals, job_id):
        file_path = os.path.join(self.metrics_intermediate_dir, file_name)
        try:
            with open(file_path, "w") as fopen:
                json.dump(metric_vals, fopen)
        except FileNotFoundError as e:
            logging.error(f"Metrics for matadata calculated for job-id {job_id} could not be saved due to file {file_name} not being found.")

    # @log_time_taken(f"Calculating batch metrics for prediction file: {self.prediction_filepath} for job-id: {self.job_id}")
    def process_batch(self):
        chunksize = 1e6 # Make this a variable in the settings or determine dynamically
        if self.metric_args == {}:
            logging.info("No metric were calculated as metric arguments were not provided.")
            return
        try:
            self.metrics_manager = MetricsManager(self.metric_args)
            running_status_meta = {'status_code': 202, 'status_msg': 'BatchScoringLiteMetricsInProgress'}
            try:
                self._update_status_to_dictator(status='RUNNING', status_meta=running_status_meta)
            except:
                logging.info("Could not set dictator status for job to running.")
            local_prediction_filepath = self.fetch_remote_file(remote_path=self.prediction_filepath, local_prefix='input', connector=settings.INPUT_CONNECTOR)
            self.chunk_size = min(self._calculate_byte_row(local_prediction_filepath), settings.CHUNK_SIZE)
            logging.debug("Successfully received prediction file from {}".format(local_prediction_filepath))
            for prediction_chunk in pd.read_csv(local_prediction_filepath, 
                                                chunksize=chunksize, 
                                                delimiter=self.delimiter):
                for feedback_filepath in self.feedback_filepaths:
                    cur_feed_time = time.time()
                    local_feedback_filepath = self.fetch_remote_file(remote_path=feedback_filepath, local_prefix='output', connector=settings.OUTPUT_CONNECTOR)
                    logging.debug("Successfully received feedback file from {}".format(local_prediction_filepath))
                    for feedback_chunk in pd.read_csv(local_feedback_filepath, 
                                                    chunksize=chunksize,
                                                    delimiter=self.delimiter):
                        try:
                            joined_df = pd.merge(prediction_chunk, feedback_chunk, left_on = "datatron_request_id", right_on = "datatron_request_id", how="inner", suffixes=[None,'_y'])
                            self.metrics_manager.batch_update(np.array(joined_df["actual_value"]), np.array(joined_df["prediction"]))
                        except KeyError as e:
                            logging.error(f"Could not join datatron_id column in either the prediction file: {local_prediction_filepath} or feedback file: {local_feedback_filepath} due to error: {str(e)}.")
                    self.delete_local_file(local_feedback_filepath)
                    logging.debug("Finished calculating metrics on feedback_file: {} in {} seconds".format(feedback_filepath, self.calculate_duration(cur_feed_time)))
            metrics_file = "{}.json".format(self.batch_id)
            metric_values = self.metrics_manager.fetch_metric_values()
            logging.debug("Metric values for batch {} are {}".format(self.batch_id, metric_values))
            self.save_metrics(metrics_file, metric_values, self.job_id)
            batch_status = "SUCCESS"
            status_msg = "BatchMetricsScoringLiteSuccess"
            status_code = 200
        except FileNotFoundError as e:
            logging.error("Metrics processing for batch id {} failed due to error: {}".format(self.batch_id, str(e)))
            batch_status = "FAILED"
            status_msg = "BatchMetricsScoringLiteFAILED due to file not found"
            status_code = 500
            traceback.print_stack()
            traceback.print_exc()
        except Exception as e:
            logging.error("The batch {} couldn't be processed due to the following errors: {}".format(self.batch_id, str(e)))
            batch_status = "FAILED"
            status_msg = "BatchMetricsScoringLiteFAILED"
            status_code = 500
            traceback.print_stack()
            traceback.print_exc()
        finally:
            status_meta = dict()
            status_meta["status_code"] = status_code
            status_meta["status_msg"] = status_msg
            logging.info('Batch predict job finished with status msg: {}, status code: {}'.format(status_msg,
                                                                                                  str(status_code)))
        self._update_status_to_dictator(status=batch_status, status_meta=status_meta)
        logging.info('Finished the batch job with exit status: {}'.format(batch_status))

if __name__ == '__main__':
    batch_metrics_job = BatchMetricsJob()
    batch_metrics_job.process_batch()