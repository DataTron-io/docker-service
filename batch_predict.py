import os
import uuid
import time
import logging
import json
import pandas as pd
import ast
from app.utils import hdfs_transfer as ht
from app.settings import settings
import requests
from datatron.common.discovery import DatatronDiscovery
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
        
        #make a local directory if it does not exists
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

    def add_request_ids(self, dframe):
        logging.info('Starting to update the current frame with the datatron requests id')
        if 'datatron_request_id' in dframe:
            logging.info('Datatron requests id already present, skipping generation of new ids')
            return dframe
        dframe['datatron_request_id'] = [self._generate_trace_id() for _ in range(len(dframe))]
        logging.info('Finished updating the current frame with the datatron requests id')
        return dframe
    
    def _get_service_discovery_client(self):
        dsd_discovery_client = DatatronDiscovery(discovery_type=settings.DISCOVERY_TYPE,
                                                 services_type='infrastructure',
                                                 hosts=settings.SHIVA_ZOOKEEPER_HOSTS,
                                                 caching=False)
        return dsd_discovery_client

    def batch_predict(self, json_data, proba=False):
        dsd_client = self._get_service_discovery_client()
        dictator_url = dsd_client.get_single_instance(service_path='dictator', pick_random=True)
        full_url = dictator_url + '/api/batch_prediction/{}'.format(settings.BATCH_ID)
        batch_response = requests.get(url=full_url)
        dsd_client.stop()
        deploy_data = batch_response.json()
        model_features = deploy_data['model']['features']
        logging.info("Features: {}".format(model_features))
        logging.info("json_data: {}".format(json_data))
        validated_features = {}
        for feature in model_features:
            if feature in json_data:
                validated_features[feature] = json_data[feature]

        logging.info('Validated Features: {}'.format(validated_features))

        endpoint = settings.PROBA_ENDPOINT if proba else settings.PREDICT_ENDPOINT
        port = settings.APIPORT
        if endpoint[0] != '/':
            endpoint = '/' + endpoint
        #sends API post request to client's container
        response = requests.post("http://localhost:" + port + endpoint, json=validated_features)
        return response

    def process_batch(self):
        logging.info('Starting the batch process for the batch id: {}'.format(self.batch_id))
        try:
            #start time of process batch
            file_process_start = time.time()

            model_key = str(self.model_version_slug) + '__' + str(self.learn_type)
            #Generate input and output .csv paths
            local_input_filepath = self.fetch_remote_file(remote_path=self.remote_input_filepath, local_prefix='input')
            input_filename = self.remote_input_filepath.rpartition('/')[2] 
            local_output_filepath = self._create_local_path(remote_path=self.remote_output_filepath, local_prefix='output') 
            
            #Checks if file is zip/tar.gz file
            compress = True if '.gz' in local_output_filepath.rpartition('/')[2] else False 
            is_first_frame = True
            
            #each_chunk is one data predict request to publisher
            for each_chunk in pd.read_csv(filepath_or_buffer=local_input_filepath, 
                                          chunksize=self.chunk_size,
                                          delimiter=self.delimiter):

                logging.info('Starting to process new chunk for the batch file: {}'.format(each_chunk))
                #start time for chunk_process & adding request_id
                chunk_process_start = time.time()
                requestid_add_start = time.time()
                #add datatron_request_id column using respective request_id
                each_chunk = self.add_request_ids(each_chunk)
                #sets new index into first column of extract line in df
                each_chunk = each_chunk.set_index('datatron_request_id') 
                logging.info("Finished adding trace ids for current frame in : {}".
                             format(self.calculate_duration(requestid_add_start)))

                model_predict_start = time.time()
                logging.info('Calling predict batch on the model: {} , for current frame'.format(model_key))
                #changes each_chunk into json str format
                ast_list=each_chunk.to_json(orient='records')
                #changes json list to json list format
                ast_output =ast.literal_eval(ast_list)
                logging.info("Sending ast_output to prediction: {}".format(ast_output))
                #Gets prediction of current frame from docker api endpoint
                output = self.batch_predict(ast_output[0])
                #Inserts prediction into dataframe for storage
                logging.info("Prediction output: {}".format(output))
                predict_df = pd.DataFrame(output, columns=['outputs'])
                predict_df.index = each_chunk.index.values

                logging.info('Model: {} finished the prediction for the current frame in: {}'
                             .format(model_key, self.calculate_duration(model_predict_start)))

                predict_merge_start = time.time()
                predict_df = predict_df.add_prefix(model_key + '__')
                each_chunk = each_chunk.merge(predict_df, how='outer', left_index=True, right_index=True)

                logging.info('Finished merging the prediction result with existing base frame in: {}'
                             .format(self.calculate_duration(predict_merge_start)))
                chunk_append_start = time.time()
                
                #Saves current prediction frame into either csv or zip formats
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