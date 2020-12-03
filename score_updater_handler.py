import os
import uuid
import time
import numpy as np
import requests
import logging
from datetime import datetime
from utils import hash_func, logger, SlugKey
from app.settings import settings
from app.utils.metric_calc import calculate_scores
from app.utils.windows_io_handler import WindowsIOHandler

logging.basicConfig(format=settings.DEFAULT_LOG_FORMAT, level=logging.INFO)

schema.ArraySchema.__hash__ = hash_func
model_catalog_overview_url = settings.DICTATOR_BASE_URL + '/model-catalog/{}/overview'

class ScoreUpdaterHandlerJob:

    def __init__(self):
        self.window_data = settings.WINDOW_STORE_DIR
        self.model_slug_learn_types = {}

    def process_batch(self):
        challenger_slug = None
        model_version_slug = None

        windows_scorer_io = None
        feedback_map = None
        grouped_keys = sorted(self.window_data.keys())

        for key in grouped_keys:
            try: 
                if key.challenger_key != challenger_slug:
                    windows_scorer_io = WindowsIOHandler(challenger_slug)
                    feedback_map = windows_scorer_io.read_feedback_map()
                
                model_version_slug = key.model_version_slug
                learn_type = self._pull_model_learn_type(model_version_slug)
                if learn_type is None:
                    logger.warning(f'The learn type was not pulled successfully for {model_version_slug}. '
                                    f'The score recalculation must skip.')
                    continue

                window_times = self.window_data[key][0]
                default_feedback = self.window_data[key][1]

                windows = windows_scorer_io.read_windows(model_version_slug)
                predictions_map = windows_scorer_io.read_predictions_map(model_version_slug)

                logger.info(
                    f'Trying to score {len(window_times)} points for challenger {challenger_slug} '
                    f'and model version {model_version_slug}')
                window_score_map = {}
                for window_time in window_times:
                    window = windows.get(window_time)

                    logger.debug(f'Getting ready to score for challenger {challenger_slug} and model version '
                                        f'{model_version_slug} for window {window_time}')

                    if window is None:
                        logger.warning(f'Window pulled for window time {window_time} was empty at {datetime.now()}')
                        continue
                    prediction_list = []
                    feedback_list = []
                    for datatron_request_id in window:
                        prediction = predictions_map.get(datatron_request_id)
                        feedback = feedback_map.get(datatron_request_id)

                        if feedback is None and default_feedback is not None:
                            feedback = type(prediction)(default_feedback)

                        if type(prediction) == dict or type(feedback) == dict:
                            logger.warning('A request was received to try to score predictions/feedback with more '
                                            'than 1 key-value pair. This is currently unsupported')
                            break

                        prediction, feedback = self._resolve_value_types(prediction, feedback)

                        if prediction is not None and feedback is not None:
                            prediction_list.append(prediction)
                            feedback_list.append(feedback)

                    if len(prediction_list) > 0 and len(feedback_list) > 0:
                        logger.debug(f'Scoring for challenger {challenger_slug} and model version '
                                        f'{model_version_slug} for window {window_time}')

                        scores = calculate_scores(np.array(prediction_list), np.array(feedback_list), learn_type)
                        if scores is None:
                            logger.warning(f'No scores were able to be created for challenger {challenger_slug} and'
                                            f' model version {model_version_slug} at window time: {window_time}')
                        else:
                            window_score_map[window_time] = scores
                    else:
                        logger.warning(
                            f'The window time {window_time} was not available for challenger slug {challenger_slug}'
                            f' and model version slug {model_version_slug} when trying to calculate scores for the'
                            f' window')

                logger.info(f'{len(window_score_map)} points were successfully scored for challenger '
                            f'{challenger_slug} and model version {model_version_slug}')

                for window_time, scores in window_score_map.items():
                    request_value = {'window_time': window_time, 'scores': scores}
                    self.producer.produce(topic='realtime-scores-updater', value=request_value,
                                            key={'challenger_slug': challenger_slug,
                                                'model_version_slug': model_version_slug})
            except Exception as e:
                if challenger_slug is None:
                    logger.exception(f'An exception was thrown while trying to process a request to score metrics')
                else:
                    logger.exception(
                        f'An exception was thrown while scoring for challenger {challenger_slug} and model version '
                        f'{model_version_slug}. This slug key pair will be removed from the current batch.\n{e}')
                    try:
                        self.window_data.pop(SlugKey(challenger_slug, model_version_slug))
                    except KeyError:
                        logger.warning(f'The slug key of challenger {challenger_slug} and {model_version_slug} was not'
                                       f' available to removed from the batch map after an exception was thrown')

    def _pull_model_learn_type(self, model_version_slug):
        model_slug = '-'.join(model_version_slug.split('-')[:-1])
        learn_type = self.model_slug_learn_types.get(model_slug)
        if learn_type is not None:
            return learn_type
        try:
            response_json = requests.get(model_catalog_overview_url.format(model_slug), timeout=(5, 15),
                                         headers={'Content-Type': 'application/json'}).json()
            learn_type = response_json['result']['learn_type']

            if learn_type:
                self.model_slug_learn_types[model_slug] = learn_type
                logger.info(f'Pulled learn type for model_slug {model_slug} was {learn_type}')
                return learn_type
        except Exception as e:
            logger.exception(f'Exception thrown while trying to pull learn type: {e}')

        logger.warning(f'Pulling a learn type was unsuccessful for model_slug {model_slug}')
        return None

    def _resolve_value_types(self, prediction, feedback):
        if str not in (type(prediction), type(feedback)):
            return prediction, feedback

        prediction = self._resolve_type(prediction)
        feedback = self._resolve_type(feedback)

        if type(prediction) == type(feedback) or all(t in (type(feedback), type(prediction)) for t in (float, int)):
            return prediction, feedback

        return None, None

    def _resolve_type(self, value):
        if type(value) == str:
            successful, value = self._try_type_cast(value, int)
            if not successful:
                _, value = self._try_type_cast(value, float)
        return value

    def _try_type_cast(self, value, cast_type):
        try:
            return True, cast_type(value)
        except ValueError:
            return False, value



if __name__ == '__main__':
    batch_job_obj = ScoreUpdaterHandlerJob()
    batch_job_obj.process_batch()