import os
from collections import OrderedDict

import msgpack
from atomicwrites import atomic_write
from filelock import FileLock

from utils import set_default, logger
from app.settings import VOLUME_PATH

WINDOWS_FILENAME = 'windows.msgpack'
PREDICTIONS_MAP_FILENAME = 'predictions_map.msgpack'
FEEDBACK_MAP_FILENAME = 'feedback_map.msgpack'
PREDICTION_TIME_MAP_FILENAME = 'prediction_time_map.msgpack'

window_data_directory = os.path.join(VOLUME_PATH, 'window-data')


class WindowsIOHandler:
    def __init__(self, challenger_slug):
        self.challenger_slug = challenger_slug
        self.challenger_directory = os.path.join(window_data_directory, self.challenger_slug)
        self.feedback_map_path = os.path.join(self.challenger_directory, FEEDBACK_MAP_FILENAME)

    def read_windows(self, model_version_slug):
        model_directory = os.path.join(self.challenger_directory, model_version_slug)
        os.makedirs(model_directory, exist_ok=True)

        file_path = os.path.join(model_directory, WINDOWS_FILENAME)
        lock_path = file_path + ".lock"
        lock = FileLock(lock_path)

        logger.debug(f'About to try to read windows for challenger {self.challenger_slug} and '
                     f'model version {model_version_slug}')

        try:
            with lock, open(file_path, 'rb') as windows_file:
                byte_data = windows_file.read()
                windows = msgpack.unpackb(byte_data, object_pairs_hook=OrderedDict)
                for window_time, window in windows.items():
                    windows[window_time] = set(window)
                return windows
        except FileNotFoundError:
            return OrderedDict()

    def write_windows(self, model_version_slug, windows):
        model_directory = os.path.join(self.challenger_directory, model_version_slug)
        os.makedirs(model_directory, exist_ok=True)

        file_path = os.path.join(model_directory, WINDOWS_FILENAME)
        lock_path = file_path + ".lock"
        lock = FileLock(lock_path)

        logger.debug(f'About to try to write windows for challenger {self.challenger_slug} and '
                     f'model version {model_version_slug}')

        with lock, atomic_write(file_path, mode='wb', overwrite=True) as windows_file:
            logger.info(f'Writing windows file for challenger {self.challenger_slug} and '
                        f'model version {model_version_slug}')
            byte_data = msgpack.packb(windows, default=set_default)
            windows_file.write(byte_data)

    def read_model_version_slugs(self):
        logger.debug(f'About to try to read model version slugs for challenger {self.challenger_slug}')

        return [o for o in os.listdir(self.challenger_directory) if
                os.path.isdir(os.path.join(self.challenger_directory, o))]

    def read_prediction_time_map(self, model_version_slug):
        logger.debug(f'About to try to read prediction time map for challenger {self.challenger_slug} and '
                     f'model version {model_version_slug}')

        try:
            with open(os.path.join(self.challenger_directory, model_version_slug, PREDICTION_TIME_MAP_FILENAME),
                      'rb') as prediction_time_map_file:
                byte_data = prediction_time_map_file.read()
                return msgpack.unpackb(byte_data)
        except FileNotFoundError:
            return {}

    def write_prediction_time_map(self, model_version_slug, prediction_time_map):
        with atomic_write(os.path.join(self.challenger_directory, model_version_slug, PREDICTION_TIME_MAP_FILENAME),
                          mode='wb', overwrite=True) as prediction_time_map_file:
            logger.info(f'Writing prediction time map file for challenger {self.challenger_slug} '
                        f'and model version {model_version_slug}')
            byte_data = msgpack.packb(prediction_time_map)
            prediction_time_map_file.write(byte_data)

    def write_predictions_map(self, model_version_slug, predictions_map):
        predictions_map_dir = os.path.join(self.challenger_directory, model_version_slug)
        os.makedirs(predictions_map_dir, exist_ok=True)

        predictions_map_file_path = os.path.join(predictions_map_dir, PREDICTIONS_MAP_FILENAME)
        lock_path = predictions_map_file_path + ".lock"
        lock = FileLock(lock_path)
        with lock, atomic_write(predictions_map_file_path, mode='wb', overwrite=True) as predictions_map_file:
            logger.info(f'Writing predictions map file for challenger {self.challenger_slug} '
                        f'and model version {model_version_slug}')
            byte_data = msgpack.packb(predictions_map, default=set_default)
            predictions_map_file.write(byte_data)

    def read_predictions_map(self, model_version_slug):
        predictions_map_dir = os.path.join(self.challenger_directory, model_version_slug)
        os.makedirs(predictions_map_dir, exist_ok=True)

        predictions_map_file_path = os.path.join(predictions_map_dir, PREDICTIONS_MAP_FILENAME)
        lock_path = predictions_map_file_path + ".lock"
        lock = FileLock(lock_path)

        logger.debug(f'About to try to read predictions map for challenger {self.challenger_slug} and '
                     f'model version {model_version_slug}')

        try:
            with lock, open(predictions_map_file_path, 'rb') as predictions_map_file:
                byte_data = predictions_map_file.read()
                return msgpack.unpackb(byte_data)
        except FileNotFoundError:
            return {}

    def write_feedback_map(self, feedback_map):
        os.makedirs(self.challenger_directory, exist_ok=True)

        lock_path = self.feedback_map_path + ".lock"
        lock = FileLock(lock_path)
        with lock, atomic_write(self.feedback_map_path, mode='wb', overwrite=True) as feedback_map_file:
            logger.info(f'Writing predictions map file for challenger {self.challenger_slug}')
            byte_data = msgpack.packb(feedback_map, default=set_default)
            feedback_map_file.write(byte_data)

    def read_feedback_map(self):
        os.makedirs(self.challenger_directory, exist_ok=True)

        lock_path = self.feedback_map_path + ".lock"
        lock = FileLock(lock_path)

        logger.debug(f'About to try to read feedback map for challenger {self.challenger_slug}')

        try:
            with lock, open(self.feedback_map_path, 'rb') as feedback_map_file:
                byte_data = feedback_map_file.read()
                return msgpack.unpackb(byte_data)
        except FileNotFoundError:
            return {}
