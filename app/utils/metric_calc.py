import math
import numpy as np
from utils import logger
from sklearn.metrics import (roc_auc_score, explained_variance_score, precision_recall_curve, auc)
from sklearn.utils.multiclass import type_of_target, unique_labels
from app.governor.datatron_metrics import MetricsManager


def calculate_scores(feedback, predictions, learn_type):
    try:
        if not inputs_are_valid(feedback, predictions):
            return

        if learn_type == 'regression':
            scores = calculate_regression_scores(feedback, predictions)
        else:
            scores = calculate_classification_scores(feedback, predictions)

        return scores
    except Exception as e:
        logger.exception(f'An exception occurred while trying to calculate scores: {e}')
        return


def calculate_regression_scores(feedback, predictions):
    metric_args = {"mse": {}, "rmse": {}, "mae": {}, "r2": {}}
    manager = MetricsManager(metric_args)
    manager.batch_update(np.array(feedback), np.array(predictions))
    custom_scores = manager.fetch_metric_values()
    scores = {
        "au_prc": explained_variance_score(feedback, predictions)
    }
    for metric in custom_scores:
        scores[metric] = scores[metric]["value"]

    return scores


def calculate_classification_scores(feedback, predictions, is_binary):
    feedback_target = type_of_target(feedback)
    prediction_target = type_of_target(predictions)
    is_binary = 'binary' == feedback_target == prediction_target

    pos_label = max(unique_labels(feedback, predictions)) if is_binary else None
    metric_args = {"accuracy": {'positive_label': pos_label}, "precision": {'positive_label': pos_label}, "recall": {'positive_label': pos_label}, "f1": {'positive_label': pos_label}}
    manager = MetricsManager(metric_args)
    manager.batch_update(np.array(feedback), np.array(predictions))
    custom_scores = manager.fetch_metric_values()
    scores = {}
    for metric in custom_scores:
        scores[metric] = scores[metric]["value"]

    is_feedback_unary = len(unique_labels(feedback)) == 1
    if not is_feedback_unary and not any(isinstance(d, str) for d in feedback + predictions):
        if is_binary:
            precision, recall, _ = precision_recall_curve(feedback, predictions, pos_label=pos_label)
            precision = np.array(precision)
            recall = np.array(recall)
            indices = precision.argsort()
            au_prc = auc(precision[indices], recall[indices])
            if not math.isnan(au_prc):
                scores['au_prc'] = au_prc

        if type_of_target(feedback) != 'multiclass':
            scores['au_roc'] = roc_auc_score(feedback, predictions, average='weighted', multi_class='ovr')

    return scores



def inputs_are_valid(feedback, predictions):
    is_valid = True
    if len(feedback) < 1:
        is_valid = False
        logger.warning(f'feedback received by static_metrics was invalid: {feedback}')
    if len(predictions) < 1:
        is_valid = False
        logger.warning(f'predictions received by static_metrics was invalid: {predictions}')
    return is_valid