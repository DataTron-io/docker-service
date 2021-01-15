import requests
import backoff


@backoff.on_exception(backoff.expo,
                      requests.exceptions.HTTPError,
                      max_tries=4)
@backoff.on_exception(backoff.expo,
                      requests.exceptions.Timeout,
                      max_tries=8)
@backoff.on_exception(backoff.expo,
                      requests.exceptions.ConnectionError,
                      max_tries=8)
def _retry_call(fn, *args, **kwargs):
    return fn(*args, **kwargs)
