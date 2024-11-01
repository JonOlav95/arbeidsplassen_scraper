import logging
import requests

from tenacity import retry, wait_fixed, stop_after_attempt


@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def request_with_retrying(url: str, headers: dict):
    try:
        response = requests.get(url=url, headers=headers)
        response.raise_for_status()
        return response
    except requests.exceptions.HTTPError as http_err:
        status_code = http_err.response.status_code
        
        if 400 <= status_code < 500:
            logging.error(f'Client error {status_code}: {http_err}')
        elif 500 <= status_code < 600:
            logging.warning(f'Server error {status_code}: {http_err}')
        else:
            logging.error(f'Unexpected HTTP error: {http_err}')
        raise

    except requests.exceptions.ConnectionError as conn_err:
        logging.warning(f'Connection error: {conn_err}')
        raise
    except requests.exceptions.Timeout as timeout_err:
        logging.warning(f'Request timed out: {timeout_err}')
        raise

    except Exception as err:
        logging.error(f'An unexpected error occurred: {err}')
        raise


def requests_wrapper(url: str, headers: dict):
    """Request get wrapper, returns None on failed request."""
    try:
        return request_with_retrying(url, headers)
    except Exception as e:
        logging.error(f'Request failed after retries: {e}')
        return None
