import math
import functools
import sys

import requests
import backoff
import simplejson
import singer
from asana.error import (
    NoAuthorizationError,
    RetryableAsanaError,
    InvalidTokenError,
    RateLimitEnforcedError,
)
from asana.page_iterator import CollectionPageIterator
from oauthlib.oauth2 import TokenExpiredError
from singer import utils
from tap_asana.context import Context

LOGGER = singer.get_logger()

# Setting default timeout as 300 seconds
REQUEST_TIMEOUT = 300

# Retry the request in the factor of 2 ie. 2, 4, 8, ...
FACTOR = 2

RESULTS_PER_PAGE = 250

# We've observed 500 errors returned if this is too large (30 days was too
# large for a customer)
DATE_WINDOW_SIZE = 1

# We will retry a 500 error a maximum of 5 times before giving up
MAX_RETRIES = 5


def is_not_status_code_fn(status_code):
    """Check for status code"""

    def gen_fn(exc):
        if getattr(exc, "code", None) and exc.code not in status_code:
            return True
        # Retry other errors up to the max
        return False

    return gen_fn


def leaky_bucket_handler(details):
    """Function to handle leaky bucket"""
    LOGGER.info("Received 429 -- sleeping for %s seconds", details["wait"])


def retry_handler(details):
    """Function for retry handler"""
    LOGGER.info(
        "Received 500 or retryable error -- Retry %s/%s", details["tries"], MAX_RETRIES
    )


# pylint: disable=unused-argument

def retry_after_wait_gen(**kwargs):
    # This is called in an except block so we can retrieve the exception
    # and check it.
    exc_info = sys.exc_info()
    resp = exc_info[1].response
    # Retry-After is an undocumented header. But honoring
    # it was proven to work in our spikes.
    sleep_time_str = resp.headers.get("Retry-After")
    yield math.floor(float(sleep_time_str))


def invalid_token_handler(details):
    """Function to handle invalid token"""
    LOGGER.info("Received invalid or expired token error, refreshing access token")
    Context.asana.refresh_access_token()


def asana_error_handling(fnc):
    """Function for error handling"""

    @backoff.on_exception(
        backoff.expo, requests.Timeout, max_tries=MAX_RETRIES, factor=FACTOR
    )
    @backoff.on_exception(
        backoff.expo,
        (InvalidTokenError, NoAuthorizationError, TokenExpiredError),
        on_backoff=invalid_token_handler,
        max_tries=MAX_RETRIES,
    )
    @backoff.on_exception(
        backoff.expo,
        (simplejson.scanner.JSONDecodeError, RetryableAsanaError),
        giveup=is_not_status_code_fn(range(500, 599)),
        on_backoff=retry_handler,
        max_tries=MAX_RETRIES,
    )
    @backoff.on_exception(
        retry_after_wait_gen,
        RateLimitEnforcedError,
        giveup=is_not_status_code_fn([429]),
        on_backoff=leaky_bucket_handler,
        # No jitter as we want a constant value
        jitter=None,
    )
    @functools.wraps(fnc)
    def wrapper(*args, **kwargs):
        return fnc(*args, **kwargs)

    return wrapper


# Apply retry decorators to the Asana SDK iterators
CollectionPageIterator.get_initial = asana_error_handling(
    CollectionPageIterator.get_initial
)
CollectionPageIterator.get_next = asana_error_handling(
    CollectionPageIterator.get_next
)


class Stream:
    """
    Base stream class with bookmarking and API call logic.
    """
    # Used for bookmarking and stream identification. Overridden by subclasses.
    name = None
    replication_method = None
    replication_key = None
    key_properties = ["gid"]

    def __init__(self):
        # Configure request timeout
        config_timeout = Context.config.get("request_timeout")
        self.request_timeout = (
            float(config_timeout) if config_timeout and float(config_timeout) else REQUEST_TIMEOUT
        )

    def get_bookmark(self):
        """Retrieve the last saved bookmark or start_date from config."""
        bookmark = (
            singer.get_bookmark(
                Context.state,
                self.name,
                self.replication_key,
            )
            or Context.config["start_date"]
        )
        return utils.strptime_to_utc(bookmark)

    def is_bookmark_old(self, value):
        """Check if a record's replication key is newer than the bookmark."""
        bookmark = self.get_bookmark()
        return utils.strptime_to_utc(value) >= bookmark

    def update_bookmark(self, value):
        """Update the stored bookmark if the value is newer."""
        try:
            ts = value.strftime("%Y-%m-%dT%H:%M:%S.%f")
        except AttributeError:
            ts = value
        if self.is_bookmark_old(ts):
            singer.write_bookmark(
                Context.state,
                self.name,
                self.replication_key,
                ts,
            )
            singer.write_state(Context.state)

    @staticmethod
    def get_updated_session_bookmark(session_bookmark, value):
        """Return the later of session_bookmark and value."""
        try:
            session = utils.strptime_with_tz(session_bookmark)
        except TypeError:
            session = session_bookmark
        try:
            new = utils.strptime_with_tz(value)
        except TypeError:
            new = value
        return new if new > session else session

    def call_api(self, resource, **query_params):
        """Make an API call with retry handling and timeout."""
        api_fn = getattr(Context.asana.client, resource)
        query_params["timeout"] = self.request_timeout
        return api_fn.find_all(**query_params)

    @staticmethod
    def get_project_gids():
        """
        Retrieve project IDs from config or fetch all projects across workspaces.

        Returns:
            List[str]: A list of Asana project GIDs.
        """
        # If project_id is specified in config, normalize to list
        pid = Context.config.get("project_gid")
        return pid

    def sync(self):
        """Yield processed objects from the stream."""
        for obj in self.get_objects():
            yield obj
