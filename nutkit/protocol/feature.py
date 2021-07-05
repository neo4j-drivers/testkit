"""
Enumerate all the capabilities in the drivers
"""
from enum import Enum


class Feature(Enum):
    # === OPTIMIZATIONS ===
    # On receiving Neo.ClientError.Security.AuthorizationExpired, the driver
    # shouldn't reuse any open connections for anything other than finishing
    # a started job. All other connections should be re-established before
    # running the next job with them.
    OPT_AUTHORIZATION_EXPIRED_TREATMENT = 'AuthorizationExpiredTreatment'
    # Driver doesn't explicitly send message data that is the default value.
    # This conserves bandwidth.
    OPT_IMPLICIT_DEFAULT_ARGUMENTS = "Optimization:ImplicitDefaultArguments"
    # The driver sends no more the the strictly necessary RESET messages.
    OPT_MINIMAL_RESETS = "Optimization:MinimalResets"
    # The driver caches connections (e.g., in a pool) and doesn't start a new
    # one (with hand-shake, HELLO, etc.) for each query.
    OPT_CONNECTION_REUSE = "Optimization:ConnectionReuse"
    # The driver doesn't wait for a SUCCESS after calling RUN but pipelines a
    # PULL right afterwards and consumes two message after that. This saves a
    # full round-trip.
    OPT_PULL_PIPELINING = "Optimization:PullPipelining"

    # === CONFIGURATION HINTS (BOLT 4.3+) ===
    # The driver understands and follow the connection hint
    # connection.recv_timeout_seconds which tells it to close the connection
    # after not receiving an answer on any request for longer than the given
    # time period. On timout, the driver should remove the server from its
    # routing table and assume all other connections to the server are dead
    # as well.
    CONF_HINT_CON_RECV_TIMEOUT = "ConfHint:connection.recv_timeout_seconds"
