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
    # The driver sends no more than the strictly necessary RESET messages.
    OPT_MINIMAL_RESETS = "Optimization:MinimalResets"
    # The driver caches connections (e.g., in a pool) and doesn't start a new
    # one (with hand-shake, HELLO, etc.) for each query.
    OPT_CONNECTION_REUSE = "Optimization:ConnectionReuse"
    # The driver doesn't wait for a SUCCESS after calling RUN but pipelines a
    # PULL right afterwards and consumes two messages after that. This saves a
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

    # Temporary driver feature that will be removed when all official drivers
    # have been unified in their behaviour of when they return a Result object.
    # We aim for drivers to not providing a Result until the server replied with
    # SUCCESS so that the result keys are already known and attached to the
    # Result object without further waiting or communication with the server.
    TMP_RESULT_KEYS = "Temporary:ResultKeys"
    # Temporary driver feature that will be removed when all official driver
    # backends have implemented all summary response fields.
    TMP_FULL_SUMMARY = "Temporary:FullSummary"
    # Temporary driver feature that will be removed when all official driver
    # backends have implemented path and relationship types
    TMP_CYPHER_PATH_AND_RELATIONSHIP = "Temporary:CypherPathAndRelationship"
    # Temporary driver feature that will be removed when all official driver
    # backends have implemented the TransactionClose request
    TMP_TRANSACTION_CLOSE = "Temporary:TransactionClose"
    # Temporary driver feature that will be removed when all official driver
    # backends have implemented fetch size configuration on driver creation
    TMP_DRIVER_FETCH_SIZE = "Temporary:DriverFetchSize"
