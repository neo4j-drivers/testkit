"""Enumerate all the capabilities in the drivers."""
from enum import Enum


class Feature(Enum):
    # === FUNCTIONAL FEATURES ===
    # The driver offers a method for checking if a connection to the remote
    # server of cluster can be established and retrieve the server info of the
    # reached remote.
    API_DRIVER_GET_SERVER_INFO = "Feature:API:Driver:GetServerInfo"
    # The driver offers a method for driver objects to report if they were
    # configured with a or without encryption.
    API_DRIVER_IS_ENCRYPTED = "Feature:API:Driver.IsEncrypted"
    # The driver offers a method for checking if a connection to the remote
    # server of cluster can be established.
    API_DRIVER_VERIFY_CONNECTIVITY = "Feature:API:Driver.VerifyConnectivity"
    # The driver offers a method for the result to return all records as a list
    # or array. This method should exhaust the result.
    API_RESULT_LIST = "Feature:API:Result.List"
    # The driver offers a method for the result to peek at the next record in
    # the result stream without advancing it (i.e. without consuming any
    # records)
    API_RESULT_PEEK = "Feature:API:Result.Peek"
    # The driver offers a method for the result to retrieve exactly one record.
    # This methods asserts that exactly one record in left in the result
    # stream, else it will raise an exception.
    API_RESULT_SINGLE = "Feature:API:Result.Single"
    # The driver supports connection liveness check.
    API_LIVENESS_CHECK = "Feature:API:Liveness.Check"
    # The driver implements explicit configuration options for SSL.
    #  - enable / disable SSL
    #  - verify signature against system store / custom cert / not at all
    API_SSL_CONFIG = "Feature:API:SSLConfig"
    # The driver understands bolt+s, bolt+ssc, neo4j+s, and neo4j+ssc schemes
    # and will configure its ssl options automatically.
    # ...+s: enforce SSL + verify  server's signature with system's trust store
    # ...+ssc: enforce SSL but do not verify the server's signature at all
    API_SSL_SCHEMES = "Feature:API:SSLSchemes"
    # The driver supports single-sign-on (SSO) by providing a bearer auth token
    # API.
    AUTH_BEARER = "Feature:Auth:Bearer"
    # The driver supports custom authentication by providing a dedicated auth
    # token API.
    AUTH_CUSTOM = "Feature:Auth:Custom"
    # The driver supports Kerberos authentication by providing a dedicated auth
    # token API.
    AUTH_KERBEROS = "Feature:Auth:Kerberos"
    # The driver supports Bolt protocol version 3
    BOLT_3_0 = "Feature:Bolt:3.0"
    # The driver supports Bolt protocol version 4.1
    BOLT_4_1 = "Feature:Bolt:4.1"
    # The driver supports Bolt protocol version 4.2
    BOLT_4_2 = "Feature:Bolt:4.2"
    # The driver supports Bolt protocol version 4.3
    BOLT_4_3 = "Feature:Bolt:4.3"
    # The driver supports Bolt protocol version 4.4
    BOLT_4_4 = "Feature:Bolt:4.4"
    # The driver supports Bolt protocol version 5.0
    BOLT_5_0 = "Feature:Bolt:5.0"
    # The driver supports impersonation
    IMPERSONATION = "Feature:Impersonation"
    # The driver supports TLS 1.1 connections.
    # If this flag is missing, TestKit assumes that attempting to establish
    # such a connection fails.
    TLS_1_1 = "Feature:TLS:1.1"
    # The driver supports TLS 1.2 connections.
    # If this flag is missing, TestKit assumes that attempting to establish
    # such a connection fails.
    TLS_1_2 = "Feature:TLS:1.2"
    # The driver supports TLS 1.3 connections.
    # If this flag is missing, TestKit assumes that attempting to establish
    # such a connection fails.
    TLS_1_3 = "Feature:TLS:1.3"
    # The driver configuration connection_acquisition_timeout_ms
    # should be suported.
    # The connection acquisition timeout must account for the whole acquisition
    # execution time, whether a new connection is created, an idle connection
    # is picked up instead or we need to wait until the full pool depletes.
    CONNECTION_ACQUISITION_TIMEOUT = \
        "Feature:Configuration:ConnectionAcquisitionTimeout"

    # === OPTIMIZATIONS ===
    # On receiving Neo.ClientError.Security.AuthorizationExpired, the driver
    # shouldn't reuse any open connections for anything other than finishing
    # a started job. All other connections should be re-established before
    # running the next job with them.
    OPT_AUTHORIZATION_EXPIRED_TREATMENT = "AuthorizationExpiredTreatment"
    # The driver caches connections (e.g., in a pool) and doesn't start a new
    # one (with hand-shake, HELLO, etc.) for each query.
    OPT_CONNECTION_REUSE = "Optimization:ConnectionReuse"
    # The driver first tries to SUCCESSfully BEGIN a transaction before calling
    # the user-defined transaction function. This way, the (potentially costly)
    # transaction function is not started until a working transaction has been
    # established.
    OPT_EAGER_TX_BEGIN = "Optimization:EagerTransactionBegin"
    # Driver doesn't explicitly send message data that is the default value.
    # This conserves bandwidth.
    OPT_IMPLICIT_DEFAULT_ARGUMENTS = "Optimization:ImplicitDefaultArguments"
    # The driver sends no more than the strictly necessary RESET messages.
    OPT_MINIMAL_RESETS = "Optimization:MinimalResets"
    # The driver doesn't wait for a SUCCESS after calling RUN but pipelines a
    # PULL right afterwards and consumes two messages after that. This saves a
    # full round-trip.
    OPT_PULL_PIPELINING = "Optimization:PullPipelining"
    # This feature requires `API_RESULT_LIST`.
    # The driver pulls all records (`PULL -1`) when Result.list() is called.
    # (As opposed to iterating over the Result with the configured fetch size.)
    # Note: If your driver supports this, make sure to document well that this
    #       method ignores the configures fetch size. Your users will
    #       appreciate it <3.
    OPT_RESULT_LIST_FETCH_ALL = "Optimization:ResultListFetchAll"

    # === IMPLEMENTATION DETAILS ===
    # `Driver.IsEncrypted` can also be called on closed drivers.
    DETAIL_CLOSED_DRIVER_IS_ENCRYPTED = "Detail:ClosedDriverIsEncrypted"
    # Security configuration options for encryption and certificates are
    # compared based on their value and might still match the default
    # configuration as long as values match.
    DETAIL_DEFAULT_SECURITY_CONFIG_VALUE_EQUALITY = \
        "Detail:DefaultSecurityConfigValueEquality"

    # === CONFIGURATION HINTS (BOLT 4.3+) ===
    # The driver understands and follow the connection hint
    # connection.recv_timeout_seconds which tells it to close the connection
    # after not receiving an answer on any request for longer than the given
    # time period. On timout, the driver should remove the server from its
    # routing table and assume all other connections to the server are dead
    # as well.
    CONF_HINT_CON_RECV_TIMEOUT = "ConfHint:connection.recv_timeout_seconds"

    # === BACKEND FEATURES FOR TESTING ===
    # The backend understands the GetRoutingTable protocol message and provides
    # a way for TestKit to request the routing table (for testing only, should
    # not be exposed to the user).
    BACKEND_RT_FETCH = "Backend:RTFetch"
    # The backend understands the ForcedRoutingTableUpdate protocol message
    # and provides a way to force a routing table update (for testing only,
    # should not be exposed to the user).
    BACKEND_RT_FORCE_UPDATE = "Backend:RTForceUpdate"

    # Temporary driver feature that will be removed when all official driver
    # backends have implemented the connection acquisition timeout config.
    TMP_CONNECTION_ACQUISITION_TIMEOUT = \
        "Temporary:ConnectionAcquisitionTimeout"
    # Temporary driver feature that will be removed when all official driver
    # backends have implemented path and relationship types
    TMP_CYPHER_PATH_AND_RELATIONSHIP = "Temporary:CypherPathAndRelationship"
    # TODO Update this once the decision has been made.
    # Temporary driver feature. There is a pending decision on whether it
    # should be supported in all drivers or be removed from all of them.
    TMP_DRIVER_FETCH_SIZE = "Temporary:DriverFetchSize"
    # Temporary driver feature that will be removed when all official driver
    # backends have implemented the max connection pool size config.
    TMP_DRIVER_MAX_CONNECTION_POOL_SIZE = \
        "Temporary:DriverMaxConnectionPoolSize"
    # Temporary driver feature that will be removed when all official driver
    # backends have implemented it.
    TMP_DRIVER_MAX_TX_RETRY_TIME = "Temporary:DriverMaxTxRetryTime"
    # Temporary driver feature that will be removed when all official driver
    # implemented failing fast and surfacing on certain error codes during
    # discovery (initial fetching of a RT).
    TMP_FAST_FAILING_DISCOVERY = "Temporary:FastFailingDiscovery"
    # Temporary driver feature that will be removed when all official driver
    # backends have implemented all summary response fields.
    TMP_FULL_SUMMARY = "Temporary:FullSummary"
    # Temporary driver feature that will be removed when all official driver
    # backends have implemented the GetConnectionPoolMetrics request.
    TMP_GET_CONNECTION_POOL_METRICS = \
        "Temporary:GetConnectionPoolMetrics"
    # Temporary driver feature that will be removed when all official drivers
    # have been unified in their behaviour of when they return a Result object.
    # We aim for drivers to not providing a Result until the server replied
    # with SUCCESS so that the result keys are already known and attached to
    # the Result object without further waiting or communication with the
    # server.
    TMP_RESULT_KEYS = "Temporary:ResultKeys"
    # Temporary driver feature that will be removed when all official driver
    # backends have implemented the TransactionClose request
    TMP_TRANSACTION_CLOSE = "Temporary:TransactionClose"
