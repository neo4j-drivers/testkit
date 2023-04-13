"""Enumerate all the capabilities in the drivers."""
from enum import Enum


class Feature(Enum):
    # === FUNCTIONAL FEATURES ===
    # The driver offers a configuration option to limit time it spends at most,
    # trying to acquire a connection from the pool.
    # The connection acquisition timeout must account for the whole acquisition
    # execution time, whether a new connection is created, an idle connection
    # is picked up instead or we need to wait until the full pool depletes.
    API_CONNECTION_ACQUISITION_TIMEOUT = \
        "Feature:API:ConnectionAcquisitionTimeout"
    # The driver offers a method for driver objects to report if they were
    # configured with a or without encryption.
    API_DRIVER_IS_ENCRYPTED = "Feature:API:Driver.IsEncrypted"
    # The driver supports connection liveness check.
    API_LIVENESS_CHECK = "Feature:API:Liveness.Check"
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
    # The driver offers a configuration option to limit time it spends at most,
    # trying to acquire a usable read/write connection for any session.
    # The connection acquisition timeout must account for the whole acquisition
    # execution time, whether a new connection is created, an idle connection
    # is picked up instead, we need to wait until the full pool depletes, or
    # a routing table must be fetched.
    API_SESSION_CONNECTION_TIMEOUT = "Feature:API:SessionConnectionTimeout"
    # The driver implements explicit configuration options for SSL.
    #  - enable / disable SSL
    #  - verify signature against system store / custom cert / not at all
    API_SSL_CONFIG = "Feature:API:SSLConfig"
    # The driver understands bolt+s, bolt+ssc, neo4j+s, and neo4j+ssc schemes
    # and will configure its ssl options automatically.
    # ...+s: enforce SSL + verify  server's signature with system's trust store
    # ...+ssc: enforce SSL but do not verify the server's signature at all
    API_SSL_SCHEMES = "Feature:API:SSLSchemes"
    # The driver supports sending and receiving temporal data types.
    API_TYPE_TEMPORAL = "Feature:API:Type.Temporal"
    # The driver offers a configuration option to limit time it spends at most,
    # trying to update the routing table whenever needed.
    API_UPDATE_ROUTING_TABLE_TIMEOUT = "Feature:API:UpdateRoutingTableTimeout"
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
    # The driver supports Bolt protocol version 4.0
    BOLT_4_0 = "Feature:Bolt:4.0"
    # The driver supports Bolt protocol version 4.1
    BOLT_4_1 = "Feature:Bolt:4.1"
    # The driver supports Bolt protocol version 4.2
    BOLT_4_2 = "Feature:Bolt:4.2"
    # The driver supports Bolt protocol version 4.3
    BOLT_4_3 = "Feature:Bolt:4.3"
    # The driver supports Bolt protocol version 4.4
    BOLT_4_4 = "Feature:Bolt:4.4"
    # The driver supports patching DateTimes to use UTC for Bolt 4.3 and 4.4
    BOLT_PATCH_UTC = "Feature:Bolt:Patch:UTC"
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
    # The Result object is not broken after receiving a record that is valid
    # on packstream level but failed hydration (e.g., it contained a structure
    # with unknown tag or a zoned date time with unknown zone id). But instead,
    # it allows the user to catch the exception and request the next record in
    # the stream. If the flag is not present, TestKit will assert the opposite:
    # The Result object is expected to be broken afterwards, i.e., either throw
    # or return NullRecord on next call to next().
    DETAIL_RESULT_STREAM_WORKS_AFTER_BROKEN_RECORD = \
        "Detail:ResultStreamWorksAfterBrokenRecord"

    # === CONFIGURATION HINTS (BOLT 4.3+) ===
    # The driver understands and follow the connection hint
    # connection.recv_timeout_seconds which tells it to close the connection
    # after not receiving an answer on any request for longer than the given
    # time period. On timout, the driver should remove the server from its
    # routing table and assume all other connections to the server are dead
    # as well.
    CONF_HINT_CON_RECV_TIMEOUT = "ConfHint:connection.recv_timeout_seconds"
