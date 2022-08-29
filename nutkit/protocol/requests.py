"""
Requests are sent to the backend from this test framework.

Each request should have one and only one matching response, except for
errors/exceptions.

All requests will be sent to backend as:
    {
        name: <class name>,
        data: {
            <all instance variables>
        }
    }

Backend responds with a suitable response as defined in responses.py or an
error as defined
in errors.py. See the requests for information on which response they expect.
"""


class StartTest:
    """
    Request the backend to confirm to run a specific test.

    The backend should respond with RunTest unless it wants the test to be
    skipped, in that case it must respond with SkipTest.

    The backend might also respond with RunSubTests. In this case, TestKit will
     - run the test, if it does not have subtests
     - ask for each subtest whether it should be run, if it has subtests
       StartSubTest will be sent to the backend, for each subtest
    """

    def __init__(self, test_name):
        self.testName = test_name


class StartSubTest:
    """
    Request the backend to confirm to run a specific subtest.

    See StartTest for when TestKit might emmit this message.

    The backend should respond with RunTest unless it wants the subtest to be
    skipped, in that case it must respond with SkipTest.
    """

    def __init__(self, test_name, subtest_arguments: dict):
        self.testName = test_name
        self.subtestArguments = subtest_arguments


class GetFeatures:
    """
    Request the backend to the list of features supported by the driver.

    The backend should respond with FeatureList.
    """


class NewDriver:
    """
    Request to create a new driver instance on the backend.

    Backend should respond with a Driver response or an Error response.
    """

    def __init__(
        self, uri, authToken, userAgent=None, resolverRegistered=False,
        domainNameResolverRegistered=False, connectionTimeoutMs=None,
        fetchSize=None, maxTxRetryTimeMs=None, encrypted=None,
        trustedCertificates=None, liveness_check_timeout_ms=None,
        max_connection_pool_size=None,
        connection_acquisition_timeout_ms=None
    ):
        # Neo4j URI to connect to
        self.uri = uri
        # Authorization token used by driver when connecting to Neo4j
        self.authorizationToken = authToken
        # Optional custom user agent string
        self.userAgent = userAgent
        self.resolverRegistered = resolverRegistered
        self.domainNameResolverRegistered = domainNameResolverRegistered
        self.connectionTimeoutMs = connectionTimeoutMs
        self.fetchSize = fetchSize
        self.maxTxRetryTimeMs = maxTxRetryTimeMs
        self.livenessCheckTimeoutMs = liveness_check_timeout_ms
        self.maxConnectionPoolSize = max_connection_pool_size
        self.connectionAcquisitionTimeoutMs = connection_acquisition_timeout_ms
        # (bool) whether to enable or disable encryption
        # field missing in message: use driver default (should be False)
        if encrypted is not None:
            self.encrypted = encrypted
        # None: trust system CAs
        # [] (empty list): trust any certificate
        # ["path", ...] (list of strings): custom CA certificates to trust
        # field missing in message: use driver default (should be system CAs)
        if trustedCertificates is not None:
            if trustedCertificates == "None":
                self.trustedCertificates = None
            else:
                self.trustedCertificates = trustedCertificates


class AuthorizationToken:
    """
    Not a request but used in NewDriver request.

    The fields depend on the chosen scheme:
    scheme == "basic"
        - principal (str)
        - credentials (str)
        - realm (str, optional)
    scheme == "kerberos"
        - credentials (str)
    scheme == "bearer"
        - credentials (str)
    further schemes should be handled with a multi-purpose auth API
    (custom auth)
        - principal (str, optional)
        - credentials (str, optional)
        - realm (str, optional)
        - parameters (dict[str, Any], optional)
    """

    def __init__(self, scheme, **kwargs):
        self.scheme = scheme
        for attr, value in kwargs.items():
            setattr(self, attr, value)


class VerifyConnectivity:
    """
    Request to verify connectivity on the driver.

    instance corresponding to the specified driver id.
    Backend should respond with a Driver response or an Error response.
    """

    def __init__(self, driver_id):
        self.driverId = driver_id


class GetServerInfo:
    """
    Request to verify connectivity on the driver and get ServerInfo.

    instance corresponding to the specified driver id.
    Backend should respond with a ServerInfo response or an Error response.
    """

    def __init__(self, driver_id):
        self.driverId = driver_id


class CheckMultiDBSupport:
    """
    Perform a check if the connected sever supports multi-db-support.

    Request to check if the server or cluster the driver connects to supports
    multi-databases.

    Backend should respond with a MultiDBSupport response.
    """

    def __init__(self, driverId):
        self.driverId = driverId


class CheckDriverIsEncrypted:
    """
    Perform a check if the driver is configured to enforce encryption.

    Backend should respond with a DriverIsEncrypted response.
    """

    def __init__(self, driverId):
        self.driverId = driverId


class ResolverResolutionCompleted:
    """
    Results of a custom address resolution.

    Pushes the results of the resolver function resolution back to the backend.
    This must only be sent immediately after the backend requests a new address
    resolution by replying with the ResolverResolutionRequired response.
    """

    def __init__(self, requestId, addresses):
        self.requestId = requestId
        self.addresses = addresses


class BookmarksSupplierCompleted:
    """
    Results of a bookmark manager's bookmark supplier call.

    Pushes bookmarks for a given database to the Bookmark Manager.
    """

    def __init__(self, request_id, bookmarks):
        self.requestId = request_id
        self.bookmarks = bookmarks


class BookmarksConsumerCompleted:
    """
    Results of a bookmark manager's bookmarks consumer call.

    Signal the method call has finished
    """

    def __init__(self, request_id):
        self.requestId = request_id


class NewBookmarkManager:
    """Instantiates a bookmark manager by calling the default factory.

    Backend should respond with a BookmarkManager response.
    """

    def __init__(self, initial_bookmarks,
                 bookmarks_supplier_registered, bookmarks_consumer_registered):
        self.initialBookmarks = initial_bookmarks
        self.bookmarksSupplierRegistered = bookmarks_supplier_registered
        self.bookmarksConsumerRegistered = bookmarks_consumer_registered


class BookmarkManagerClose:
    """Destroy the bookmark manager in the backend and free the resources.

    Backend should respond with a BookmarkManager response.
    """

    def __init__(self, id):
        self.id = id


class DomainNameResolutionCompleted:
    """
    Results of a DNS resolution.

    Pushes the results of the domain name resolver function resolution back to
    the backend. This must only be sent immediately after the backend requests
    a new address resolution by replying with the DomainNameResolutionRequired
    response.
    """

    def __init__(self, requestId, addresses):
        self.requestId = requestId
        self.addresses = addresses


class DriverClose:
    """
    Request to close the driver instance on the backend.

    Backend should respond with a Driver response representing the closed
    driver or an error response.
    """

    def __init__(self, driverId):
        # Id of driver to close on backend
        self.driverId = driverId


class NewSession:
    """
    Request to create a new session.

    Create the sessionon the backend on the driver instance corresponding to
    the specified driver id.

    Backend should respond with a Session response or an Error response.
    """

    def __init__(self, driverId, accessMode, bookmarks=None,
                 database=None, fetchSize=None, impersonatedUser=None,
                 bookmark_manager=None):
        # Id of driver on backend that session should be created on
        self.driverId = driverId
        # Session accessmode: 'r' for read access and 'w' for write access.
        self.accessMode = accessMode
        # Array of boookmarks in the form of list of strings.
        self.bookmarks = bookmarks
        self.database = database
        self.fetchSize = fetchSize
        self.impersonatedUser = impersonatedUser
        if bookmark_manager is not None:
            self.bookmarkManagerId = bookmark_manager.id


class SessionClose:
    """
    Request to close the session instance on the backend.

    Backend should respond with a Session response representing the closed
    session or an error response.
    """

    def __init__(self, sessionId):
        self.sessionId = sessionId


class SessionRun:
    """
    Request to run a query on a specified session.

    Backend should respond with a Result response or an Error response.
    """

    def __init__(self, sessionId, cypher, params, txMeta=None, **kwargs):
        self.sessionId = sessionId
        self.cypher = cypher
        self.params = params
        self.txMeta = txMeta
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]


class SessionReadTransaction:
    """
    Request to run a retryable read transaction.

    Backend should respond with a RetryableTry response or an Error response.
    """

    def __init__(self, sessionId, txMeta=None, **kwargs):
        self.sessionId = sessionId
        self.txMeta = txMeta
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]


class SessionWriteTransaction:
    """
    Request to run a retryable write transaction.

    Backend should respond with a RetryableTry response or an Error response.
    """

    def __init__(self, sessionId, txMeta=None, **kwargs):
        self.sessionId = sessionId
        self.txMeta = txMeta
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]


class SessionBeginTransaction:
    """
    Request to Begin a transaction.

    Backend should respond with a Transaction response or an Error response.
    """

    def __init__(self, sessionId, txMeta=None, **kwargs):
        self.sessionId = sessionId
        self.txMeta = txMeta
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]


class SessionLastBookmarks:
    """
    Request for last bookmarks on a session.

    Backend should respond with a Bookmarks response or an Error response.
    If there are no bookmarks in the session, the backend should return a
    Bookmark with empty array.
    """

    def __init__(self, sessionId):
        self.sessionId = sessionId


class TransactionRun:
    """
    Request to run a query in a specified transaction.

    Backend should respond with a Result response or an Error response.
    """

    def __init__(self, txId, cypher, params):
        self.txId = txId
        self.cypher = cypher
        self.params = params


class TransactionCommit:
    """
    Request to run a query in a specified transaction.

    Backend should respond with a Result response or an Error response.
    """

    def __init__(self, txId):
        self.txId = txId


class TransactionRollback:
    """
    Request to run a query in a specified transaction.

    Backend should respond with a Result response or an Error response.
    """

    def __init__(self, txId):
        self.txId = txId


class TransactionClose:
    """
    Request to close the transaction instance on the backend.

    Backend should respond with a transaction response representing the closed
    transaction or an error response.
    """

    def __init__(self, txId):
        self.txId = txId


class ResultNext:
    """
    Request to retrieve the next record on a result living on the backend.

    Backend should respond with a Record if there is a record, an Error if an
    error occurred while retrieving next record or NullRecord if there were no
    error and no record.
    """

    def __init__(self, resultId):
        self.resultId = resultId


class ResultSingle:
    """
    Request to expect and return exactly one record in the result stream.

    Backend should respond with a Record if exactly one record was found.
    If more or fewer records are left in the result stream, or if any other
    error occurs while retrieving the records, an Error response should be
    returned.
    """

    def __init__(self, resultId):
        self.resultId = resultId


class ResultSingleOptional:
    """
    Request to expect and return exactly one record in the result stream.

    Furthermore, the method is supposed to fully exhaust the result stream.

    The backend should respond with a RecordOptional or, if any error occurs
    while retrieving the records, an Error response should be returned.
    """

    def __init__(self, resultId):
        self.resultId = resultId


class ResultPeek:
    """
    Request to return the next result in the Stream without consuming it.

    I.e., without advancing the position in the stream.
    Backend should respond with a Record if there is a record, an Error if an
    error occurred while retrieving next record or NullRecord if there were no
    error and no record.
    """

    def __init__(self, resultId):
        self.resultId = resultId


class ResultConsume:
    """
    Request to close the result and to discard all remaining records.

    Backend should respond with Summary or an Error if an error occured.
    """

    def __init__(self, resultId):
        self.resultId = resultId


class ResultList:
    """
    Request to retrieve the entire result stream of records.

    Backend should respond with RecordList or an Error if an error occurred.
    """

    def __init__(self, resultId):
        self.resultId = resultId


class RetryablePositive:
    """
    Request to commit the retryable transaction.

    Backend responds with either a RetryableTry response (if it failed to
    commit and wants to retry) or a RetryableDone response if committed
    succesfully or an Error response if the backend failed in an
    unretriable way.
    """

    def __init__(self, sessionId):
        self.sessionId = sessionId


class RetryableNegative:
    """
    Request to rollback (or more correct NOT commit) the retryable transaction.

    Backend will abort retrying and respond with an Error response.
    If the backend sends an error that is generated by the test code (or in
    comparison with real driver usage, client code) the errorId should be ""
    and the backend will respond with a ClientError, if error id is not empty
    the backend will resend that error.
    """

    def __init__(self, sessionId, errorId=""):
        self.sessionId = sessionId
        self.errorId = errorId


class ForcedRoutingTableUpdate:
    """
    Request to update the routing table for the given database.

    This API shouldn't be part of the driver's public API, but is used for
    testing purposes only.
    The Backend should respond with a Driver response if the update was
    successful or an Error if not.
    """

    def __init__(self, driverId, database=None, bookmarks=None):
        self.driverId = driverId
        self.database = database
        # Array of boookmarks in form of a list of strings or None
        self.bookmarks = bookmarks


class GetRoutingTable:
    """
    Request the backend to extract the routing table for the given database.

    Default database if None is specified.
    This API shouldn't be part of the drivers's public API, but is used for
    testing purposes only.
    The Backend should respond with a RoutingTable response.
    """

    def __init__(self, driverId, database=None):
        self.driverId = driverId
        self.database = database


class GetConnectionPoolMetrics:
    """
    Request the backend to return connection pool metrics for given address.

    Note that depending on implementation driver might keep connection pools
    indexed by domain names or resolved ip addresses.
    The Backend should respond with a ConnectionPoolMetrics response.
    """

    def __init__(self, driverId, address):
        self.driverId = driverId
        self.address = address


class CypherTypeField:
    """
    Request to retrieve the next record on a result then extract field.

    Backend should respond with a Field if there is field on a record,
    an Error if error occurred while retrieving next record or reading field.
    """

    def __init__(self, result_id, record_key, type_name, field_id):
        self.resultId = result_id
        self.recordKey = record_key
        self.type = type_name
        self.field = field_id
