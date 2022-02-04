"""
Responses are sent from the backend to the frontend testing framework.

All responses are sent from backend as:
    {
        name: <class name>
        data: {
            <all instance variables in python class>
        }
    }

For example response to NewDriver request should be sent from backend as:

    {
        name: "Driver"
        data: {
            "id": <backend driver id>
        }
    }
"""


class RunTest:
    """Response to StartTest indicating that the test can be started."""


class FeatureList:
    """
    Response to GetFeatures.

    An indication of the features supported by the driver.
    """

    def __init__(self, features=None):
        if features is None:
            features = []
        self.features = features


class SkipTest:
    """Response to StartTest indicating that the test should be skipped."""

    def __init__(self, reason):
        self.reason = reason


class Driver:
    """Represents a driver instance on the backend."""

    def __init__(self, id):
        # Id of Driver instance on backend
        self.id = id


class ResolverResolutionRequired:
    """
    Represents a need for new address resolution.

    This means that the backend is expecting the frontend
    to call the resolver function and submit a new request
    with the results of it.
    """

    def __init__(self, id, address):
        # Id of callback request
        self.id = id
        self.address = address


class DomainNameResolutionRequired:
    """
    Represents a need for new domain name resolution.

    This means that the backend is expecting the frontend
    to call the domain name resolver function and submit a new request
    with the results of it.
    """

    def __init__(self, id, name):
        # Id of callback request
        self.id = id
        self.name = name


class MultiDBSupport:
    """
    Whether the driver is connection to a sever with supports multi-db-support.

    Specifies whether the server or cluster the driver connects to supports
    multi-databases. It is sent in response to the CheckMultiDBSupport request.
    """

    def __init__(self, id, available):
        self.id = id
        self.available = available


class DriverIsEncrypted:
    """Whether the driver is configured to use encrypted connections."""

    def __init__(self, encrypted):
        self.encrypted = encrypted


class Session:
    """Represents a session instance on the backend."""

    def __init__(self, id):
        # Id of Session instance on backend
        self.id = id


class Transaction:
    """Represents a session instance on the backend."""

    def __init__(self, id):
        # Id of Transaction instance on backend
        self.id = id


class Result:
    """Represents a result instance on the backend."""

    def __init__(self, id, keys=None):
        # Id of Result instance on backend
        self.id = id
        # Keys is a list of strings: ['field1', 'field2']
        self.keys = keys


class Record:
    """
    A record received from a query `Result`.

    A record is not represented on the backend after it has been retrieved,
    the full instance is sent from the backend to the frontend. The backend
    should not keep it in memory.

    values is a list of field values where each value is a CypherX
    instance Backend sends Record with values =
        [CypherNull(), CypherInt(value=1)] as
        {
            name: "Record",
            data: {
                values: [
                    { name: "CypherNull", data: {}},
                    { name: "CypherInt", data: { value: 1 }},
                ]
            }
        }
    """

    def __init__(self, values=None):
        self.values = values

    def __eq__(self, other):
        if not isinstance(other, Record):
            return False

        return other.values == self.values

    def __str__(self):
        v = []
        for x in self.values:
            v.append(str(x))
        return "Record, values {}".format(v)

    def __repr__(self):
        return self.__str__()


class NullRecord:
    """Represents end of records when iterating through records with Next."""

    def __init__(self):
        pass

    def __str__(self):
        return "NullRecord"

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return isinstance(other, NullRecord)


class RecordList:
    """Represents list of records returned from ResultList request."""

    def __init__(self, records=None):
        record_list = []
        if records is not None:
            for record in records:
                record_list.append(Record(values=record["values"]))

        self.records = record_list


class Summary:
    """Represents summary returned from a ResultConsume request."""

    def __init__(self, **data):
        # TODO: remove block when all drivers support the address field
        # ---------------------------------------------------------------------
        class AnyAddress:
            """Fake address that will match anything."""

            def __eq__(self, _):
                return True

        from tests.shared import get_driver_name
        if get_driver_name() in ["javascript", "go", "dotnet", "ruby"]:
            if "address" in data["serverInfo"]:
                import warnings
                warnings.warn(
                    "Backend supports address field in Summary.serverInfo. "
                    "Remove the backwards compatibility check!"
                )
            else:
                data["serverInfo"]["address"] = AnyAddress()
        # ---------------------------------------------------------------------
        # TODO: remove block when all drivers support the fields
        # ---------------------------------------------------------------------
        from tests.shared import get_driver_name
        if get_driver_name() in ["javascript"]:
            # already sends counters but the wrong format and not all fields
            if "_stats" in data["counters"]:
                del data["counters"]
            else:
                import warnings
                warnings.warn(
                    "Backend supports well-formatted counter. "
                    "Remove the backwards compatibility check!"
                )
        if get_driver_name() in ["javascript", "go", "dotnet"]:
            if "counters" in data:
                import warnings
                warnings.warn(
                    "Backend supports counters field in Summary. "
                    "Remove the backwards compatibility check!"
                )
            else:
                data["counters"] = {
                    "constraintsAdded": None,
                    "constraintsRemoved": None,
                    "containsSystemUpdates": None,
                    "containsUpdates": None,
                    "indexesAdded": None,
                    "indexesRemoved": None,
                    "labelsAdded": None,
                    "labelsRemoved": None,
                    "nodesCreated": None,
                    "nodesDeleted": None,
                    "propertiesSet": None,
                    "relationshipsCreated": None,
                    "relationshipsDeleted": None,
                    "systemUpdates": None
                }
            if "query" in data:
                import warnings
                warnings.warn(
                    "Backend supports query field in Summary. "
                    "Remove the backwards compatibility check!"
                )
            else:
                data["query"] = {
                    "text": None,
                    "parameters": None
                }
            for field in (
                "database", "notifications", "plan", "profile",
                "queryType", "resultAvailableAfter", "resultConsumedAfter"
            ):
                if field in data:
                    import warnings
                    warnings.warn(
                        "Backend supports %s field in Summary. "
                        "Remove the backwards compatibility check!" % field
                    )
                else:
                    data[field] = None
        # ---------------------------------------------------------------------
        self.counters = SummaryCounters(**data["counters"])
        self.database = data["database"]
        self.notifications = data["notifications"]
        self.plan = data["plan"]
        self.profile = data["profile"]
        self.query = SummaryQuery(**data["query"])
        self.query_type = data["queryType"]
        self.result_available_after = data["resultAvailableAfter"]
        self.result_consumed_after = data["resultConsumedAfter"]
        self.server_info = ServerInfo(**data["serverInfo"])


class ServerInfo:
    """Represents server info.

    It can be part of a Summary response or a stand-alone response
    """

    def __init__(self, address, agent, protocolVersion):
        self.address = address
        self.agent = agent
        self.protocol_version = protocolVersion


class SummaryCounters:
    """Represents the counters info included in the Summary response."""

    def __init__(self, constraintsAdded, constraintsRemoved,
                 containsSystemUpdates, containsUpdates, indexesAdded,
                 indexesRemoved, labelsAdded, labelsRemoved, nodesCreated,
                 nodesDeleted, propertiesSet, relationshipsCreated,
                 relationshipsDeleted, systemUpdates):
        self.constraints_added = constraintsAdded
        self.constraints_removed = constraintsRemoved
        self.contains_system_updates = containsSystemUpdates
        self.contains_updates = containsUpdates
        self.indexes_added = indexesAdded
        self.indexes_removed = indexesRemoved
        self.labels_added = labelsAdded
        self.labels_removed = labelsRemoved
        self.nodes_created = nodesCreated
        self.nodes_deleted = nodesDeleted
        self.properties_set = propertiesSet
        self.relationships_created = relationshipsCreated
        self.relationships_deleted = relationshipsDeleted
        self.system_updates = systemUpdates


class SummaryQuery:
    """Represents the query info that is included in the Summary response."""

    def __init__(self, text, parameters):
        self.text = text
        self.parameters = parameters


class Bookmarks:
    """
    Represents an array of bookmarks.

    `bookmarks` is an array of bookmarks (str).
    """

    def __init__(self, bookmarks):
        self.bookmarks = bookmarks


class RetryableTry:
    """
    Represents a retryable transaction on the backend.

    The backend has created a transaction and will enter retryable function on
    the backend, all further requests will be applied through that retryable
    function.
    """

    def __init__(self, id):
        # Id of backend transaction
        self.id = id


class RetryableDone:
    """
    Transaction successfully committed.

    Sent from backend when a retryable transaction has been successfully
    committed.
    """

    def __init__(self):
        pass


class RoutingTable:
    def __init__(self, database, ttl, routers, readers, writers):
        """
        Sent from the backend in response to GetRoutingTable.

        routers, readers, and writers are all supposed to be lists of addresses
        as strings.
        """
        self.database = database
        self.ttl = ttl
        self.routers = routers
        self.readers = readers
        self.writers = writers


class ConnectionPoolMetrics:
    def __init__(self, inUse, idle):
        """Sent from the backend in response to GetConnectionPoolMetrics."""
        self.in_use = inUse
        self.idle = idle


class BaseError(Exception):
    """
    Base class for all types of errors, should not be sent from backend.

    All classes inheriting from this will be thrown as exceptions upon
    retrieval from backend.
    """


class DriverError(BaseError):
    """
    Base class for all driver errors that are NOT a backend specific error.

    The backend should keep it's error representation in memory and send the
    id of that error in the response. By doing this there is no need to
    serialize/deserialize errors or exceptions.

    The retry logic can be implemented by just referring to the error when
    such occures and let the backend hide it's internal types if it chooses so.

    Over time there will be more specific driver errors if/when the generic
    test framework needs to check detailed error handling.
    """

    def __init__(self, id=None, errorType="", msg="", code=""):
        self.id = id
        self.errorType = errorType
        self.msg = msg
        self.code = code

    def __str__(self):
        return "DriverError : " + self.errorType + " : " + self.msg

    def __repr__(self):
        return self.__str__()


class FrontendError(BaseError):
    """
    Error originating from client code.

    As in cases where the driver invokes client code and that code
    returns/raises an error.
    """

    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return "FrontendError : " + self.msg


# For backward compatibility
ClientError = FrontendError


class BackendError(BaseError):
    """
    Internal backend error.

    Sent by backend when there is an internal error in the backend, not
    the driver.

    The backend can choose to send this to simplify debugging of implementation
    of the backend and to make it easier to clearly see the errors in CI logs
    as the error message will be output.
    """

    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return "BackendError : " + self.msg
