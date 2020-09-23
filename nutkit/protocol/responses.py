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


class Driver:
    """ Represents a driver instance on the backend
    """
    def __init__(self, id):
        # Id of Driver instance on backend
        self.id = id


class Session:
    """ Represents a session instance on the backend
    """
    def __init__(self, id):
        # Id of Session instance on backend
        self.id = id


class Transaction:
    """ Represents a session instance on the backend
    """
    def __init__(self, id):
        # Id of Transaction instance on backend
        self.id = id


class Result:
    """ Represents a result instance on the backend
    """
    def __init__(self, id, keys=None):
        # Id of Result instance on backend
        self.id = id
        # Keys is a list of strings: ['field1', 'field2']
        self.keys = keys


class Record:
    """ A record is not represented on the backend after it has been retrieved, the
    full instance is sent from the backend to the frontend. The backend should not keep
    it in memory.
    """
    def __init__(self, values=None):
        """ values is a list of field values where each value is a CypherX instance
        Backend sends Record with values = [CypherNull(), CypherInt(value=1)] as
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
    """ Represents end of records when iterating through records with Next.
    """
    def __init__(self):
        pass

    def __str__(self):
        return "NullRecord"

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return isinstance(other, NullRecord)


class Bookmarks:
    """ Represents an array of bookmarks.
    """
    def __init__(self, bookmarks):
        self.bookmarks = bookmarks


class RetryableTry:
    """ Represents a retryable transaction on the backend. The backend has created a transaction
    and will enter retryable function on the backend, all further requests will be applied through
    that retryable function.
    """
    def __init__(self, id):
        # Id of backend transaction
        self.id = id


class RetryableDone:
    """ Sent from backend when a retryable transaction has been successfully committed.
    """
    def __init__(self):
        pass


class BaseError(Exception):
    """ Base class for all types of errors, should not be sent from backend

    All classes inheriting from this will be thrown as exceptions upon retrieval from backend.
    """
    pass


class DriverError(BaseError):
    """ Base class for all kind of driver errors that is NOT a backend specific error

    The backend should keep it's error representation in memory and send the id of that error
    in the response. By doing this there is no need to serialize/deserialize errors or exceptions.

    The retry logic can be implemented by just referring to the error when such occures and let the
    backend hide it's internal types if it chooses so.

    Over time there will be more specific driver errors if/when the generic test framework
    needs to check detailed error handling.
    """
    def __init__(self, id=None, errorType="", msg=""):
        self.id = id
        self.errorType = errorType
        self.msg = msg

    def __str__(self):
        return "DriverError : " + self.errorType + " : " + self.msg


class FrontendError(BaseError):
    """ Represents an error originating from client code.

    As in cases where the driver invokes client code and that code returns/raises an error.
    """
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return "FrontendError : " + self.msg

# For backward compatibility
ClientError = FrontendError


class BackendError(BaseError):
    """ Sent by backend when there is an internal error in the backend, not the driver.

    The backend can choose to send this to simplify debugging of implementation of the backend
    and to make it easier to clearly see the errors in CI logs as the error message will be
    output.
    """
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return "BackendError : " + self.msg

