
"""
All requests will be sent to backend as:
    {
        name: <class name>,
        data: {
            <all instance variables>
        }
    }
"""

class NewDriver:
    def __init__(self, uri, authToken):
        self.uri = uri
        self.authorizationToken = authToken


class NewSession:
    def __init__(self, driverId, accessMode, bookmarks):
        self.driverId = driverId
        self.accessMode = accessMode
        self.bookmarks = bookmarks


"""
Response should be Result model or raised Error model
"""
class SessionRun:
    def __init__(self, sessionId, cypher, params):
        self.sessionId = sessionId
        self.cypher = cypher
        self.params = params


"""
Response should be Record model, NullRecord to indicate last record or raised Error model if record
couldn't be retrieved.
"""
class ResultNext:
    def __init__(self, resultId):
        self.resultId = resultId


class AuthorizationToken:
    def __init__(self, scheme="none", principal="", credentials="", realm="", ticket=""):
        self.scheme=scheme
        self.principal=principal
        self.credentials=credentials
        self.realm=realm
        self.ticket=ticket

