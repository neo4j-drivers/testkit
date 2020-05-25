
"""
All requests will be sent to backend as:
    {
        name: <class name>,
        data: {
            <all instance variables>
        }
    }
"""

class NewDriverRequest:
    def __init__(self, uri, authToken):
        self.uri = uri
        self.authorizationToken = authToken


class NewSessionRequest:
    def __init__(self, driverId, accessMode, bookmarks):
        self.driverId = driverId
        self.accessMode = accessMode
        self.bookmarks = bookmarks


class SessionRunRequest:
    def __init__(self, sessionId, cypher):
        self.sessionId = sessionId
        self.cypher = cypher


class AuthorizationToken:
    def __init__(self, scheme="none", principal="", credentials="", realm="", ticket=""):
        self.scheme=scheme
        self.principal=principal
        self.credentials=credentials
        self.realm=realm
        self.ticket=ticket

