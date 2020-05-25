
import nutkit.protocol as protocol



class Driver:
    def __init__(self, backend, uri, authToken):
        self._backend = backend
        req = protocol.NewDriverRequest(uri, authToken)
        self._driver = backend.sendAndReceive(req)

    def session(self, accessMode, bookmarks):
        req = protocol.NewSessionRequest(self._driver.id, accessMode, bookmarks)
        res = self._backend.sendAndReceive(req)
        return Session(self._backend, res)


class Session:
    def __init__(self, backend, session):
        self._backend = backend
        self._session = session

    def run(self, cypher):
        req = protocol.SessionRunRequest(self._session.id, cypher)
        res = self._backend.sendAndReceive(req)
        return Result(self._backend, res)

class Result:
    def __init__(self, backend, result):
        self._backend = backend
