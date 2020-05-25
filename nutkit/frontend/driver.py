import nutkit.protocol as protocol


class Driver:
    def __init__(self, backend, uri, authToken):
        self._backend = backend
        req = protocol.NewDriver(uri, authToken)
        res = backend.sendAndReceive(req)
        if not isinstance(res, protocol.Driver):
            raise "Should be driver"
        self._driver = res

    def session(self, accessMode, bookmarks):
        req = protocol.NewSession(self._driver.id, accessMode, bookmarks)
        res = self._backend.sendAndReceive(req)
        if not isinstance(res, protocol.Session):
            raise "Should be session"
        return Session(self._backend, res)


class Session:
    def __init__(self, backend, session):
        self._backend = backend
        self._session = session

    def run(self, cypher):
        req = protocol.SessionRun(self._session.id, cypher)
        res = self._backend.sendAndReceive(req)
        if not isinstance(res, protocol.Result):
            raise "Should be result"
        return Result(self._backend, res)


class Result:
    def __init__(self, backend, result):
        self._backend = backend
        self._result = result

    def next(self):
        req = protocol.ResultNext(self._result.id)
        return self._backend.sendAndReceive(req)

