import nutkit.protocol as protocol

from .session import Session


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
