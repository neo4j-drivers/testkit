import nutkit.protocol as protocol

from .session import Session


class Driver:
    """
    At present, the resolverFn is only supported in tx functions.
    """
    def __init__(self, backend, uri, authToken, userAgent=None, resolverFn=None):
        self._backend = backend
        self._resolverFn = resolverFn
        req = protocol.NewDriver(uri, authToken, userAgent=userAgent, resolverRegistered=resolverFn is not None)
        res = backend.sendAndReceive(req)
        if not isinstance(res, protocol.Driver):
            raise Exception("Should be driver")
        self._driver = res

    def verifyConnectivity(self):
        req = protocol.VerifyConnectivity(self._driver.id)
        res = self._backend.sendAndReceive(req)
        if not isinstance(res, protocol.Driver):
            raise Exception("Should be driver")

    def close(self):
        req = protocol.DriverClose(self._driver.id)
        res = self._backend.sendAndReceive(req)
        if not isinstance(res, protocol.Driver):
            raise Exception("Should be driver")

    def session(self, accessMode, bookmarks=None, database=None,
                fetchSize=None):
        req = protocol.NewSession(self._driver.id, accessMode,
                                  bookmarks=bookmarks, database=database,
                                  fetchSize=fetchSize)
        res = self._backend.sendAndReceive(req)
        if not isinstance(res, protocol.Session):
            raise Exception("Should be session")
        return Session(self, self._backend, res)

    def resolve(self, address):
        return self._resolverFn(address)
