from .session import Session
from .. import protocol


class Driver:
    def __init__(self, backend, uri, authToken, userAgent=None, resolverFn=None, domainNameResolverFn=None,
                 connectionTimeoutMs=None):
        self._backend = backend
        self._resolverFn = resolverFn
        self._domainNameResolverFn = domainNameResolverFn
        req = protocol.NewDriver(uri, authToken, userAgent=userAgent, resolverRegistered=resolverFn is not None,
                                 domainNameResolverRegistered=domainNameResolverFn is not None,
                                 connectionTimeoutMs=connectionTimeoutMs)
        res = backend.sendAndReceive(req)
        if not isinstance(res, protocol.Driver):
            raise Exception("Should be Driver but was %s" % res)
        self._driver = res

    def verifyConnectivity(self):
        req = protocol.VerifyConnectivity(self._driver.id)
        self._backend.send(req)
        while True:
            res = self._backend.receive()
            if isinstance(res, protocol.DomainNameResolutionRequired):
                addresses = self.resolveDomainName(res.name)
                self._backend.send(protocol.DomainNameResolutionCompleted(res.id, addresses))
            elif isinstance(res, protocol.Driver):
                return
            else:
                raise Exception("Should be Driver or DomainNameResolutionRequired but was: %s" % res)

    def supportsMultiDB(self):
        req = protocol.CheckMultiDBSupport(self._driver.id)
        res = self._backend.sendAndReceive(req)
        if not isinstance(res, protocol.MultiDBSupport):
            raise Exception("Should be MultiDBSupport")
        return res.available

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

    def resolveDomainName(self, name):
        return self._domainNameResolverFn(name)

    def updateRoutingTable(self, database=None, bookmarks=None):
        req = protocol.ForcedRoutingTableUpdate(
            self._driver.id, database=database, bookmarks=bookmarks
        )
        res = self._backend.sendAndReceive(req)
        if not isinstance(res, protocol.Driver):
            return Exception("Should be Driver")

    def getRoutingTable(self, database=None):
        req = protocol.GetRoutingTable(self._driver.id, database=database)
        res = self._backend.sendAndReceive(req)
        if not isinstance(res, protocol.RoutingTable):
            raise Exception("Should be RoutingTable")
        return res
