from .result import Result
from .. import protocol


class Transaction:
    def __init__(self, backend, id, driver):
        self._backend = backend
        self._id = id
        self._driver = driver

    def run(self, cypher, params=None):
        req = protocol.TransactionRun(self._id, cypher, params)
        while True:
            res = self._backend.sendAndReceive(req)
            if isinstance(res, protocol.ResolverResolutionRequired):
                addresses = self._driver.resolve(res.address)
                self._backend.send(
                    protocol.ResolverResolutionCompleted(res.id, addresses)
                )
            elif isinstance(res, protocol.DomainNameResolutionRequired):
                addresses = self._driver.resolveDomainName(res.name)
                self._backend.send(
                    protocol.DomainNameResolutionCompleted(res.id, addresses)
                )
            elif isinstance(res, protocol.Result):
                return Result(self._backend, res, self._driver)
            else:
                raise Exception("Should be Result or ResolverResolutionRequired but was: %s" % res)

    def commit(self):
        req = protocol.TransactionCommit(self._id)
        res = self._backend.sendAndReceive(req)
        if not isinstance(res, protocol.Transaction):
            raise Exception("Should be transaction but was: %s" % res)

    def rollback(self):
        req = protocol.TransactionRollback(self._id)
        res = self._backend.sendAndReceive(req)
        if not isinstance(res, protocol.Transaction):
            raise Exception("Should be transaction but was: %s" % res)
