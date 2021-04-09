from .. import protocol


class Result:
    def __init__(self, backend, result, driver):
        self._backend = backend
        self._result = result
        self._driver = driver

    def next(self):
        """ Moves to next record in result.
        """
        req = protocol.ResultNext(self._result.id)
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
            elif isinstance(res, protocol.Record) or isinstance(
                res, protocol.NullRecord
            ):
                return res
            else:
                raise Exception("Should be Result or ResolverResolutionRequired but was: %s" % res)

    def consume(self):
        """ Discards all records in result and returns summary.
        """
        req = protocol.ResultConsume(self._result.id)
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
            elif isinstance(res, protocol.Summary):
                return res
            else:
                raise Exception("Should be Summary or ResolverResolutionRequired but was: %s" % res)
