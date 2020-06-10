import nutkit.protocol as protocol

from .result import Result

class Session:
    def __init__(self, backend, session):
        self._backend = backend
        self._session = session

    def run(self, cypher, params=None):
        req = protocol.SessionRun(self._session.id, cypher, params)
        res = self._backend.sendAndReceive(req)
        if not isinstance(res, protocol.Result):
            raise "Should be result"
        return Result(self._backend, res)

    def readTransaction(self, fn, config=None):
        req = protocol.SessionReadTransaction(self._session.id)
        res = self._backend.send(req)
        x = None
        while True:
            res = self._backend.receive()
            if isinstance(res, protocol.RetryableTry):
                tx = Transaction(self._backend, res.id)
                try:
                    x = fn(tx)
                    self._backend.send(protocol.RetryablePositive(self._session.id))
                except Exception as e:
                    # Todo: Check exception for id of error
                    print(e)
                    self._backend.send(protocol.RetryableNegative(self._session.id))
            elif isinstance(res, protocol.RetryableDone):
                return x


class Transaction:
    def __init__(self, backend, id):
        self._backend = backend
        self._id = id

    def run(self, cypher, params=None):
        req = protocol.TransactionRun(self._id, cypher, params)
        res = self._backend.sendAndReceive(req)
        if not isinstance(res, protocol.Result):
            raise "Should be result"
        return Result(self._backend, res)

