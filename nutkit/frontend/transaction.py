import nutkit.protocol as protocol
from .result import Result


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

    def commit(self):
        req = protocol.TransactionCommit(self._id)
        res = self._backend.sendAndReceive(req)
        if not isinstance(res, protocol.Transaction):
            raise "Should be transaction"
        return Result(self._backend, res)

    def rollback(self):
        req = protocol.TransactionRollback(self._id)
        res = self._backend.sendAndReceive(req)
        if not isinstance(res, protocol.Transaction):
            raise "Should be transaction"
        return Result(self._backend, res)